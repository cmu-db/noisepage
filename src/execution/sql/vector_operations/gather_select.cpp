#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "execution/sql/operators/comparison_operators.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::sql {

// The operations in this file implement a fused gather+select operation. A
// simple implementation iterates the TIDs in the input filter, loads a memory
// pointer value, adds the requested byte offset, and performs the requested
// comparison operation all in a scalar loop. This pattern is perfectly suited
// to SIMD optimization using a masked gather and compare since a TupleIdList is
// just a wrapper around a bit vector. Rather than writing custom SIMD code, it
// would be nice if the compiler auto-vectorized the loop for us. In fact, Clang
// WILL auto-vectorize the loop for us, BUT only if we use a loop pattern it
// recognizes. That loop pattern is:
//
// for i in range(num_bits):
//   if (bits[i]):
//     bits[i] = 1 if ptrs[i]=input[i] else 0
//
// The above pattern, in normal use, is disgustingly slow. TupleIdList::Filter()
// is blazing fast in comparison, more than 2x. But, Clang won't auto-vectorize
// our super-fast loop.
//
// Since SIMD gathers offer poor performance at the time of writing, and because
// I don't want to write a bunch of custom SIMD code, I'm using a vanilla filter.
//
// TODO(pmenon): Revisit if and when SIMD masked gathers perform well. Use the
//               JoinManagerBenchmark to check.

namespace {

void CheckGatherAndSelect(const Vector &input, const Vector &pointers, UNUSED_ATTRIBUTE std::size_t offset,
                          TupleIdList *result) {
  if (pointers.GetTypeId() != TypeId::Pointer) {
    throw EXECUTION_EXCEPTION(
        fmt::format("Pointers vector must be TypeId::Pointer, but is type {}.", TypeIdToString(pointers.GetTypeId())),
        common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
  if (input.GetSize() != pointers.GetSize()) {
    throw EXECUTION_EXCEPTION(
        fmt::format("Input vectors have mismatched shapes, {} vs {}.", input.GetSize(), pointers.GetSize()),
        common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
  if (result->GetCapacity() != input.GetSize()) {
    throw EXECUTION_EXCEPTION(
        fmt::format(
            "Result list not large enough to store all TIDs in input vector, input size {} results capacity {}.",
            input.GetSize(), result->GetCapacity()),
        common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
}

template <typename T, typename Op>
void TemplatedGatherAndSelectOperationConstant(const Vector &input, const Vector &pointers, const std::size_t offset,
                                               TupleIdList *tid_list) {
  // If input is a NULL constant, there aren't any matches.
  if (input.IsNull(0)) {
    tid_list->Clear();
    return;
  }

  // Check.
  const auto *RESTRICT constant = reinterpret_cast<T *>(input.GetData());
  const auto *RESTRICT raw_pointers = reinterpret_cast<const byte **>(pointers.GetData());
  tid_list->Filter([&](const uint64_t i) {
    const auto *RESTRICT element = reinterpret_cast<const T *>(raw_pointers[i] + offset);
    return Op{}(*element, *constant);
  });
}

template <typename T, typename Op>
void TemplatedGatherAndSelectOperationVector(const Vector &input, const Vector &pointers, const std::size_t offset,
                                             TupleIdList *tid_list) {
  // Strip out NULL inputs now to avoid checking in the loop.
  tid_list->GetMutableBits()->Difference(input.GetNullMask());

  // Check.
  const auto *RESTRICT raw_inputs = reinterpret_cast<T *>(input.GetData());
  const auto *RESTRICT raw_pointers = reinterpret_cast<const byte **>(pointers.GetData());
  tid_list->Filter([&](const uint64_t i) {
    const auto *RESTRICT element = reinterpret_cast<const T *>(raw_pointers[i] + offset);
    return Op{}(*element, raw_inputs[i]);
  });
}

template <typename T, template <typename> typename Op>
void TemplatedGatherAndSelectOperation(const Vector &input, const Vector &pointers, const std::size_t offset,
                                       TupleIdList *tid_list) {
  if (input.IsConstant()) {
    TemplatedGatherAndSelectOperationConstant<T, Op<T>>(input, pointers, offset, tid_list);
  } else {
    TemplatedGatherAndSelectOperationVector<T, Op<T>>(input, pointers, offset, tid_list);
  }
}

template <template <typename> typename Op>
void GatherAndSelectOperation(const Vector &input, const Vector &pointers, const std::size_t offset,
                              TupleIdList *tid_list) {
  // Sanity check.
  CheckGatherAndSelect(input, pointers, offset, tid_list);

  // Lift-off.
  switch (input.GetTypeId()) {
    case TypeId::Boolean:
      TemplatedGatherAndSelectOperation<bool, Op>(input, pointers, offset, tid_list);
      break;
    case TypeId::TinyInt:
      TemplatedGatherAndSelectOperation<int8_t, Op>(input, pointers, offset, tid_list);
      break;
    case TypeId::SmallInt:
      TemplatedGatherAndSelectOperation<int16_t, Op>(input, pointers, offset, tid_list);
      break;
    case TypeId::Integer:
      TemplatedGatherAndSelectOperation<int32_t, Op>(input, pointers, offset, tid_list);
      break;
    case TypeId::BigInt:
      TemplatedGatherAndSelectOperation<int64_t, Op>(input, pointers, offset, tid_list);
      break;
    case TypeId::Float:
      TemplatedGatherAndSelectOperation<float, Op>(input, pointers, offset, tid_list);
      break;
    case TypeId::Double:
      TemplatedGatherAndSelectOperation<double, Op>(input, pointers, offset, tid_list);
      break;
    case TypeId::Date:
      TemplatedGatherAndSelectOperation<Date, Op>(input, pointers, offset, tid_list);
      break;
    case TypeId::Timestamp:
      TemplatedGatherAndSelectOperation<Timestamp, Op>(input, pointers, offset, tid_list);
      break;
    case TypeId::Varchar:
      TemplatedGatherAndSelectOperation<storage::VarlenEntry, Op>(input, pointers, offset, tid_list);
      break;
    case TypeId::Varbinary:
      TemplatedGatherAndSelectOperation<Blob, Op>(input, pointers, offset, tid_list);
      break;
    default:
      throw NOT_IMPLEMENTED_EXCEPTION(fmt::format("Gather+select on type {}.", TypeIdToString(input.GetTypeId())));
  }
}

}  // namespace

void VectorOps::GatherAndSelectEqual(const Vector &input, const Vector &pointers, const std::size_t offset,
                                     TupleIdList *tid_list) {
  GatherAndSelectOperation<noisepage::execution::sql::Equal>(input, pointers, offset, tid_list);
}

void VectorOps::GatherAndSelectGreaterThan(const Vector &input, const Vector &pointers, const std::size_t offset,
                                           TupleIdList *tid_list) {
  GatherAndSelectOperation<noisepage::execution::sql::GreaterThan>(input, pointers, offset, tid_list);
}

void VectorOps::GatherAndSelectGreaterThanEqual(const Vector &input, const Vector &pointers, const std::size_t offset,
                                                TupleIdList *tid_list) {
  GatherAndSelectOperation<noisepage::execution::sql::GreaterThanEqual>(input, pointers, offset, tid_list);
}

void VectorOps::GatherAndSelectLessThan(const Vector &input, const Vector &pointers, const std::size_t offset,
                                        TupleIdList *tid_list) {
  GatherAndSelectOperation<noisepage::execution::sql::LessThan>(input, pointers, offset, tid_list);
}

void VectorOps::GatherAndSelectLessThanEqual(const Vector &input, const Vector &pointers, const std::size_t offset,
                                             TupleIdList *tid_list) {
  GatherAndSelectOperation<noisepage::execution::sql::LessThanEqual>(input, pointers, offset, tid_list);
}

void VectorOps::GatherAndSelectNotEqual(const Vector &input, const Vector &pointers, const std::size_t offset,
                                        TupleIdList *tid_list) {
  GatherAndSelectOperation<noisepage::execution::sql::NotEqual>(input, pointers, offset, tid_list);
}

}  // namespace noisepage::execution::sql
