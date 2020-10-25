#include "common/error/exception.h"
#include "execution/exec/execution_settings.h"
#include "execution/sql/operators/comparison_operators.h"
#include "execution/sql/operators/like_operators.h"
#include "execution/sql/runtime_types.h"
#include "execution/sql/tuple_id_list.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::sql {

namespace {

// Filter optimization:
// --------------------
// When perform a comparison between two vectors we __COULD__ just iterate the input TID list, apply
// the predicate, update the list based on the result of the comparison, and be done with it. But,
// we take advantage of the fact that, for some data types, we can operate on unselected data and
// potentially leverage SIMD to accelerate performance. This only works for simple types like
// integers because unselected data can safely participate in comparisons and get masked out later.
// This is not true for complex types like strings which may have NULLs or other garbage.
//
// This "full-compute" optimization is only beneficial for a range of selectivities that depend on
// the input vector's data type. We use the is_safe_for_full_compute type trait to determine whether
// the input type supports "full-compute". If so, we also verify that the selectivity of the TID
// list warrants the optimization.

template <typename T, typename Enable = void>
struct IsSafeForFullCompute {
  static constexpr bool VALUE = false;
};

template <typename T>
struct IsSafeForFullCompute<T, std::enable_if_t<std::is_fundamental_v<T> || std::is_same_v<T, Date> ||
                                                std::is_same_v<T, Timestamp> || std::is_same_v<T, Decimal32> ||
                                                std::is_same_v<T, Decimal64> || std::is_same_v<T, Decimal128>>> {
  static constexpr bool VALUE = true;
};

// When performing a selection between two vectors, we need to make sure of a few things:
// 1. The types of the two vectors are the same
// 2. If both input vectors are not constants
//   2a. The size of the vectors are the same
//   2b. The selection counts (i.e., the number of "active" or "visible" elements is equal)
// 3. The output TID list is sufficiently large to represents all TIDs in both left and right inputs
void CheckSelection(const Vector &left, const Vector &right, TupleIdList *result) {
  if (left.GetTypeId() != right.GetTypeId()) {
    throw EXECUTION_EXCEPTION(fmt::format("Input vector types must match for selections, left {} right {}.",
                                          TypeIdToString(left.GetTypeId()), TypeIdToString(right.GetTypeId())),
                              common::ErrorCode::ERRCODE_INTERNAL_ERROR);
  }
  if (!left.IsConstant() && !right.IsConstant()) {
    if (left.GetSize() != right.GetSize()) {
      throw EXECUTION_EXCEPTION(
          fmt::format("Left and right vectors to comparison have different sizes, left {} right {}.", left.GetSize(),
                      right.GetSize()),
          common::ErrorCode::ERRCODE_INTERNAL_ERROR);
    }
    if (left.GetCount() != right.GetCount()) {
      throw EXECUTION_EXCEPTION(
          fmt::format("Left and right vectors to comparison have different counts, left {} right {}.", left.GetCount(),
                      right.GetCount()),
          common::ErrorCode::ERRCODE_INTERNAL_ERROR);
    }
    if (result->GetCapacity() != left.GetSize()) {
      throw EXECUTION_EXCEPTION(
          fmt::format("Result list not large enough to store all TIDs in input vector, result {} input {}.",
                      result->GetCapacity(), left.GetSize()),
          common::ErrorCode::ERRCODE_INTERNAL_ERROR);
    }
  }
}

template <typename T, typename Op>
void TemplatedSelectOperationVectorConstant(const exec::ExecutionSettings &exec_settings, const Vector &left,
                                            const Vector &right, TupleIdList *tid_list) {
  // If the scalar constant is NULL, all comparisons are NULL.
  if (right.IsNull(0)) {
    tid_list->Clear();
    return;
  }

  auto *left_data = reinterpret_cast<const T *>(left.GetData());
  auto &constant = *reinterpret_cast<const T *>(right.GetData());

  // Safe full-compute. Refer to comment at start of file for explanation.
  if constexpr (IsSafeForFullCompute<T>::VALUE) {  // NOLINT
    const auto full_compute_threshold = exec_settings.GetSelectOptThreshold();

    if (full_compute_threshold <= tid_list->ComputeSelectivity()) {
      TupleIdList::BitVectorType *bit_vector = tid_list->GetMutableBits();
      bit_vector->UpdateFull([&](uint64_t i) { return Op{}(left_data[i], constant); });
      bit_vector->Difference(left.GetNullMask());
      return;
    }
  }

  // Remove all NULL entries from left input. Right constant is guaranteed non-NULL by this point.
  tid_list->GetMutableBits()->Difference(left.GetNullMask());

  // Filter
  tid_list->Filter([&](uint64_t i) { return Op{}(left_data[i], constant); });
}

template <typename T, typename Op>
void TemplatedSelectOperationVectorVector(const exec::ExecutionSettings &exec_settings, const Vector &left,
                                          const Vector &right, TupleIdList *tid_list) {
  auto *left_data = reinterpret_cast<const T *>(left.GetData());
  auto *right_data = reinterpret_cast<const T *>(right.GetData());

  // Safe full-compute. Refer to comment at start of file for explanation.
  if constexpr (IsSafeForFullCompute<T>::VALUE) {  // NOLINT
    const auto full_compute_threshold = exec_settings.GetSelectOptThreshold();

    // Only perform the full compute if the TID selectivity is larger than the threshold
    if (full_compute_threshold <= tid_list->ComputeSelectivity()) {
      TupleIdList::BitVectorType *bit_vector = tid_list->GetMutableBits();
      bit_vector->UpdateFull([&](uint64_t i) { return Op{}(left_data[i], right_data[i]); });
      bit_vector->Difference(left.GetNullMask()).Difference(right.GetNullMask());
      return;
    }
  }

  // Remove all NULL entries in either vector
  tid_list->GetMutableBits()->Difference(left.GetNullMask()).Difference(right.GetNullMask());

  // Filter
  tid_list->Filter([&](uint64_t i) { return Op{}(left_data[i], right_data[i]); });
}

template <typename T, template <typename> typename Op>
void TemplatedSelectOperation(const exec::ExecutionSettings &exec_settings, const Vector &left, const Vector &right,
                              TupleIdList *tid_list) {
  if (right.IsConstant()) {
    TemplatedSelectOperationVectorConstant<T, Op<T>>(exec_settings, left, right, tid_list);
  } else if (left.IsConstant()) {
    // NOLINTNEXTLINE re-arrange arguments
    TemplatedSelectOperationVectorConstant<T, typename Op<T>::SymmetricOp>(exec_settings, right, left, tid_list);
  } else {
    TemplatedSelectOperationVectorVector<T, Op<T>>(exec_settings, left, right, tid_list);
  }
}

template <template <typename> typename Op>
void SelectOperation(const exec::ExecutionSettings &exec_settings, const Vector &left, const Vector &right,
                     TupleIdList *tid_list) {
  // Sanity check
  CheckSelection(left, right, tid_list);

  // Lift-off
  switch (left.GetTypeId()) {
    case TypeId::Boolean:
      TemplatedSelectOperation<bool, Op>(exec_settings, left, right, tid_list);
      break;
    case TypeId::TinyInt:
      TemplatedSelectOperation<int8_t, Op>(exec_settings, left, right, tid_list);
      break;
    case TypeId::SmallInt:
      TemplatedSelectOperation<int16_t, Op>(exec_settings, left, right, tid_list);
      break;
    case TypeId::Integer:
      TemplatedSelectOperation<int32_t, Op>(exec_settings, left, right, tid_list);
      break;
    case TypeId::BigInt:
      TemplatedSelectOperation<int64_t, Op>(exec_settings, left, right, tid_list);
      break;
    case TypeId::Hash:
      TemplatedSelectOperation<hash_t, Op>(exec_settings, left, right, tid_list);
      break;
    case TypeId::Pointer:
      TemplatedSelectOperation<uintptr_t, Op>(exec_settings, left, right, tid_list);
      break;
    case TypeId::Float:
      TemplatedSelectOperation<float, Op>(exec_settings, left, right, tid_list);
      break;
    case TypeId::Double:
      TemplatedSelectOperation<double, Op>(exec_settings, left, right, tid_list);
      break;
    case TypeId::Date:
      TemplatedSelectOperation<Date, Op>(exec_settings, left, right, tid_list);
      break;
    case TypeId::Timestamp:
      TemplatedSelectOperation<Timestamp, Op>(exec_settings, left, right, tid_list);
      break;
    case TypeId::Varchar:
      TemplatedSelectOperation<storage::VarlenEntry, Op>(exec_settings, left, right, tid_list);
      break;
    default:
      throw NOT_IMPLEMENTED_EXCEPTION(
          fmt::format("selections on vector type '{}' not supported", TypeIdToString(left.GetTypeId())).data());
  }
}

}  // namespace

void VectorOps::SelectEqual(const exec::ExecutionSettings &exec_settings, const Vector &left, const Vector &right,
                            TupleIdList *tid_list) {
  SelectOperation<noisepage::execution::sql::Equal>(exec_settings, left, right, tid_list);
}

void VectorOps::SelectGreaterThan(const exec::ExecutionSettings &exec_settings, const Vector &left, const Vector &right,
                                  TupleIdList *tid_list) {
  SelectOperation<noisepage::execution::sql::GreaterThan>(exec_settings, left, right, tid_list);
}

void VectorOps::SelectGreaterThanEqual(const exec::ExecutionSettings &exec_settings, const Vector &left,
                                       const Vector &right, TupleIdList *tid_list) {
  SelectOperation<noisepage::execution::sql::GreaterThanEqual>(exec_settings, left, right, tid_list);
}

void VectorOps::SelectLessThan(const exec::ExecutionSettings &exec_settings, const Vector &left, const Vector &right,
                               TupleIdList *tid_list) {
  SelectOperation<noisepage::execution::sql::LessThan>(exec_settings, left, right, tid_list);
}

void VectorOps::SelectLessThanEqual(const exec::ExecutionSettings &exec_settings, const Vector &left,
                                    const Vector &right, TupleIdList *tid_list) {
  SelectOperation<noisepage::execution::sql::LessThanEqual>(exec_settings, left, right, tid_list);
}

void VectorOps::SelectNotEqual(const exec::ExecutionSettings &exec_settings, const Vector &left, const Vector &right,
                               TupleIdList *tid_list) {
  SelectOperation<noisepage::execution::sql::NotEqual>(exec_settings, left, right, tid_list);
}

}  // namespace noisepage::execution::sql
