#pragma once

#include <type_traits>

#include "execution/util/exception.h"
#include "execution/sql/vector.h"
#include "execution/sql/vector_operations/traits.h"
#include "execution/sql/vector_operations/vector_operations.h"

namespace terrier::execution::sql {

/**
 * Check:
 * - Input and output vectors have the same type.
 * - Input and output vectors have the same shape.
 *
 * @param result The vector storing the result of the in-place operation.
 * @param input The right-side input into the in-place operation.
 */
inline void CheckInplaceOperation(const Vector *result, const Vector &input) {
  if (result->GetTypeId() != input.GetTypeId()) {
    throw TypeMismatchException(
        result->GetTypeId(), input.GetTypeId(),
        "left and right vector types to inplace operation must be the same");
  }
  if (!input.IsConstant() && result->GetCount() != input.GetCount()) {
    throw Exception(ExceptionType::Cardinality,
                    "left and right input vectors to binary operation must have the same size");
  }
}

 class InPlaceOperationExecutor : public common::AllStatic {
 public:
  /**
   * Execute an in-place operation on all active elements in two vectors, @em result and @em input,
   * and store the result into the first input/output vector, @em result.
   *
   * @pre Both input vectors have the same shape.
   *
   * @note This function leverages the terrier::execution::sql::traits::ShouldPerformFullCompute trait to determine
   *       whether the operation should be performed on ALL vector elements or just the active
   *       elements. Callers can control this feature by optionally specialization the trait for
   *       their operation type. If you want to use this optimization, you cannot pass in a
   *       std::function; move your logic into a function object and pass an instance.
   *
   *
   * @tparam ResultType The native CPP type of the elements in the result output vector.
   * @tparam InputType The native CPP type of the elements in the first input vector.
   * @tparam Op The binary operation to perform. Each invocation will receive an element from the
   *            result and input input vectors and must produce an element that is stored back into
   *            the result vector.
   * @param[in,out] result The result vector.
   * @param input The right input.
   */
  template <typename ResultType, typename InputType, class Op>
  static void Execute(Vector *result, const Vector &input) {
    Execute<ResultType, InputType, Op>(result, input, Op{});
  }

  /**
   * Execute an in-place operation on all active elements in two vectors, @em result and @em input,
   * and store the result into the first input/output vector, @em result.
   *
   * @pre Both input vectors have the same shape.
   *
   * @note This function leverages the terrier::execution::sql::traits::ShouldPerformFullCompute trait to determine
   *       whether the operation should be performed on ALL vector elements or just the active
   *       elements. Callers can control this feature by optionally specialization the trait for
   *       their operation type. If you want to use this optimization, you cannot pass in a
   *       std::function; move your logic into a function object and pass an instance.
   *
   *
   * @tparam ResultType The native CPP type of the elements in the result output vector.
   * @tparam InputType The native CPP type of the elements in the first input vector.
   * @tparam Op The binary operation to perform. Each invocation will receive an element from the
   *            result and input input vectors and must produce an element that is stored back into
   *            the result vector.
   * @param[in,out] result The result vector.
   * @param input The right input.
   * @param op The operation to perform.
   */
  template <typename ResultType, typename InputType, class Op>
  static void Execute(Vector *result, const Vector &input, Op &&op) {
    // Ensure operator has correct interface.
    static_assert(std::is_invocable_v<Op, ResultType *, InputType>,
                  "In-place operation has invalid interface for given template arguments.");

    auto input_data = reinterpret_cast<InputType *>(input.GetData());
    auto result_data = reinterpret_cast<ResultType *>(result->GetData());

    if (input.IsConstant()) {
      if (input.IsNull(0)) {
        result->GetMutableNullMask()->SetAll();
      } else {
        if (traits::ShouldPerformFullCompute<Op>()(result->GetFilteredTupleIdList())) {
          VectorOps::ExecIgnoreFilter(
              *result, [&](uint64_t i, uint64_t k) { op(&result_data[i], input_data[0]); });
        } else {
          VectorOps::Exec(*result,
                          [&](uint64_t i, uint64_t k) { op(&result_data[i], input_data[0]); });
        }
      }
    } else {
      TERRIER_ASSERT(result->GetFilteredTupleIdList() == input.GetFilteredTupleIdList(),
                 "Filter list of inputs to in-place operation do not match");

      result->GetMutableNullMask()->Union(input.GetNullMask());
      if (traits::ShouldPerformFullCompute<Op>()(result->GetFilteredTupleIdList())) {
        VectorOps::ExecIgnoreFilter(
            *result, [&](uint64_t i, uint64_t k) { op(&result_data[i], input_data[i]); });
      } else {
        VectorOps::Exec(*result,
                        [&](uint64_t i, uint64_t k) { op(&result_data[i], input_data[i]); });
      }
    }
  }
};

}  // namespace terrier::execution::sql
