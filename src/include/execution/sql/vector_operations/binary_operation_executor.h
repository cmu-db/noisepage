#pragma once

#include <type_traits>
#include <utility>

#include "execution/sql/vector.h"
#include "execution/sql/vector_operations/traits.h"
#include "execution/sql/vector_operations/vector_operations.h"

namespace noisepage::execution::sql {

/** Static class for operating binary operations. */
class BinaryOperationExecutor {
 public:
  /** This class cannot be copied or moved. */
  DISALLOW_COPY_AND_MOVE(BinaryOperationExecutor);

  /**
   * Execute a binary operation on all active elements contained in two input vectors, @em left and
   * @em right, and store the result into an output vector, @em result. An instance of the binary
   * operation templated type is created and used for the operation. Thus, it's assumed that it
   * contains no state.
   *
   * @pre 1. Both vectors cannot be constants.
   *      2. Both input vectors have the same type and shape.
   *      3. The template types of both inputs and the output match the underlying vector types.
   * @post The output vector has the same shape (size and filter status) as both inputs.
   *
   * @note This function leverages the noisepage::execution::sql::traits::ShouldPerformFullCompute trait to determine
   *       whether the operation should be performed on ALL vector elements or just the active
   *       elements. Callers can control this feature by optionally specialization the trait for
   *       their operation type. If you want to use this optimization, you cannot pass in a
   *       std::function; move your logic into a function object and pass an instance.
   *
   * @tparam LeftType The native CPP type of the elements in the first input vector.
   * @tparam RightType The native CPP type of the elements in the second input vector.
   * @tparam ResultType The native CPP type of the elements in the result output vector.
   * @tparam Op The binary operation to perform. Each invocation will receive an element from the
   *            first and second input vectors and must produce an element that is stored in the
   *            result vector.
   * @tparam IgnoreNull Flag indicating if the operation should skip NULL values as in either input.
   * @param exec_settings The execution settings used by this query.
   * @param left The left input.
   * @param right The right input.
   * @param[out] result The result vector.
   */
  template <typename LeftType, typename RightType, typename ResultType, typename Op, bool IgnoreNull = false>
  static void Execute(const exec::ExecutionSettings &exec_settings, const Vector &left, const Vector &right,
                      Vector *result) {
    Execute<LeftType, RightType, ResultType, Op, IgnoreNull>(exec_settings, left, right, result, Op{});
  }

  /**
   * Execute the provided binary operation, @em op, on all active elements contained in two input
   * vectors, @em left and @em right, and store the result into an output vector, @em result.
   *
   * @pre Both input vectors have the same type and shape. The template types of both inputs and the
   *      output match the underlying vector types.
   * @post The output vector has the same shape (size and filter status) as both inputs.
   *
   * @note This function leverages the noisepage::execution::sql::traits::ShouldPerformFullCompute trait to determine
   *       whether the operation should be performed on ALL vector elements or just the active
   *       elements. Callers can control this feature by optionally specialization the trait for
   *       their operation type. If you want to use this optimization, you cannot pass in a
   *       std::function; move your logic into a function object and pass an instance.
   *
   * @tparam LeftType The native CPP type of the elements in the first input vector.
   * @tparam RightType The native CPP type of the elements in the second input vector.
   * @tparam ResultType The native CPP type of the elements in the result output vector.
   * @tparam Op The binary operation to perform. Each invocation will receive an element from the
   *            first and second input vectors and must produce an element that is stored in the
   *            result vector.
   * @tparam IgnoreNull Flag indicating if the operation should skip NULL values as in either input.
   * @param exec_settings The execution settings used by this query.
   * @param left The left input.
   * @param right The right input.
   * @param[out] result The result vector.
   * @param op The binary operation.
   */
  template <typename LeftType, typename RightType, typename ResultType, typename Op, bool IgnoreNull = false>
  static void Execute(const exec::ExecutionSettings &exec_settings, const Vector &left, const Vector &right,
                      Vector *result, Op &&op) {
    // Ensure operator has correct interface.
    static_assert(std::is_invocable_r_v<ResultType, Op, LeftType, RightType>,
                  "Binary operation has invalid interface for given template arguments.");

    // Ensure at least one of the inputs are vectors.
    NOISEPAGE_ASSERT(!left.IsConstant() || !right.IsConstant(), "Both inputs to binary cannot be constants");

    if (left.IsConstant()) {
      ExecuteImplConstantVector<LeftType, RightType, ResultType, Op, IgnoreNull>(exec_settings, left, right, result,
                                                                                 std::forward<Op>(op));
    } else if (right.IsConstant()) {
      ExecuteImplVectorConstant<LeftType, RightType, ResultType, Op, IgnoreNull>(exec_settings, left, right, result,
                                                                                 std::forward<Op>(op));
    } else {
      ExecuteImplVectorVector<LeftType, RightType, ResultType, Op, IgnoreNull>(exec_settings, left, right, result,
                                                                               std::forward<Op>(op));
    }
  }

 private:
  // Binary operation where the left input is a constant value.
  template <typename LeftType, typename RightType, typename ResultType, typename Op, bool IgnoreNull>
  static void ExecuteImplConstantVector(const exec::ExecutionSettings &exec_settings, const Vector &left,
                                        const Vector &right, Vector *result, Op &&op) {
    auto *RESTRICT left_data = reinterpret_cast<LeftType *>(left.GetData());
    auto *RESTRICT right_data = reinterpret_cast<RightType *>(right.GetData());
    auto *RESTRICT result_data = reinterpret_cast<ResultType *>(result->GetData());

    result->Resize(right.GetSize());
    result->SetFilteredTupleIdList(right.GetFilteredTupleIdList(), right.GetCount());

    if (left.IsNull(0)) {
      VectorOps::FillNull(result);
    } else {
      result->GetMutableNullMask()->Copy(right.GetNullMask());

      if (IgnoreNull && result->GetNullMask().Any()) {
        VectorOps::Exec(right, [&](uint64_t i, uint64_t k) {
          if (!result->GetNullMask()[i]) {
            result_data[i] = op(left_data[0], right_data[i]);
          }
        });
      } else {
        if (traits::ShouldPerformFullCompute<Op>()(exec_settings, right.GetFilteredTupleIdList())) {
          VectorOps::ExecIgnoreFilter(
              right, [&](uint64_t i, uint64_t k) { result_data[i] = op(left_data[0], right_data[i]); });
        } else {
          VectorOps::Exec(right, [&](uint64_t i, uint64_t k) { result_data[i] = op(left_data[0], right_data[i]); });
        }
      }
    }
  }

  // Binary operation where the right input is a constant value.
  template <typename LeftType, typename RightType, typename ResultType, typename Op, bool IgnoreNull>
  static void ExecuteImplVectorConstant(const exec::ExecutionSettings &exec_settings, const Vector &left,
                                        const Vector &right, Vector *result, Op &&op) {
    auto *RESTRICT left_data = reinterpret_cast<LeftType *>(left.GetData());
    auto *RESTRICT right_data = reinterpret_cast<RightType *>(right.GetData());
    auto *RESTRICT result_data = reinterpret_cast<ResultType *>(result->GetData());

    result->Resize(left.GetSize());
    result->SetFilteredTupleIdList(left.GetFilteredTupleIdList(), left.GetCount());

    if (right.IsNull(0)) {
      VectorOps::FillNull(result);
    } else {
      result->GetMutableNullMask()->Copy(left.GetNullMask());

      if (IgnoreNull && result->GetNullMask().Any()) {
        VectorOps::Exec(left, [&](uint64_t i, uint64_t k) {
          if (!result->GetNullMask()[i]) {
            result_data[i] = op(left_data[i], right_data[0]);
          }
        });
      } else {
        if (traits::ShouldPerformFullCompute<Op>()(exec_settings, left.GetFilteredTupleIdList())) {
          VectorOps::ExecIgnoreFilter(
              left, [&](uint64_t i, uint64_t k) { result_data[i] = op(left_data[i], right_data[0]); });
        } else {
          VectorOps::Exec(left, [&](uint64_t i, uint64_t k) { result_data[i] = op(left_data[i], right_data[0]); });
        }
      }
    }
  }

  // Binary operation where both inputs are vectors.
  template <typename LeftType, typename RightType, typename ResultType, typename Op, bool IgnoreNull>
  static void ExecuteImplVectorVector(const exec::ExecutionSettings &exec_settings, const Vector &left,
                                      const Vector &right, Vector *result, Op &&op) {
    NOISEPAGE_ASSERT(left.GetFilteredTupleIdList() == right.GetFilteredTupleIdList(),
                     "Mismatched selection vectors for comparison");
    NOISEPAGE_ASSERT(left.GetCount() == right.GetCount(), "Mismatched vector counts for comparison");

    auto *RESTRICT left_data = reinterpret_cast<LeftType *>(left.GetData());
    auto *RESTRICT right_data = reinterpret_cast<RightType *>(right.GetData());
    auto *RESTRICT result_data = reinterpret_cast<ResultType *>(result->GetData());

    result->Resize(left.GetSize());
    result->GetMutableNullMask()->Copy(left.GetNullMask()).Union(right.GetNullMask());
    result->SetFilteredTupleIdList(left.GetFilteredTupleIdList(), left.GetCount());

    if (IgnoreNull && result->GetNullMask().Any()) {
      VectorOps::Exec(left, [&](uint64_t i, uint64_t k) {
        if (!result->GetNullMask()[i]) {
          result_data[i] = op(left_data[i], right_data[i]);
        }
      });
    } else {
      if (traits::ShouldPerformFullCompute<Op>()(exec_settings, left.GetFilteredTupleIdList())) {
        VectorOps::ExecIgnoreFilter(left,
                                    [&](uint64_t i, uint64_t k) { result_data[i] = op(left_data[i], right_data[i]); });
      } else {
        VectorOps::Exec(left, [&](uint64_t i, uint64_t k) { result_data[i] = op(left_data[i], right_data[i]); });
      }
    }
  }
};

}  // namespace noisepage::execution::sql
