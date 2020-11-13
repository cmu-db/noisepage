#pragma once

#include <type_traits>
#include <utility>

#include "execution/sql/vector.h"
#include "execution/sql/vector_operations/traits.h"
#include "execution/sql/vector_operations/vector_operations.h"

namespace noisepage::execution::sql {

/**
 * Static utility class enabling execution of unary functions over vectors. The main API is
 * UnaryOperationExecutor::Execute() which offers two overloads. The first overload relies on a
 * unary-operation template parameter to perform the logic of the unary operation. This is likely
 * the most common usage.
 */
class UnaryOperationExecutor {
 public:
  /** This class cannot be copied or moved. */
  DISALLOW_COPY_AND_MOVE(UnaryOperationExecutor);

  /**
   * Execute a unary function on all active elements in an input vector and store the results into
   * the provided output vector. The unary function to execute is provided as a template parameter.
   * Like other operators, it's logic must be implemented in Op::Apply().
   *
   * @pre The input vector is allowed to be either a constant or a full vector, filtered or not. The
   *      template operator must be invokable as: <code>OutputType out = f(InputType in)</code>
   * @post The result vector will have the same shape as the input.
   *
   * @tparam InputType The native CPP type of the values in the input vector.
   * @tparam ResultType The native CPP type of the values in the result vector.
   * @tparam Op The unary operation to perform on each element in the input.
   * @tparam IgnoreNull Flag indicating if the operation should skip NULL values in the input.
   * @param exec_settings The execution settings used in this query.
   * @param input The input vector to read values from.
   * @param[out] result The output vector where the results of the unary operation are written into.
   *                    The result vector will have the same set of NULLs, selection vector, and
   *                    count as the input vector.
   */
  template <typename InputType, typename ResultType, typename Op, bool IgnoreNull = false>
  static void Execute(const exec::ExecutionSettings &exec_settings, const Vector &input, Vector *result) {
    ExecuteImpl<InputType, ResultType, Op, IgnoreNull>(exec_settings, input, result, Op{});
  }

  /**
   * Execute a unary functor on all active elements in an input vector and store the results into
   * the provided output vector. The unary functor is provided as the last argument parameter. Its
   * signature
   *
   * @pre The input vector is allowed to be either a constant or a full vector, filtered or not. The
   *      template operator must be invokable as: <code>OutputType out = f(InputType in)</code>
   * @post The result vector will have the same shape as the input.
   *
   * @tparam InputType The native CPP type of the values in the input vector.
   * @tparam ResultType The native CPP type of the values in the result vector.
   * @tparam IgnoreNull Flag indicating if the operation should skip NULL values in the input.
   * @tparam Op The type of the unary functor.
   * @param exec_settings The execution settings for this query.
   * @param input The input vector to read values from.
   * @param op The operation to perform.
   * @param[out] result The output vector where the results of the unary operation are written into.
   *                    The result vector will have the same set of NULLs, selection vector, and
   *                    count as the input vector.
   */
  template <typename InputType, typename ResultType, bool IgnoreNull = false, typename Op>
  static void Execute(const exec::ExecutionSettings &exec_settings, const Vector &input, Vector *result, Op &&op) {
    ExecuteImpl<InputType, ResultType, Op, IgnoreNull>(exec_settings, input, result, std::forward<Op>(op));
  }

 private:
  // Given an input vector and a result vector, apply the provided wrapped
  // operation on all active elements. This helper function exists to provide a
  // common base for unary operations that rely on templates and those that rely
  // on functors.
  template <typename InputType, typename ResultType, typename Op, bool IgnoreNull>
  static inline void ExecuteImpl(const exec::ExecutionSettings &exec_settings, const Vector &input, Vector *result,
                                 Op &&op) {
    // Ensure operator has correct interface.
    static_assert(std::is_invocable_r_v<ResultType, Op, InputType>,
                  "Unary operation has invalid interface for given template arguments");

    auto *RESTRICT input_data = reinterpret_cast<const InputType *>(input.GetData());
    auto *RESTRICT result_data = reinterpret_cast<ResultType *>(result->GetData());

    result->Resize(input.GetSize());
    result->GetMutableNullMask()->Copy(input.GetNullMask());
    result->SetFilteredTupleIdList(input.GetFilteredTupleIdList(), input.GetCount());

    if (input.IsConstant()) {
      if (!input.IsNull(0)) {
        result_data[0] = op(input_data[0]);
      }
    } else {
      if (IgnoreNull && input.GetNullMask().Any()) {
        const auto &null_mask = input.GetNullMask();
        VectorOps::Exec(input, [&](uint64_t i, uint64_t k) {
          if (!null_mask[i]) {
            result_data[i] = op(input_data[i]);
          }
        });
      } else {
        if (traits::ShouldPerformFullCompute<Op>{}(exec_settings, input.GetFilteredTupleIdList())) {
          VectorOps::ExecIgnoreFilter(input, [&](uint64_t i, uint64_t k) { result_data[i] = op(input_data[i]); });
        } else {
          VectorOps::Exec(input, [&](uint64_t i, uint64_t k) { result_data[i] = op(input_data[i]); });
        }
      }
    }
  }
};

}  // namespace noisepage::execution::sql
