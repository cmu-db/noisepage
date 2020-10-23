#pragma once

namespace noisepage::execution::exec {
class ExecutionSettings;
}  // namespace noisepage::execution::exec

namespace noisepage::execution::sql {

class TupleIdList;

namespace traits {

/**
 * A type-trait struct to determine whether the given templated operation type should use a
 * full-computation implementation. In normal vector operations, an operation is only applied to
 * active vector elements. However, for some operations, some data types, and vector selectivities
 * it is faster to apply the operation on both active AND inactive elements (with some compensating
 * work) in order to take advantage of auto-vectorization applied by the compiler. This trait allows
 * operators to participate in this optimization.
 *
 * By default, full-computation is disabled. Thus, if you don't want your operator to be considered
 * for this optimization, you don've to do anything.
 *
 * If you want to enable full-computation for your operation, specialize this struct and implement
 * the call operator. The argument to the call operator is the (potentially NULL) TID list of the
 * vector inputs to the operator.
 *
 * For example, if you have the following operator:
 *
 * @code
 * template <typename A>
 * struct MyFancyAssOperator {
 *   void operator()(A a) const { // operator logic // }
 * };
 * @endcode
 *
 * And you wanted to participate in full-computation for integers if selectivity is > 25% and for
 * floats only is selectivity > 50%, you'd specialize as follows:
 *
 * @code
 * template <>
 * struct ShouldPerformFullCompute<MyFancyAssOperator<int>> {
 *   bool operator()(const TupleIdList *tids) { return tids->ComputeSelectivity() > 0.25; }
 * };
 * template <>
 * struct ShouldPerformFullCompute<MyFancyAssOperator<float>> {
 *   bool operator()(const TupleIdList *tids) { return tids->ComputeSelectivity() > 0.5; }
 * };
 * @endcode
 *
 * @tparam Op The operation to consider. All input argument types should be determined from the
 *            operator template arguments.
 * @tparam Enable Enabling switch.
 */
template <typename Op, typename Enable = void>
struct ShouldPerformFullCompute {
  /**
   * Call operator used by vector infrastructure to check if the given operator should be executed
   * in full-compute on inputs with the given TID list.
   * @param exec_settings The execution settings to use.
   * @param tid_list The TID list that the operator will use. Potentially NULL.
   * @return True if full-compute is enabled; false otherwise.
   */
  constexpr bool operator()(const exec::ExecutionSettings &exec_settings, const TupleIdList *tid_list) const noexcept {
    return false;
  }
};

}  // namespace traits

}  // namespace noisepage::execution::sql
