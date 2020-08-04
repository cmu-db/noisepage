#pragma once

#include "common/constants.h"
#include "execution/util/execution_common.h"

namespace terrier::execution::exec {
/**
 * ExecutionSettings stores settings that are passed down from the upper layers.
 * TODO(WAN): Hook this up to the settings manager. Since everything is currently hardcoded, it can wait.
 */
class EXPORT ExecutionSettings {
 public:
  /** @return The vector active element threshold past which full auto-vectorization is done on vectors. */
  constexpr double GetSelectOptThreshold() const { return select_opt_threshold_; }

  /** @return The vector selectivity past which full computation is done. */
  constexpr double GetArithmeticFullComputeOptThreshold() const { return arithmetic_full_compute_opt_threshold_; }

  /** @return The minimum bit vector density before using a SIMD decoding algorithm. */
  constexpr float GetMinBitDensityThresholdForAVXIndexDecode() const {
    return min_bit_density_threshold_for_avx_index_decode_;
  }

  /** @return The statistics sampling frequency when adaptively reordering predicate clauses. */
  constexpr float GetAdaptivePredicateOrderSamplingFrequency() const {
    return adaptive_predicate_order_sampling_frequency_;
  }

  /** @return True if parallel query execution is enabled. */
  constexpr bool GetIsParallelQueryExecutionEnabled() const { return is_parallel_query_execution_; }

 private:
  double select_opt_threshold_{common::Constants::SELECT_OPT_THRESHOLD};
  double arithmetic_full_compute_opt_threshold_{common::Constants::ARITHMETIC_FULL_COMPUTE_THRESHOLD};
  float min_bit_density_threshold_for_avx_index_decode_{common::Constants::BIT_DENSITY_THRESHOLD_FOR_AVX_INDEX_DECODE};
  float adaptive_predicate_order_sampling_frequency_{common::Constants::ADAPTIVE_PRED_ORDER_SAMPLE_FREQ};
  bool is_parallel_query_execution_{common::Constants::IS_PARALLEL_QUERY_EXECUTION};
};
}  // namespace terrier::execution::exec
