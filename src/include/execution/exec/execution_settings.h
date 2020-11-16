#pragma once

#include "common/constants.h"
#include "common/managed_pointer.h"
#include "execution/util/execution_common.h"

namespace noisepage::settings {
class SettingsManager;
}  // namespace noisepage::settings

namespace noisepage::runner {
class MiniRunners;
}  // namespace noisepage::runner

namespace noisepage::execution {
class SqlBasedTest;
}  // namespace noisepage::execution

namespace noisepage::optimizer {
class IdxJoinTest_SimpleIdxJoinTest_Test;
class IdxJoinTest_MultiPredicateJoin_Test;
class IdxJoinTest_MultiPredicateJoinWithExtra_Test;
class IdxJoinTest_FooOnlyScan_Test;
class IdxJoinTest_BarOnlyScan_Test;
class IdxJoinTest_IndexToIndexJoin_Test;
}  // namespace noisepage::optimizer

namespace noisepage::tpch {
class Workload;
}  // namespace noisepage::tpch

namespace noisepage::execution::exec {
/**
 * ExecutionSettings stores settings that are passed down from the upper layers.
 * TODO(WAN): Hook this up to the settings manager. Since everything is currently hardcoded, it can wait.
 */
class EXPORT ExecutionSettings {
 public:
  /**
   * Updates flags from SettingsManager
   * @param settings SettingsManager
   */
  void UpdateFromSettingsManager(common::ManagedPointer<settings::SettingsManager> settings);

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
  constexpr bool GetIsParallelQueryExecutionEnabled() const { return is_parallel_execution_enabled_; }

  /** @return True if counters are enabled. */
  bool GetIsCountersEnabled() const { return is_counters_enabled_; }

  /** @return True if pipeline metrics are enabled */
  bool GetIsPipelineMetricsEnabled() const { return is_pipeline_metrics_enabled_; }

  /** @return number of threads used for parallel execution. */
  int GetNumberOfParallelExecutionThreads() const { return number_of_parallel_execution_threads_; }

  /** @return True if static partitioner is enabled. */
  constexpr bool GetIsStaticPartitionerEnabled() const { return is_static_partitioner_enabled_; }

 private:
  double select_opt_threshold_{common::Constants::SELECT_OPT_THRESHOLD};
  double arithmetic_full_compute_opt_threshold_{common::Constants::ARITHMETIC_FULL_COMPUTE_THRESHOLD};
  float min_bit_density_threshold_for_avx_index_decode_{common::Constants::BIT_DENSITY_THRESHOLD_FOR_AVX_INDEX_DECODE};
  float adaptive_predicate_order_sampling_frequency_{common::Constants::ADAPTIVE_PRED_ORDER_SAMPLE_FREQ};
  bool is_parallel_execution_enabled_{common::Constants::IS_PARALLEL_EXECUTION_ENABLED};
  bool is_counters_enabled_{common::Constants::IS_COUNTERS_ENABLED};
  bool is_pipeline_metrics_enabled_{common::Constants::IS_PIPELINE_METRICS_ENABLED};
  int number_of_parallel_execution_threads_{common::Constants::NUM_PARALLEL_EXECUTION_THREADS};
  bool is_static_partitioner_enabled_{common::Constants::IS_STATIC_PARTITIONER_ENABLED};

  // MiniRunners needs to set query_identifier and pipeline_operating_units_.
  friend class noisepage::runner::MiniRunners;
  friend class noisepage::tpch::Workload;
  friend class noisepage::execution::SqlBasedTest;
  friend class noisepage::optimizer::IdxJoinTest_SimpleIdxJoinTest_Test;
  friend class noisepage::optimizer::IdxJoinTest_MultiPredicateJoin_Test;
  friend class noisepage::optimizer::IdxJoinTest_MultiPredicateJoinWithExtra_Test;
  friend class noisepage::optimizer::IdxJoinTest_FooOnlyScan_Test;
  friend class noisepage::optimizer::IdxJoinTest_BarOnlyScan_Test;
  friend class noisepage::optimizer::IdxJoinTest_IndexToIndexJoin_Test;
};
}  // namespace noisepage::execution::exec
