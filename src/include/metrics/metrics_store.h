#pragma once

#include <bitset>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "execution/exec_defs.h"
#include "metrics/abstract_metric.h"
#include "metrics/abstract_raw_data.h"
#include "metrics/execution_metric.h"
#include "metrics/garbage_collection_metric.h"
#include "metrics/logging_metric.h"
#include "metrics/metrics_defs.h"
#include "metrics/pipeline_metric.h"
#include "metrics/transaction_metric.h"

namespace terrier::metrics {

class MetricsManager;

/**
 * @brief Class responsible for collecting raw data on a single thread.
 *
 * Each thread should be assigned one collector, at the time of system startup, that is globally
 * unique. This is to ensure that we can collect raw data in an non-blocking way as the
 * collection code runs on critical query path. Periodically a dedicated aggregator thread
 * will put the data from all collectors together into a meaningful form.
 */
class MetricsStore {
 public:
  /**
   * Record metrics from the LogSerializerTask
   * @param num_bytes first entry of metrics datapoint
   * @param num_records second entry of metrics datapoint
   * @param resource_metrics third entry of metrics datapoint
   */
  void RecordSerializerData(const uint64_t num_bytes, const uint64_t num_records,
                            const common::ResourceTracker::Metrics &resource_metrics) {
    TERRIER_ASSERT(ComponentEnabled(MetricsComponent::LOGGING), "LoggingMetric not enabled.");
    TERRIER_ASSERT(logging_metric_ != nullptr, "LoggingMetric not allocated. Check MetricsStore constructor.");
    logging_metric_->RecordSerializerData(num_bytes, num_records, resource_metrics);
  }

  /**
   * Record metrics from the LogConsumerTask
   * @param num_bytes first entry of metrics datapoint
   * @param num_records second entry of metrics datapoint
   * @param resource_metrics third entry of metrics datapoint
   */
  void RecordConsumerData(const uint64_t num_bytes, const uint64_t num_records,
                          const common::ResourceTracker::Metrics &resource_metrics) {
    TERRIER_ASSERT(ComponentEnabled(MetricsComponent::LOGGING), "LoggingMetric not enabled.");
    TERRIER_ASSERT(logging_metric_ != nullptr, "LoggingMetric not allocated. Check MetricsStore constructor.");
    logging_metric_->RecordConsumerData(num_bytes, num_records, resource_metrics);
  }

  /**
   * Record metrics from the GC deallocation
   * @param num_processed first entry of metrics datapoint
   * @param resource_metrics second entry of metrics datapoint
   */
  void RecordDeallocateData(const uint64_t num_processed, const common::ResourceTracker::Metrics &resource_metrics) {
    TERRIER_ASSERT(ComponentEnabled(MetricsComponent::GARBAGECOLLECTION), "GarbageCollectionMetric not enabled.");
    TERRIER_ASSERT(gc_metric_ != nullptr, "GarbageCollectionMetric not allocated. Check MetricsStore constructor.");
    gc_metric_->RecordDeallocateData(num_processed, resource_metrics);
  }

  /**
   * Record metrics from the GC deallocation
   * @param num_processed first entry of metrics datapoint
   * @param num_buffers second entry of metrics datapoint
   * @param num_readonly third entry of metrics datapoint
   * @param resource_metrics forth entry of metrics datapoint
   */
  void RecordUnlinkData(const uint64_t num_processed, const uint64_t num_buffers, const uint64_t num_readonly,
                        const common::ResourceTracker::Metrics &resource_metrics) {
    TERRIER_ASSERT(ComponentEnabled(MetricsComponent::GARBAGECOLLECTION), "GarbageCollectionMetric not enabled.");
    TERRIER_ASSERT(gc_metric_ != nullptr, "GarbageCollectionMetric not allocated. Check MetricsStore constructor.");
    gc_metric_->RecordUnlinkData(num_processed, num_buffers, num_readonly, resource_metrics);
  }

  /**
   * Record metrics for transaction manager when beginning transaction
   * @param resource_metrics first entry of txn datapoint
   */
  void RecordBeginData(const common::ResourceTracker::Metrics &resource_metrics) {
    TERRIER_ASSERT(ComponentEnabled(MetricsComponent::TRANSACTION), "TransactionMetric not enabled.");
    TERRIER_ASSERT(txn_metric_ != nullptr, "TransactionMetric not allocated. Check MetricsStore constructor.");
    txn_metric_->RecordBeginData(resource_metrics);
  }

  /**
   * Record metrics for transaction manager when ending transaction
   * @param is_readonly first entry of txn datapoint
   * @param resource_metrics second entry of txn datapoint
   */
  void RecordCommitData(const uint64_t is_readonly, const common::ResourceTracker::Metrics &resource_metrics) {
    TERRIER_ASSERT(ComponentEnabled(MetricsComponent::TRANSACTION), "TransactionMetric not enabled.");
    TERRIER_ASSERT(txn_metric_ != nullptr, "TransactionMetric not allocated. Check MetricsStore constructor.");
    txn_metric_->RecordCommitData(is_readonly, resource_metrics);
  }

  /**
   * Record metrics for the execution engine when finish a pipeline
   * @param feature first entry of execution datapoint
   * @param len second entry of execution datapoint
   * @param execution_mode Execution mode
   * @param resource_metrics Metrics
   */
  void RecordExecutionData(const char *feature, uint32_t len, uint8_t execution_mode,
                           const common::ResourceTracker::Metrics &resource_metrics) {
    TERRIER_ASSERT(ComponentEnabled(MetricsComponent::EXECUTION), "ExecutionMetric not enabled.");
    TERRIER_ASSERT(execution_metric_ != nullptr, "ExecutionMetric not allocated. Check MetricsStore constructor.");
    execution_metric_->RecordExecutionData(feature, len, execution_mode, resource_metrics);
  }

  /**
   * Record metrics for the execution engine when finish a pipeline
   * @param query_id Query Identifier
   * @param pipeline_id Pipeline Identifier
   * @param execution_mode Execution Mode
   * @param features Feature Vector
   * @param resource_metrics Metrics
   */
  void RecordPipelineData(execution::query_id_t query_id, execution::pipeline_id_t pipeline_id, uint8_t execution_mode,
                          std::vector<brain::ExecutionOperatingUnitFeature> &&features,
                          const common::ResourceTracker::Metrics &resource_metrics) {
    TERRIER_ASSERT(ComponentEnabled(MetricsComponent::EXECUTION_PIPELINE), "PipelineMMetric not enabled.");
    TERRIER_ASSERT(pipeline_metric_ != nullptr, "PipelineMetric not allocated. Check MetricsStore constructor.");
    pipeline_metric_->RecordPipelineData(query_id, pipeline_id, execution_mode, std::move(features), resource_metrics);
  }

  /**
   * @param component metrics component to test
   * @return true if metrics enabled for this component, false otherwise
   */
  bool ComponentEnabled(const MetricsComponent component) {
    return enabled_metrics_.test(static_cast<uint8_t>(component));
  }

  /**
   * @param component to be tested
   * @return true if metrics are enabled for this component and we should record this metric according to the sample
   * interval, false otherwise
   */
  bool ComponentToRecord(const MetricsComponent component) {
    auto component_index = static_cast<uint8_t>(component);
    if (!enabled_metrics_.test(component_index)) return false;

    sample_count_[component_index] =
        sample_count_[component_index] >= sample_interval_[component_index] ? 0 : sample_count_[component_index] + 1;

    return sample_count_[component_index] == 0;
  }

  /**
   * MetricsManager pointer that created this MetricsStore
   */
  common::ManagedPointer<metrics::MetricsManager> MetricsManager() const { return metrics_manager_; }

 private:
  friend class MetricsManager;

  const common::ManagedPointer<metrics::MetricsManager> metrics_manager_;

  explicit MetricsStore(common::ManagedPointer<metrics::MetricsManager> metrics_manager,
                        const std::bitset<NUM_COMPONENTS> &enabled_metrics,
                        const std::array<uint32_t, NUM_COMPONENTS> &sampling_masks);

  std::array<std::unique_ptr<AbstractRawData>, NUM_COMPONENTS> GetDataToAggregate();

  std::unique_ptr<LoggingMetric> logging_metric_;
  std::unique_ptr<TransactionMetric> txn_metric_;
  std::unique_ptr<GarbageCollectionMetric> gc_metric_;
  std::unique_ptr<ExecutionMetric> execution_metric_;
  std::unique_ptr<PipelineMetric> pipeline_metric_;

  const std::bitset<NUM_COMPONENTS> &enabled_metrics_;
  const std::array<uint32_t, NUM_COMPONENTS> &sample_interval_;
  std::array<uint32_t, NUM_COMPONENTS> sample_count_{0};
};

}  // namespace terrier::metrics
