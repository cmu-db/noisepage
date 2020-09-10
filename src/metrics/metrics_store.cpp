#include "metrics/metrics_store.h"

#include <bitset>
#include <memory>
#include <vector>

#include "metrics/logging_metric.h"
#include "metrics/metrics_defs.h"

namespace terrier::metrics {

MetricsStore::MetricsStore(const common::ManagedPointer<metrics::MetricsManager> metrics_manager,
                           const std::bitset<NUM_COMPONENTS> &enabled_metrics,
                           const std::array<uint32_t, NUM_COMPONENTS> &sampling_masks)
    : metrics_manager_(metrics_manager), enabled_metrics_{enabled_metrics}, sample_interval_(sampling_masks) {
  logging_metric_ = std::make_unique<LoggingMetric>();
  txn_metric_ = std::make_unique<TransactionMetric>();
  gc_metric_ = std::make_unique<GarbageCollectionMetric>();
  execution_metric_ = std::make_unique<ExecutionMetric>();
  pipeline_metric_ = std::make_unique<PipelineMetric>();
  bind_command_metric_ = std::make_unique<BindCommandMetric>();
  execute_command_metric_ = std::make_unique<ExecuteCommandMetric>();
  query_trace_metric_ = std::make_unique<QueryTraceMetric>();
}

std::array<std::unique_ptr<AbstractRawData>, NUM_COMPONENTS> MetricsStore::GetDataToAggregate() {
  std::array<std::unique_ptr<AbstractRawData>, NUM_COMPONENTS> result;

  for (uint8_t component = 0; component < NUM_COMPONENTS; component++) {
    if (enabled_metrics_.test(component)) {
      switch (static_cast<MetricsComponent>(component)) {
        case MetricsComponent::LOGGING: {
          TERRIER_ASSERT(
              logging_metric_ != nullptr,
              "LoggingMetric cannot be a nullptr. Check the MetricsStore constructor that it was allocated.");
          result[component] = logging_metric_->Swap();
          break;
        }
        case MetricsComponent::TRANSACTION: {
          TERRIER_ASSERT(
              txn_metric_ != nullptr,
              "TransactionMetric cannot be a nullptr. Check the MetricsStore constructor that it was allocated.");
          result[component] = txn_metric_->Swap();
          break;
        }
        case MetricsComponent::GARBAGECOLLECTION: {
          TERRIER_ASSERT(
              gc_metric_ != nullptr,
              "GarbageCollectionMetric cannot be a nullptr. Check the MetricsStore constructor that it was allocated.");
          result[component] = gc_metric_->Swap();
          break;
        }
        case MetricsComponent::EXECUTION: {
          TERRIER_ASSERT(
              execution_metric_ != nullptr,
              "ExecutionMetric cannot be a nullptr. Check the MetricsStore constructor that it was allocated.");
          result[component] = execution_metric_->Swap();
          break;
        }
        case MetricsComponent::EXECUTION_PIPELINE: {
          TERRIER_ASSERT(
              pipeline_metric_ != nullptr,
              "PipelineMetric cannot be a nullptr. Check the MetricsStore constructor that it was allocated.");
          result[component] = pipeline_metric_->Swap();
          break;
        }
        case MetricsComponent::BIND_COMMAND: {
          TERRIER_ASSERT(
              bind_command_metric_ != nullptr,
              "BindCommandMetric cannot be a nullptr. Check the MetricsStore constructor that it was allocated.");
          result[component] = bind_command_metric_->Swap();
          break;
        }
        case MetricsComponent::EXECUTE_COMMAND: {
          TERRIER_ASSERT(
              execute_command_metric_ != nullptr,
              "ExecuteCommandMetric cannot be a nullptr. Check the MetricsStore constructor that it was allocated.");
          result[component] = execute_command_metric_->Swap();
          break;
        }
        case MetricsComponent::QUERY_TRACE: {
          TERRIER_ASSERT(
              query_trace_metric_ != nullptr,
              "QueryTraceMetric cannot be a nullptr. Check the MetricsStore constructor that it was allocated.");
          result[component] = query_trace_metric_->Swap();
          break;
        }
      }
    }
  }

  return result;
}
}  // namespace terrier::metrics
