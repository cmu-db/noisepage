#include "metrics/metrics_store.h"
#include <bitset>
#include <memory>
#include <vector>
#include "metrics/logging_metric.h"
#include "metrics/metrics_defs.h"

namespace terrier::metrics {

MetricsStore::MetricsStore(const common::ManagedPointer<metrics::MetricsManager> metrics_manager,
                           const std::bitset<NUM_COMPONENTS> &enabled_metrics)
    : metrics_manager_(metrics_manager), enabled_metrics_{enabled_metrics} {
  logging_metric_ = std::make_unique<LoggingMetric>();
  txn_metric_ = std::make_unique<TransactionMetric>();
  recovery_metric_ = std::make_unique<RecoveryMetric>();
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
        case MetricsComponent::RECOVERY: {
          TERRIER_ASSERT(
              recovery_metric_ != nullptr,
              "RecoveryMetric cannot be a nullptr. Check the MetricsStore constructor that it was allocated.");
          result[component] = recovery_metric_->Swap();
          break;
        }
      }
    }
  }

  return result;
}
}  // namespace terrier::metrics
