#pragma once

#include <bitset>
#include <memory>
#include <unordered_map>
#include <vector>
#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "metrics/abstract_metric.h"
#include "metrics/abstract_raw_data.h"
#include "metrics/logging_metric.h"
#include "metrics/metrics_defs.h"
#include "metrics/recovery_metric.h"
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
   * @param elapsed_us first entry of metrics datapoint
   * @param num_bytes second entry of metrics datapoint
   * @param num_records third entry of metrics datapoint
   */
  void RecordSerializerData(const uint64_t elapsed_us, const uint64_t num_bytes, const uint64_t num_records) {
    TERRIER_ASSERT(ComponentEnabled(MetricsComponent::LOGGING), "LoggingMetric not enabled.");
    TERRIER_ASSERT(logging_metric_ != nullptr, "LoggingMetric not allocated. Check MetricsStore constructor.");
    logging_metric_->RecordSerializerData(elapsed_us, num_bytes, num_records);
  }

  /**
   * Record metrics from the LogConsumerTask
   * @param write_us first entry of metrics datapoint
   * @param persist_us second entry of metrics datapoint
   * @param num_bytes third entry of metrics datapoint
   * @param num_records fourth entry of metrics datapoint
   */
  void RecordConsumerData(const uint64_t write_us, const uint64_t persist_us, const uint64_t num_bytes,
                          const uint64_t num_records) {
    TERRIER_ASSERT(ComponentEnabled(MetricsComponent::LOGGING), "LoggingMetric not enabled.");
    TERRIER_ASSERT(logging_metric_ != nullptr, "LoggingMetric not allocated. Check MetricsStore constructor.");
    logging_metric_->RecordConsumerData(write_us, persist_us, num_bytes, num_records);
  }

  /**
   * Record metrics for transaction manager when beginning transaction
   * @param elapsed_us first entry of txn datapoint
   * @param txn_start second entry of txn datapoint
   */
  void RecordBeginData(const uint64_t elapsed_us, const transaction::timestamp_t txn_start) {
    TERRIER_ASSERT(ComponentEnabled(MetricsComponent::TRANSACTION), "TransactionMetric not enabled.");
    TERRIER_ASSERT(txn_metric_ != nullptr, "TransactionMetric not allocated. Check MetricsStore constructor.");
    txn_metric_->RecordBeginData(elapsed_us, txn_start);
  }

  /**
   * Record metrics for transaction manager when ending transaction
   * @param elapsed_us first entry of txn datapoint
   * @param txn_start second entry of txn datapoint
   */
  void RecordCommitData(const uint64_t elapsed_us, const transaction::timestamp_t txn_start) {
    TERRIER_ASSERT(ComponentEnabled(MetricsComponent::TRANSACTION), "TransactionMetric not enabled.");
    TERRIER_ASSERT(txn_metric_ != nullptr, "TransactionMetric not allocated. Check MetricsStore constructor.");
    txn_metric_->RecordCommitData(elapsed_us, txn_start);
  }

  /**
   * Record throughput metrics for recovery manager during recovery
   * @param num_txns number of transactions
   * @param num_bytes size of log files processed
   * @param elapsed_us time taken to execute the transactions
   */
  void RecordRecoveryData(const uint64_t num_txns, const uint64_t num_bytes, const uint64_t elapsed_us) {
    TERRIER_ASSERT(ComponentEnabled(MetricsComponent::RECOVERY), "RecoveryMetric not enabled.");
    TERRIER_ASSERT(txn_metric_ != nullptr, "RecoveryMetric not allocated. Check MetricsStore constructor.");
    recovery_metric_->RecordRecoveryData(num_txns, num_bytes, elapsed_us);
  }

  /**
   * @param component metrics component to test
   * @return true if metrics enabled for this component, false otherwise
   */
  bool ComponentEnabled(const MetricsComponent component) {
    return enabled_metrics_.test(static_cast<uint8_t>(component));
  }

  /**
   * MetricsManager pointer that created this MetricsStore
   */
  common::ManagedPointer<metrics::MetricsManager> MetricsManager() const { return metrics_manager_; }

 private:
  friend class MetricsManager;

  const common::ManagedPointer<metrics::MetricsManager> metrics_manager_;

  explicit MetricsStore(common::ManagedPointer<metrics::MetricsManager> metrics_manager,
                        const std::bitset<NUM_COMPONENTS> &enabled_metrics);

  std::array<std::unique_ptr<AbstractRawData>, NUM_COMPONENTS> GetDataToAggregate();

  std::unique_ptr<LoggingMetric> logging_metric_;
  std::unique_ptr<TransactionMetric> txn_metric_;
  std::unique_ptr<RecoveryMetric> recovery_metric_;

  const std::bitset<NUM_COMPONENTS> &enabled_metrics_;
};

}  // namespace terrier::metrics
