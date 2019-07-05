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
#include "metrics/metric_defs.h"

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
   * @param elapsed_ns first entry of metrics datapoint
   * @param num_bytes second entry of metrics datapoint
   * @param num_records third entry of metrics datapoint
   */
  void RecordSerializerData(const uint64_t elapsed_ns, const uint64_t num_bytes, const uint64_t num_records) {
    if (ComponentEnabled(MetricsComponent::LOGGING))
      logging_metric_->RecordSerializerData(elapsed_ns, num_bytes, num_records);
  }

  /**
   * Record metrics from the LogConsumerTask
   * @param write_ns first entry of metrics datapoint
   * @param persist_ns second entry of metrics datapoint
   * @param num_bytes third entry of metrics datapoint
   * @param num_records fourth entry of metrics datapoint
   */
  void RecordConsumerData(const uint64_t write_ns, const uint64_t persist_ns, const uint64_t num_bytes,
                          const uint64_t num_records) {
    if (ComponentEnabled(MetricsComponent::LOGGING))
      logging_metric_->RecordConsumerData(write_ns, persist_ns, num_bytes, num_records);
  }

 private:
  bool ComponentEnabled(const MetricsComponent component) {
    return enabled_metrics_.test(static_cast<uint8_t>(component));
  }

  friend class MetricsManager;

  explicit MetricsStore(const std::bitset<NUM_COMPONENTS> &enabled_metrics);

  std::array<std::unique_ptr<AbstractRawData>, NUM_COMPONENTS> GetDataToAggregate();

  std::unique_ptr<LoggingMetric> logging_metric_;

  const std::bitset<NUM_COMPONENTS> &enabled_metrics_;
};

}  // namespace terrier::metrics
