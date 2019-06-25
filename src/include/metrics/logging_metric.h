#pragma once

#include <chrono>  //NOLINT
#include <forward_list>
#include <utility>

#include "catalog/catalog_defs.h"
#include "metrics/abstract_metric.h"
#include "transaction/transaction_defs.h"

namespace terrier::metrics {

/**
 * Raw data object for holding stats collected at logging level
 */
class LoggingMetricRawData : public AbstractRawData {
 public:
  void Aggregate(AbstractRawData *other) override {
    //    auto other_db_metric = dynamic_cast<LoggingMetricRawData *>(other);
    //    serializer_data_.splice_after()
  }

  /**
   * @return the type of the metric this object is holding the data for
   */
  MetricsComponent GetMetricType() const override { return MetricsComponent::LOGGING; }

  void RecordSerializerData(const uint64_t elapsed_ns, const uint64_t num_bytes, const uint64_t num_records) {
    serializer_data_.emplace_front(elapsed_ns, num_bytes, num_records);
  }

  void RecordConsumerData(const uint64_t elapsed_ns, const uint64_t num_bytes, const uint64_t num_records) {
    consumer_data_.emplace_front(elapsed_ns, num_bytes, num_records);
  }

 private:
  struct LoggingData {
    LoggingData(const uint64_t elapsed_ns, const uint64_t num_bytes, const uint64_t num_records)
        : elapsed_ns_(elapsed_ns), num_bytes_(num_bytes), num_records_(num_records) {}
    const uint64_t elapsed_ns_;
    const uint64_t num_bytes_;
    const uint64_t num_records_;
  };

  std::forward_list<LoggingData> serializer_data_;
  std::forward_list<LoggingData> consumer_data_;
};

class LoggingMetric : public AbstractMetric<LoggingMetricRawData> {
 public:
  void OnLogSerialize(const uint64_t elapsed_ns, const uint64_t num_bytes, const uint64_t num_records) final {}
  void OnLogConsume(const uint64_t elapsed_ns, const uint64_t num_bytes, const uint64_t num_records) final {}
};
}  // namespace terrier::metrics
