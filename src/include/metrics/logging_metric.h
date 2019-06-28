#pragma once

#include <chrono>  //NOLINT
#include <forward_list>
#include <fstream>
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
    auto other_db_metric = dynamic_cast<LoggingMetricRawData *>(other);
    if (!other_db_metric->serializer_data_.empty()) {
      serializer_data_.splice_after(serializer_data_.cbefore_begin(), other_db_metric->serializer_data_,
                                    other_db_metric->serializer_data_.cbefore_begin());
    }
    if (!other_db_metric->consumer_data_.empty()) {
      consumer_data_.splice_after(consumer_data_.cbefore_begin(), other_db_metric->consumer_data_,
                                  other_db_metric->consumer_data_.cbefore_begin());
    }
  }

  /**
   * @return the type of the metric this object is holding the data for
   */
  MetricsComponent GetMetricType() const override { return MetricsComponent::LOGGING; }

  void RecordSerializerData(const uint64_t elapsed_ns, const uint64_t num_bytes, const uint64_t num_records) {
    serializer_data_.emplace_front(elapsed_ns, num_bytes, num_records);
  }

  void RecordConsumerData(const uint64_t write_ns, const uint64_t persist_ns, const uint64_t num_bytes,
                          const uint64_t num_records) {
    consumer_data_.emplace_front(write_ns, persist_ns, num_bytes, num_records);
  }

  void ToCSV(common::ManagedPointer<std::ofstream> outfile) const final {
    TERRIER_ASSERT(outfile->is_open(), "File not opened.");
    for (const auto &data : serializer_data_) {
      (*outfile) << data.elapsed_ns_ << "," << data.num_bytes_ << "," << data.num_records_ << std::endl;
    }
    for (const auto &data : consumer_data_) {
      (*outfile) << data.write_ns_ << "," << data.persist_ns_ << "," << data.num_bytes_ << "," << data.num_records_
                 << std::endl;
    }
  }

 private:
  struct SerializerData {
    SerializerData(const uint64_t elapsed_ns, const uint64_t num_bytes, const uint64_t num_records)
        : elapsed_ns_(elapsed_ns), num_bytes_(num_bytes), num_records_(num_records) {}
    const uint64_t elapsed_ns_;
    const uint64_t num_bytes_;
    const uint64_t num_records_;
  };

  struct ConsumerData {
    ConsumerData(const uint64_t write_ns, const uint64_t persist_ns, const uint64_t num_bytes,
                 const uint64_t num_records)
        : write_ns_(write_ns), persist_ns_(persist_ns), num_bytes_(num_bytes), num_records_(num_records) {}
    const uint64_t write_ns_;
    const uint64_t persist_ns_;
    const uint64_t num_bytes_;
    const uint64_t num_records_;
  };

  std::forward_list<SerializerData> serializer_data_;
  std::forward_list<ConsumerData> consumer_data_;
};

class LoggingMetric : public AbstractMetric<LoggingMetricRawData> {
 public:
  void OnLogSerialize(const uint64_t elapsed_ns, const uint64_t num_bytes, const uint64_t num_records) final {
    GetRawData()->RecordSerializerData(elapsed_ns, num_bytes, num_records);
  }
  void OnLogConsume(const uint64_t write_ns, const uint64_t persist_ns, const uint64_t num_bytes,
                    const uint64_t num_records) final {
    GetRawData()->RecordConsumerData(write_ns, persist_ns, num_bytes, num_records);
  }
};
}  // namespace terrier::metrics
