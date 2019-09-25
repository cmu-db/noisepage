#pragma once

#include <algorithm>
#include <chrono>  //NOLINT
#include <fstream>
#include <list>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "metrics/abstract_metric.h"
#include "metrics/metrics_util.h"
#include "transaction/transaction_defs.h"

namespace terrier::metrics {

/**
 * Raw data object for holding stats collected at logging level
 */
class LoggingMetricRawData : public AbstractRawData {
 public:
  void Aggregate(AbstractRawData *const other) override {
    auto other_db_metric = dynamic_cast<LoggingMetricRawData *>(other);
    if (!other_db_metric->serializer_data_.empty()) {
      serializer_data_.splice(serializer_data_.cbegin(), other_db_metric->serializer_data_);
    }
    if (!other_db_metric->consumer_data_.empty()) {
      consumer_data_.splice(consumer_data_.cbegin(), other_db_metric->consumer_data_);
    }
  }

  /**
   * @return the type of the metric this object is holding the data for
   */
  MetricsComponent GetMetricType() const override { return MetricsComponent::LOGGING; }

  /**
   * Writes the data out to ofstreams
   * @param outfiles vector of ofstreams to write to that have been opened by the MetricsManager
   */
  void ToCSV(std::vector<std::ofstream> *const outfiles) final {
    TERRIER_ASSERT(outfiles->size() == FILES.size(), "Number of files passed to metric is wrong.");
    TERRIER_ASSERT(std::count_if(outfiles->cbegin(), outfiles->cend(),
                                 [](const std::ofstream &outfile) { return !outfile.is_open(); }) == 0,
                   "Not all files are open.");

    for (const auto &data : serializer_data_) {
      ((*outfiles)[0]) << data.now_ << "," << data.elapsed_us_ << "," << data.num_bytes_ << "," << data.num_records_
                       << std::endl;
    }
    for (const auto &data : consumer_data_) {
      ((*outfiles)[1]) << data.now_ << "," << data.write_us_ << "," << data.persist_us_ << "," << data.num_bytes_ << ","
                       << data.num_buffers_ << std::endl;
    }
    serializer_data_.clear();
    consumer_data_.clear();
  }

  /**
   * Files to use for writing to CSV.
   */
  static constexpr std::array<std::string_view, 2> FILES = {"./log_serializer_task.csv",
                                                            "./disk_log_consumer_task.csv"};
  /**
   * Columns to use for writing to CSV.
   */
  static constexpr std::array<std::string_view, 2> COLUMNS = {"now,elapsed_us,num_bytes,num_records",
                                                              "now,write_us,persist_us,num_bytes,num_buffers"};

 private:
  friend class LoggingMetric;
  FRIEND_TEST(MetricsTests, LoggingCSVTest);

  void RecordSerializerData(const uint64_t elapsed_us, const uint64_t num_bytes, const uint64_t num_records) {
    serializer_data_.emplace_front(elapsed_us, num_bytes, num_records);
  }

  void RecordConsumerData(const uint64_t write_us, const uint64_t persist_us, const uint64_t num_bytes,
                          const uint64_t num_buffers) {
    consumer_data_.emplace_front(write_us, persist_us, num_bytes, num_buffers);
  }

  struct SerializerData {
    SerializerData(const uint64_t elapsed_us, const uint64_t num_bytes, const uint64_t num_records)
        : now_(MetricsUtil::Now()), elapsed_us_(elapsed_us), num_bytes_(num_bytes), num_records_(num_records) {}
    const uint64_t now_;
    const uint64_t elapsed_us_;
    const uint64_t num_bytes_;
    const uint64_t num_records_;
  };

  struct ConsumerData {
    ConsumerData(const uint64_t write_us, const uint64_t persist_us, const uint64_t num_bytes,
                 const uint64_t num_buffers)
        : now_(MetricsUtil::Now()),
          write_us_(write_us),
          persist_us_(persist_us),
          num_bytes_(num_bytes),
          num_buffers_(num_buffers) {}
    const uint64_t now_;
    const uint64_t write_us_;
    const uint64_t persist_us_;
    const uint64_t num_bytes_;
    const uint64_t num_buffers_;
  };

  std::list<SerializerData> serializer_data_;
  std::list<ConsumerData> consumer_data_;
};

/**
 * Metrics for the logging components of the system: currently buffer consumer (writes to disk) and the record
 * serializer
 */
class LoggingMetric : public AbstractMetric<LoggingMetricRawData> {
 private:
  friend class MetricsStore;

  void RecordSerializerData(const uint64_t elapsed_us, const uint64_t num_bytes, const uint64_t num_records) {
    GetRawData()->RecordSerializerData(elapsed_us, num_bytes, num_records);
  }
  void RecordConsumerData(const uint64_t write_us, const uint64_t persist_us, const uint64_t num_bytes,
                          const uint64_t num_buffers) {
    GetRawData()->RecordConsumerData(write_us, persist_us, num_bytes, num_buffers);
  }
};
}  // namespace terrier::metrics
