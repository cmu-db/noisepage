#pragma once

#include <algorithm>
#include <chrono>  //NOLINT
#include <fstream>
#include <list>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/resource_tracker.h"
#include "metrics/abstract_metric.h"
#include "metrics/metrics_util.h"
#include "transaction/transaction_defs.h"

namespace noisepage::metrics {

/**
 * Raw data object for holding stats collected at logging level
 */
class LoggingMetricRawData : public AbstractRawData {
 public:
  void Aggregate(AbstractRawData *const other) override {
    auto other_db_metric = dynamic_cast<LoggingMetricRawData *>(other);
    if (!other_db_metric->serializer_data_.empty()) {
      serializer_data_.splice(serializer_data_.cend(), other_db_metric->serializer_data_);
    }
    if (!other_db_metric->consumer_data_.empty()) {
      consumer_data_.splice(consumer_data_.cend(), other_db_metric->consumer_data_);
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
    NOISEPAGE_ASSERT(outfiles->size() == FILES.size(), "Number of files passed to metric is wrong.");
    NOISEPAGE_ASSERT(std::count_if(outfiles->cbegin(), outfiles->cend(),
                                   [](const std::ofstream &outfile) { return !outfile.is_open(); }) == 0,
                     "Not all files are open.");

    auto &serializer_outfile = (*outfiles)[0];
    auto &consumer_outfile = (*outfiles)[1];

    for (const auto &data : serializer_data_) {
      serializer_outfile << data.num_bytes_ << ", " << data.num_records_ << ", " << data.num_txns_ << ", "
                         << data.interval_ << ", ";
      data.resource_metrics_.ToCSV(serializer_outfile);
      serializer_outfile << std::endl;
    }
    for (const auto &data : consumer_data_) {
      consumer_outfile << data.num_bytes_ << ", " << data.num_buffers_ << ", " << data.interval_ << ", ";
      data.resource_metrics_.ToCSV(consumer_outfile);
      consumer_outfile << std::endl;
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
   * Note: This includes the columns for the input feature, but not the output (resource counters)
   */
  static constexpr std::array<std::string_view, 2> FEATURE_COLUMNS = {"num_bytes, num_records, num_txns, interval",
                                                                      "num_bytes, num_buffers, interval"};

 private:
  friend class LoggingMetric;
  FRIEND_TEST(MetricsTests, LoggingCSVTest);

  void RecordSerializerData(const uint64_t num_bytes, const uint64_t num_records, const uint64_t num_txns,
                            const uint64_t interval, const common::ResourceTracker::Metrics &resource_metrics) {
    serializer_data_.emplace_back(num_bytes, num_records, num_txns, interval, resource_metrics);
  }

  void RecordConsumerData(const uint64_t num_bytes, const uint64_t num_buffers, const uint64_t interval,
                          const common::ResourceTracker::Metrics &resource_metrics) {
    consumer_data_.emplace_back(num_bytes, num_buffers, interval, resource_metrics);
  }

  struct SerializerData {
    SerializerData(const uint64_t num_bytes, const uint64_t num_records, const uint64_t num_txns,
                   const uint64_t interval, const common::ResourceTracker::Metrics &resource_metrics)
        : num_bytes_(num_bytes),
          num_records_(num_records),
          num_txns_(num_txns),
          interval_(interval),
          resource_metrics_(resource_metrics) {}
    const uint64_t num_bytes_;
    const uint64_t num_records_;
    const uint64_t num_txns_;
    const uint64_t interval_;
    const common::ResourceTracker::Metrics resource_metrics_;
  };

  struct ConsumerData {
    ConsumerData(const uint64_t num_bytes, const uint64_t num_buffers, const uint64_t interval,
                 const common::ResourceTracker::Metrics &resource_metrics)
        : num_bytes_(num_bytes), num_buffers_(num_buffers), interval_(interval), resource_metrics_(resource_metrics) {}
    const uint64_t num_bytes_;
    const uint64_t num_buffers_;
    const uint64_t interval_;
    const common::ResourceTracker::Metrics resource_metrics_;
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

  void RecordSerializerData(const uint64_t num_bytes, const uint64_t num_records, const uint64_t num_txns,
                            const uint64_t interval, const common::ResourceTracker::Metrics &resource_metrics) {
    GetRawData()->RecordSerializerData(num_bytes, num_records, num_txns, interval, resource_metrics);
  }
  void RecordConsumerData(const uint64_t num_bytes, const uint64_t num_buffers, const uint64_t interval,
                          const common::ResourceTracker::Metrics &resource_metrics) {
    GetRawData()->RecordConsumerData(num_bytes, num_buffers, interval, resource_metrics);
  }
};
}  // namespace noisepage::metrics
