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

namespace terrier::metrics {

/**
 * Raw data object for holding stats collected for the garbage collection
 */
class GarbageCollectionMetricRawData : public AbstractRawData {
 public:
  GarbageCollectionMetricRawData() : aggregate_data_(transaction::DAF_TAG_COUNT, AggregateData()) {}

  /**
   * Currently used in metrics thread to record the start time of each metric interval
   */
  void SetGCMetricsWakeUpTime() override { start_ = metrics::MetricsUtil::Now(); }

  void Aggregate(AbstractRawData *const other) override {
    auto other_db_metric = dynamic_cast<GarbageCollectionMetricRawData *>(other);
    if (!other_db_metric->action_data_.empty()) {
      action_data_.splice(action_data_.cbegin(), other_db_metric->action_data_);
    }

    // aggregate the data by daf type
    for (const auto action : action_data_) {
      if (aggregate_data_.at(int32_t(action.daf_id_)).daf_id_ == transaction::DafId::INVALID) {
        aggregate_data_[int32_t(action.daf_id_)] = {action.resource_metrics_.start_, action.daf_id_, 1,
                                                    action.resource_metrics_.elapsed_us_};
      } else {
        aggregate_data_[int32_t(action.daf_id_)].num_processed_++;
        aggregate_data_[int32_t(action.daf_id_)].time_elapsed_ += action.resource_metrics_.elapsed_us_;
      }
    }
    max_queue_length_ = other_db_metric->max_queue_length_;
  }

  /**
   * @return the type of the metric this object is holding the data for
   */
  MetricsComponent GetMetricType() const override { return MetricsComponent::GARBAGECOLLECTION; }

  /**
   * Writes the data out to ofstreams
   * @param outfiles vector of ofstreams to write to that have been opened by the MetricsManager
   */
  void ToCSV(std::vector<std::ofstream> *const outfiles) final {
    TERRIER_ASSERT(outfiles->size() == FILES.size(), "Number of files passed to metric is wrong.");
    TERRIER_ASSERT(std::count_if(outfiles->cbegin(), outfiles->cend(),
                                 [](const std::ofstream &outfile) { return !outfile.is_open(); }) == 0,
                   "Not all files are open.");
//    std::cout << "to csv " << static_cast<unsigned long>(max_queue_length_) << std::endl;

    auto &daf_count_agg = (*outfiles)[0];
    auto &daf_time_agg = (*outfiles)[1];

//    for (const auto &data : action_data_) {
//      daf_event << static_cast<int>(data.daf_id_) << ", ";
//      data.resource_metrics_.ToCSV(daf_event);
//      daf_event << std::endl;
//    }
    daf_count_agg << static_cast<unsigned long>(start_);
    daf_time_agg << static_cast<unsigned long>(start_);
    uint64_t total_processed = 0;
    uint64_t total_elapsed = 0;
    for (const auto &data : aggregate_data_) {
//      if (data.daf_id_ != transaction::DafId::INVALID) {
//        daf_agg << data.start_ << ", " << static_cast<int>(data.daf_id_) << ", " << data.num_processed_ << ", "
//                << data.time_elapsed_;
//        daf_agg << std::endl;
//      }
      total_processed += data.num_processed_;
      total_elapsed += data.time_elapsed_;
      daf_count_agg << ", " << static_cast<unsigned long>(data.num_processed_);
      daf_time_agg << ", " << static_cast<unsigned long>(data.time_elapsed_);
    }
    daf_count_agg << ", " << static_cast<unsigned long>(total_processed) << ", " << static_cast<unsigned long>(max_queue_length_) << std::endl;
    daf_time_agg << ", " << static_cast<unsigned long>(total_elapsed) << std::endl;

    max_queue_length_ = 0;
    action_data_.clear();
    auto local_agg_data = std::vector<AggregateData>(transaction::DAF_TAG_COUNT, AggregateData());
    aggregate_data_.swap(local_agg_data);
  }

  /**
   * Files to use for writing to CSV.
   */
//  static constexpr std::array<std::string_view, 4> FILES = {"./daf_events.csv", "./daf_aggregate.csv",
//                                                            "./daf_count_agg.csv", "./daf_time_agg.csv"};
  static constexpr std::array<std::string_view, 2> FILES = {"./daf_count_agg.csv", "./daf_time_agg.csv"};
  /**
   * Columns to use for writing to CSV.
   * Note: This includes the columns for the input feature, but not the output (resource counters)
   */
//  static constexpr std::array<std::string_view, 4> FEATURE_COLUMNS = {
//      "start_time, daf_id, num_processed, time_elapsed",
//      "start_time, MEMORY_DEALLOCATION, CATALOG_TEARDOWN, INDEX_REMOVE_KEY, COMPACTION, LOG_RECORD_REMOVAL, TXN_REMOVAL, "
//      "UNLINK, total_num",
//      "start_time, MEMORY_DEALLOCATION, CATALOG_TEARDOWN, INDEX_REMOVE_KEY, COMPACTION, LOG_RECORD_REMOVAL, TXN_REMOVAL, "
//      "UNLINK, total_time"};
  static constexpr std::array<std::string_view, 2> FEATURE_COLUMNS = {
      "start_time, MEMORY_DEALLOCATION, CATALOG_TEARDOWN, INDEX_REMOVE_KEY, COMPACTION, LOG_RECORD_REMOVAL, TXN_REMOVAL, "
      "UNLINK, INVALID, total_num, max_queue_length",
      "start_time, MEMORY_DEALLOCATION, CATALOG_TEARDOWN, INDEX_REMOVE_KEY, COMPACTION, LOG_RECORD_REMOVAL, TXN_REMOVAL, "
      "UNLINK, INVALID, total_time"};

 private:
  friend class GarbageCollectionMetric;
  FRIEND_TEST(MetricsTests, LoggingCSVTest);

  void RecordActionData(const transaction::DafId daf_id, const common::ResourceTracker::Metrics &resource_metrics) {
    action_data_.emplace_front(daf_id, resource_metrics);
  }

  void RecordQueueSize(const uint32_t queue_size) {
//    std::cout << "before record " << max_queue_length_ << std::endl;
    if (static_cast<uint64_t>(queue_size) > max_queue_length_)
      max_queue_length_ = queue_size;
//    std::cout << "record " << max_queue_length_ << std::endl;
  }
  struct ActionData {
    ActionData(const transaction::DafId daf_id, const common::ResourceTracker::Metrics &resource_metrics)
        : start_(metrics::MetricsUtil::Now()), daf_id_(daf_id), resource_metrics_(resource_metrics) {}
    const uint64_t start_;
    const transaction::DafId daf_id_;
    const common::ResourceTracker::Metrics resource_metrics_;
  };

  struct AggregateData {
    AggregateData() = default;

    AggregateData(const uint64_t start, const transaction::DafId daf_id, const uint64_t num_processed,
                  const uint64_t time_elapsed)
        : start_(start), daf_id_(daf_id), num_processed_(num_processed), time_elapsed_(time_elapsed) {}

    AggregateData(const AggregateData &other) = default;
    uint64_t start_{0};
    transaction::DafId daf_id_{transaction::DafId::INVALID};
    uint64_t num_processed_{0};
    uint64_t time_elapsed_{0};
  };

  std::list<ActionData> action_data_;
  std::vector<AggregateData> aggregate_data_;

  uint64_t start_ = 0;

  uint64_t max_queue_length_ = 0;
};

/**
 * Metrics for the garbage collection components of the system: currently deallocation and unlinking
 */
class GarbageCollectionMetric : public AbstractMetric<GarbageCollectionMetricRawData> {
 private:
  friend class MetricsStore;

  void RecordActionData(const transaction::DafId daf_id, const common::ResourceTracker::Metrics &resource_metrics) {
    GetRawData()->RecordActionData(daf_id, resource_metrics);
  }

  void RecordQueueSize(const uint32_t queue_size) {
    GetRawData()->RecordQueueSize(queue_size);
  }
};
}  // namespace terrier::metrics
