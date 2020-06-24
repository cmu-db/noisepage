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
  void PrintNeeded() override { std::cout << num_txns_processed_ << std::endl; }
  void Aggregate(AbstractRawData *const other) override {
    auto other_db_metric = dynamic_cast<GarbageCollectionMetricRawData *>(other);
    other_db_metric->latch_.Lock();
//    if (!other_db_metric->action_data_.empty()) {
//      action_data_.splice(action_data_.cbegin(), other_db_metric->action_data_);
//    }

    // aggregate the data by daf type
    for (const auto action : other_db_metric->action_data_) {
      if (aggregate_data_.at(int32_t(action.daf_id_)).daf_id_ == transaction::DafId::INVALID) {
        aggregate_data_[int32_t(action.daf_id_)] = {action.daf_id_, 1, action.resource_metrics_};
      } else {
        aggregate_data_[int32_t(action.daf_id_)].num_actions_processed_++;
        aggregate_data_[int32_t(action.daf_id_)].resource_metrics_ += action.resource_metrics_;
      }
    }

    if (before_queue_length_ < other_db_metric->before_queue_length_) {
      before_queue_length_.store(other_db_metric->before_queue_length_.exchange(0));
    }

    if (after_queue_length_ < other_db_metric->after_queue_length_) {
      after_queue_length_.store(other_db_metric->after_queue_length_.exchange(0));
    }
//      before_queue_length_ += other_db_metric->before_queue_length_.exchange(0);

//    std::cout << "before record " << num_txns_processed_ << std::endl;
    num_daf_wakeup_ += other_db_metric->num_daf_wakeup_.exchange(0);

    num_txns_processed_ += other_db_metric->num_txns_processed_.exchange(0);
//    std::cout << "record " << num_txns_processed_ << std::endl;
    other_db_metric->latch_.Unlock();

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

    latch_.Lock();
    // get the corresponding number of txn processed in this interval.
    auto &daf_count_agg = (*outfiles)[0];
    auto &daf_time_agg = (*outfiles)[1];

//    for (const auto &data : action_data_) {
//      daf_event << static_cast<int>(data.daf_id_) << ", ";
//      data.resource_metrics_.ToCSV(daf_event);
//      daf_event << std::endl;
//    }
    start_ = metrics::MetricsUtil::Now() % 1000000000;
    daf_count_agg << (start_);
    daf_time_agg << (start_);
    int total_processed = 0;
    int total_elapsed = 0;
    common::ResourceTracker::Metrics resource_metrics = {};
    for (const auto &data : aggregate_data_) {
      if (data.daf_id_ != transaction::DafId::INVALID) {
        resource_metrics += data.resource_metrics_;
      }
//      total_processed += data.num_actions_processed_;
      total_elapsed += data.resource_metrics_.elapsed_us_;
      daf_count_agg << ", " << (data.num_actions_processed_);
      daf_time_agg << ", " << (data.resource_metrics_.elapsed_us_);
    }
    daf_count_agg << ", " << (total_processed) << ", " << (before_queue_length_) << ", " << (after_queue_length_) << ", " << (num_daf_wakeup_) << ", " << (num_txns_processed_) << ", ";
    resource_metrics.ToCSV(daf_count_agg);
    daf_count_agg << std::endl;
    daf_time_agg << ", " << (total_elapsed) << ", "  << (num_daf_wakeup_) << ", " << (num_txns_processed_) << ", ";
    resource_metrics.ToCSV(daf_time_agg);
    daf_time_agg << std::endl;

    num_daf_wakeup_ = 0;
    before_queue_length_ = 0;
    after_queue_length_ = 0;
    num_txns_processed_ = 0;
    action_data_.clear();
    auto local_agg_data = std::vector<AggregateData>(transaction::DAF_TAG_COUNT, AggregateData());
    aggregate_data_.swap(local_agg_data);
    latch_.Unlock();
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
      "UNLINK, INVALID, total_num_actions, before_queue_length, after_queue_length, num_daf_wakeup, total_num_txns",
      "start_time, MEMORY_DEALLOCATION, CATALOG_TEARDOWN, INDEX_REMOVE_KEY, COMPACTION, LOG_RECORD_REMOVAL, TXN_REMOVAL, "
      "UNLINK, INVALID, total_time, num_daf_wakeup, total_num_txns"};

 private:
  friend class GarbageCollectionMetric;
  FRIEND_TEST(MetricsTests, LoggingCSVTest);

  void RecordActionData(const transaction::DafId daf_id, const common::ResourceTracker::Metrics &resource_metrics) {
    latch_.Lock();
    action_data_.emplace_front(daf_id, resource_metrics);
    latch_.Unlock();
  }

  void RecordQueueSize(const int UNUSED_ATTRIBUTE queue_size) {
    if (before_queue_length_ < queue_size) {
      before_queue_length_.store(queue_size);
    }
  }

  void RecordAfterQueueSize(const int UNUSED_ATTRIBUTE queue_size) {
    if (after_queue_length_ < queue_size) {
      after_queue_length_.store(queue_size);
    }
  }

  void RecordTxnsProcessed() {
    latch_.Lock();

    num_txns_processed_++;
    latch_.Unlock();

  }

  void RecordDafWakeup() {
    num_daf_wakeup_++;
  }

  bool CheckWakeUp() {
    if (num_daf_wakeup_ == 0)
      num_daf_wakeup_.store(9999);
    return num_daf_wakeup_ > 0;
  }

  struct ActionData {
    ActionData(const transaction::DafId daf_id, const common::ResourceTracker::Metrics &resource_metrics)
        : start_(metrics::MetricsUtil::Now()), daf_id_(daf_id), resource_metrics_(resource_metrics) {}
    const int start_;
    const transaction::DafId daf_id_;
    const common::ResourceTracker::Metrics resource_metrics_;
  };

  struct AggregateData {
    AggregateData() = default;

    AggregateData(const transaction::DafId daf_id, const int num_processed, const common::ResourceTracker::Metrics &resource_metrics)
        : daf_id_(daf_id), num_actions_processed_(num_processed), resource_metrics_(resource_metrics) {}

    AggregateData(const AggregateData &other) = default;
//    int start_{0};
    transaction::DafId daf_id_{transaction::DafId::INVALID};
    int num_actions_processed_{0};
//    uint64_t time_elapsed_{0};
    common::ResourceTracker::Metrics resource_metrics_;
  };

  std::list<ActionData> action_data_;
  std::vector<AggregateData> aggregate_data_;

  uint64_t start_ = 0;

  std::atomic<int> before_queue_length_ = 0;
  std::atomic<int> after_queue_length_ = 0;

  std::atomic<int> num_txns_processed_ = 0;
  std::atomic<int> num_daf_wakeup_ = 0;
  common::SpinLatch latch_;
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

  void RecordQueueSize(const int queue_size) {
    GetRawData()->RecordQueueSize(queue_size);
  }

  void RecordAfterQueueSize(const int queue_size) {
    GetRawData()->RecordAfterQueueSize(queue_size);
  }

  void RecordTxnsProcessed() {
    GetRawData()->RecordTxnsProcessed();
  }

  void RecordDafWakeup() {
    GetRawData()->RecordDafWakeup();
  }

  bool CheckWakeUp() {
    return GetRawData()->CheckWakeUp();
  }
};
}  // namespace terrier::metrics
