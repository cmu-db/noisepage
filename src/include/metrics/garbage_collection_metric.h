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
 * Raw data object for holding stats collected for the garbage collection
 */
class GarbageCollectionMetricRawData : public AbstractRawData {
 public:
  GarbageCollectionMetricRawData() : aggregate_data_(transaction::DAF_TAG_COUNT, AggregateData()) {}

  void Aggregate(AbstractRawData *const other) override {
    auto other_db_metric = dynamic_cast<GarbageCollectionMetricRawData *>(other);

    std::list<ActionData> local;
    other_db_metric->action_data_.swap(local);

    // aggregate the data by DAF type
    for (const auto action : local) {
      if (aggregate_data_.at(int32_t(action.daf_id_)).daf_id_ == transaction::DafId::INVALID) {
        aggregate_data_[int32_t(action.daf_id_)] = {action.daf_id_, 1};
      } else {
        aggregate_data_[int32_t(action.daf_id_)].num_actions_processed_++;
      }
    }
    // Record max_queue_length
    if (max_queue_length_ < other_db_metric->max_queue_length_) {
      max_queue_length_ = other_db_metric->max_queue_length_;
    }
    other_db_metric->max_queue_length_ = -1;

    // record number of transaction processed
    num_txns_processed_ += other_db_metric->num_txns_processed_;
    other_db_metric->num_txns_processed_ = -1;
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
    NOISEPAGE_ASSERT(outfiles->size() == FILES.size(), "Number of files passed to metric is wrong.");
    NOISEPAGE_ASSERT(std::count_if(outfiles->cbegin(), outfiles->cend(),
                                   [](const std::ofstream &outfile) { return !outfile.is_open(); }) == 0,
                     "Not all files are open.");

    // get the corresponding number of txn processed in this interval.
    auto &daf_count_agg = (*outfiles)[0];

    start_ = metrics::MetricsUtil::Now();
    daf_count_agg << (start_);
    int total_processed = 0;
    common::ResourceTracker::Metrics resource_metrics = {};
    for (const auto &data : aggregate_data_) {
      total_processed += data.num_actions_processed_;
      daf_count_agg << ", " << (data.num_actions_processed_);
    }
    daf_count_agg << ", " << (total_processed) << ", " << (max_queue_length_) << ", " << (num_txns_processed_) << ", ";
    resource_metrics.ToCSV(daf_count_agg);
    daf_count_agg << std::endl;

    max_queue_length_ = -1;
    num_txns_processed_ = -1;
    action_data_.clear();
    auto local_agg_data = std::vector<AggregateData>(transaction::DAF_TAG_COUNT, AggregateData());
    aggregate_data_.swap(local_agg_data);
  }

  /**
   * Files to use for writing to CSV.
   */
  static constexpr std::array<std::string_view, 1> FILES = {"./daf_count_agg.csv"};
  /**
   * Columns to use for writing to CSV.
   * Note: This includes the columns for the input feature, but not the output (resource counters)
   */
  static constexpr std::array<std::string_view, 1> FEATURE_COLUMNS = {
      "start_time, MEMORY_DEALLOCATION, CATALOG_TEARDOWN, INDEX_REMOVE_KEY, COMPACTION, LOG_RECORD_REMOVAL, "
      "TXN_REMOVAL, "
      "UNLINK, INVALID, total_num_actions, max_queue_size, total_num_txns"};

 private:
  friend class GarbageCollectionMetric;
  FRIEND_TEST(MetricsTests, LoggingCSVTest);

  void RecordActionData(const transaction::DafId daf_id) { action_data_.emplace_front(daf_id); }

  void RecordQueueSize(const int UNUSED_ATTRIBUTE queue_length) {
    if (max_queue_length_ < queue_length) {
      max_queue_length_ = queue_length;
    }
  }

  void RecordTxnsProcessed() {
    if (num_txns_processed_ == -1)
      num_txns_processed_ = 1;
    else
      num_txns_processed_++;
  }

  // Leave it here so that we can easily collect resource metrics for individual action if needed
  struct ActionData {
    explicit ActionData(const transaction::DafId daf_id) : daf_id_(daf_id) {}
    const transaction::DafId daf_id_;
  };

  struct AggregateData {
    AggregateData() = default;

    AggregateData(const transaction::DafId daf_id, const int num_processed)
        : daf_id_(daf_id), num_actions_processed_(num_processed) {}

    AggregateData(const AggregateData &other) = default;
    transaction::DafId daf_id_{transaction::DafId::INVALID};
    int num_actions_processed_{0};
  };

  std::list<ActionData> action_data_;
  std::vector<AggregateData> aggregate_data_;

  uint64_t start_ = 0;

  /** Initialize to -1 for periods with no data point collected **/
  int max_queue_length_ = -1;
  int num_txns_processed_ = -1;
};

/**
 * Metrics for the garbage collection components of the system: currently deallocation and unlinking
 */
class GarbageCollectionMetric : public AbstractMetric<GarbageCollectionMetricRawData> {
 private:
  friend class MetricsStore;

  void RecordActionData(const transaction::DafId daf_id) { GetRawData()->RecordActionData(daf_id); }

  void RecordQueueSize(const int queue_size) { GetRawData()->RecordQueueSize(queue_size); }

  void RecordTxnsProcessed() { GetRawData()->RecordTxnsProcessed(); }
};
}  // namespace noisepage::metrics
