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
  explicit GarbageCollectionMetricRawData() : aggregate_data_(transaction::DAF_TAG_COUNT, AggregateData()) {}

  void Aggregate(AbstractRawData *const other) override {
    auto other_db_metric = dynamic_cast<GarbageCollectionMetricRawData *>(other);
    if (!other_db_metric->action_data_.empty()) {
      action_data_.splice(action_data_.cbegin(), other_db_metric->action_data_);
    }

    // aggregate the data by daf type
    for (const auto action : action_data_) {
      if (aggregate_data_.at(int32_t(action.daf_id_)).daf_id_ == transaction::DafId::INVALID) {
        aggregate_data_[int32_t(action.daf_id_)] = {action.resource_metrics_.start_, action.daf_id_, 1, action.resource_metrics_.elapsed_us_};
      } else {
        aggregate_data_[int32_t(action.daf_id_)].num_processed_++;
        aggregate_data_[int32_t(action.daf_id_)].time_elapsed_ += action.resource_metrics_.elapsed_us_;
      }
      //std::cout << aggregate_data_[int32_t(action.daf_id_)].start_ << ", " << aggregate_data_[int32_t(action.daf_id_)].daf_id_ << ", " << aggregate_data_[int32_t(action.daf_id_)].num_processed_ << ", " << aggregate_data_[int32_t(action.daf_id_)].time_elapsed_ << std::endl;
    }
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

    auto &daf_event = (*outfiles)[0];
    auto &daf_agg = (*outfiles)[1];
    auto &daf_count_agg = (*outfiles)[2];
    auto &daf_time_agg = (*outfiles)[3];

    for (const auto &data : action_data_) {
      daf_event << static_cast<int>(data.daf_id_) << ", ";
      data.resource_metrics_.ToCSV(daf_event);
      daf_event << std::endl;
    }
    daf_count_agg << idx;
    daf_time_agg << idx;
    for (const auto &data : aggregate_data_) {
      if (data.daf_id_ != transaction::DafId::INVALID) {
        daf_agg << data.start_ << ", " << static_cast<int>(data.daf_id_) << ", " << data.num_processed_ << ", "<< data.time_elapsed_;
        daf_agg << std::endl;
      }
      daf_count_agg << ", " << static_cast<int>(data.num_processed_);
      daf_time_agg << ", " << static_cast<int>(data.time_elapsed_);
    }
    daf_count_agg << std::endl;
    daf_time_agg << std::endl;
    idx++;
    action_data_.clear();
    auto local_agg_data = std::vector<AggregateData>(transaction::DAF_TAG_COUNT, AggregateData());
    aggregate_data_.swap(local_agg_data);
  }

  /**
   * Files to use for writing to CSV.
   */
  static constexpr std::array<std::string_view, 4> FILES = {"./daf_events.csv", "./daf_aggregate.csv", "./daf_count_agg.csv", "./daf_time_agg.csv"};
  /**
   * Columns to use for writing to CSV.
   * Note: This includes the columns for the input feature, but not the output (resource counters)
   */
  static constexpr std::array<std::string_view, 4> FEATURE_COLUMNS = {"daf_id",
                                                                      "start_time, daf_id, num_processed, time_elapsed",
                                                                      "idx, MEMORY_DEALLOCATION, CATALOG_TEARDOWN, INDEX_REMOVE_KEY, COMPACTION, LOG_RECORD_REMOVAL, TXN_REMOVAL, UNLINK",
                                                                      "idx, MEMORY_DEALLOCATION, CATALOG_TEARDOWN, INDEX_REMOVE_KEY, COMPACTION, LOG_RECORD_REMOVAL, TXN_REMOVAL, UNLINK"};

 private:
  friend class GarbageCollectionMetric;
  FRIEND_TEST(MetricsTests, LoggingCSVTest);

  void RecordActionData(const transaction::DafId daf_id, const common::ResourceTracker::Metrics &resource_metrics) {
    action_data_.emplace_front(daf_id, resource_metrics);
  }

  struct ActionData {
    ActionData(const transaction::DafId daf_id, const common::ResourceTracker::Metrics &resource_metrics)
        : daf_id_(daf_id), resource_metrics_(resource_metrics) {}
    const transaction::DafId daf_id_;
    const common::ResourceTracker::Metrics resource_metrics_;
  };

  struct AggregateData {
    AggregateData() : start_(0), daf_id_(transaction::DafId::INVALID), num_processed_(0), time_elapsed_(0) {}

    AggregateData(const uint64_t start, const transaction::DafId daf_id, const uint64_t num_processed,
               const uint64_t time_elapsed)
        : start_(start),
          daf_id_(daf_id),
          num_processed_(num_processed),
          time_elapsed_(time_elapsed) {}

    AggregateData(const AggregateData &other) = default;
    uint64_t start_;
    transaction::DafId daf_id_;
    uint64_t num_processed_;
    uint64_t time_elapsed_;
  };

  std::list<ActionData> action_data_;
  std::vector<AggregateData> aggregate_data_;

  int idx = 0;
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
};
}  // namespace terrier::metrics
