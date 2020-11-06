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

namespace noisepage::metrics {

/**
 * Raw data object for holding stats collected at transaction level
 */
class TransactionMetricRawData : public AbstractRawData {
 public:
  void Aggregate(AbstractRawData *const other) override {
    auto other_db_metric = dynamic_cast<TransactionMetricRawData *>(other);
    if (!other_db_metric->begin_data_.empty()) {
      begin_data_.splice(begin_data_.cend(), other_db_metric->begin_data_);
    }
    if (!other_db_metric->commit_data_.empty()) {
      commit_data_.splice(commit_data_.cend(), other_db_metric->commit_data_);
    }
  }

  /**
   * @return the type of the metric this object is holding the data for
   */
  MetricsComponent GetMetricType() const override { return MetricsComponent::TRANSACTION; }

  /**
   * Writes the data out to ofstreams
   * @param outfiles vector of ofstreams to write to that have been opened by the MetricsManager
   */
  void ToCSV(std::vector<std::ofstream> *const outfiles) final {
    NOISEPAGE_ASSERT(outfiles->size() == FILES.size(), "Number of files passed to metric is wrong.");
    NOISEPAGE_ASSERT(std::count_if(outfiles->cbegin(), outfiles->cend(),
                                   [](const std::ofstream &outfile) { return !outfile.is_open(); }) == 0,
                     "Not all files are open.");

    auto &begin_outfile = (*outfiles)[0];
    auto &commit_outfile = (*outfiles)[1];

    for (const auto &data : begin_data_) {
      data.resource_metrics_.ToCSV(begin_outfile);
      begin_outfile << std::endl;
    }
    for (const auto &data : commit_data_) {
      commit_outfile << data.is_readonly_ << ", ";
      data.resource_metrics_.ToCSV(commit_outfile);
      commit_outfile << std::endl;
    }
    begin_data_.clear();
    commit_data_.clear();
  }

  /**
   * Files to use for writing to CSV.
   */
  static constexpr std::array<std::string_view, 2> FILES = {"./txn_begin.csv", "./txn_commit.csv"};

  /**
   * Columns to use for writing to CSV.
   */
  static constexpr std::array<std::string_view, 2> FEATURE_COLUMNS = {"", "is_readonly"};

 private:
  friend class TransactionMetric;
  FRIEND_TEST(MetricsTests, TransactionCSVTest);

  void RecordBeginData(const common::ResourceTracker::Metrics &resource_metrics) {
    begin_data_.emplace_back(resource_metrics);
  }

  void RecordCommitData(const uint64_t is_readonly, const common::ResourceTracker::Metrics &resource_metrics) {
    commit_data_.emplace_back(is_readonly, resource_metrics);
  }

  struct BeginData {
    explicit BeginData(const common::ResourceTracker::Metrics &resource_metrics)
        : resource_metrics_(resource_metrics) {}
    const common::ResourceTracker::Metrics resource_metrics_;
  };

  struct CommitData {
    CommitData(const uint64_t is_readonly, const common::ResourceTracker::Metrics &resource_metrics)
        : is_readonly_(is_readonly), resource_metrics_(resource_metrics) {}
    const uint64_t is_readonly_;
    const common::ResourceTracker::Metrics resource_metrics_;
  };

  std::list<BeginData> begin_data_;
  std::list<CommitData> commit_data_;
};

/**
 * Metrics for the transaction components of the system: currently begin gate and table latch
 */
class TransactionMetric : public AbstractMetric<TransactionMetricRawData> {
 private:
  friend class MetricsStore;

  void RecordBeginData(const common::ResourceTracker::Metrics &resource_metrics) {
    GetRawData()->RecordBeginData(resource_metrics);
  }
  void RecordCommitData(const uint64_t is_readonly, const common::ResourceTracker::Metrics &resource_metrics) {
    GetRawData()->RecordCommitData(is_readonly, resource_metrics);
  }
};
}  // namespace noisepage::metrics
