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
 * Raw data object for holding stats collected at transaction level
 */
class TransactionMetricRawData : public AbstractRawData {
 public:
  void Aggregate(AbstractRawData *const other) override {
    auto other_db_metric = dynamic_cast<TransactionMetricRawData *>(other);
    if (!other_db_metric->begin_data_.empty()) {
      begin_data_.splice(begin_data_.cbegin(), other_db_metric->begin_data_);
    }
    if (!other_db_metric->commit_data_.empty()) {
      commit_data_.splice(commit_data_.cbegin(), other_db_metric->commit_data_);
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
    TERRIER_ASSERT(outfiles->size() == FILES.size(), "Number of files passed to metric is wrong.");
    TERRIER_ASSERT(std::count_if(outfiles->cbegin(), outfiles->cend(),
                                 [](const std::ofstream &outfile) { return !outfile.is_open(); }) == 0,
                   "Not all files are open.");

    for (const auto &data : begin_data_) {
      ((*outfiles)[0]) << data.now_ << "," << data.elapsed_us_ << "," << data.txn_start_ << std::endl;
    }
    for (const auto &data : commit_data_) {
      ((*outfiles)[1]) << data.now_ << "," << data.elapsed_us_ << "," << data.txn_start_ << std::endl;
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
  static constexpr std::array<std::string_view, 2> COLUMNS = {"now,elapsed_us,txn_start", "now,elapsed_us,txn_start"};

 private:
  friend class TransactionMetric;
  FRIEND_TEST(MetricsTests, TransactionCSVTest);

  void RecordBeginData(const uint64_t elapsed_us, const transaction::timestamp_t txn_start) {
    begin_data_.emplace_back(elapsed_us, txn_start);
  }

  void RecordCommitData(const uint64_t elapsed_us, const transaction::timestamp_t txn_start) {
    commit_data_.emplace_back(elapsed_us, txn_start);
  }

  struct Data {
    Data(const uint64_t elapsed_us, const transaction::timestamp_t txn_start)
        : now_(MetricsUtil::Now()), elapsed_us_(elapsed_us), txn_start_(txn_start) {}
    const uint64_t now_;
    const uint64_t elapsed_us_;
    const transaction::timestamp_t txn_start_;
  };

  std::list<Data> begin_data_;
  std::list<Data> commit_data_;
};

/**
 * Metrics for the transaction components of the system: currently begin gate and table latch
 */
class TransactionMetric : public AbstractMetric<TransactionMetricRawData> {
 private:
  friend class MetricsStore;

  void RecordBeginData(const uint64_t elapsed_us, const transaction::timestamp_t txn_start) {
    GetRawData()->RecordBeginData(elapsed_us, txn_start);
  }
  void RecordCommitData(const uint64_t elapsed_us, const transaction::timestamp_t txn_start) {
    GetRawData()->RecordCommitData(elapsed_us, txn_start);
  }
};
}  // namespace terrier::metrics
