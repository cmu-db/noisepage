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

namespace terrier::storage {
class RecoveryTests;
}  // namespace terrier::storage

namespace terrier::metrics {

/**
 * Raw data object for holding stats collected during recovery
 */
class RecoveryMetricRawData : public AbstractRawData {
 public:
  void Aggregate(AbstractRawData *const other) override {
    auto other_db_metric = dynamic_cast<RecoveryMetricRawData *>(other);
    if (!other_db_metric->recovery_data_.empty()) {
      recovery_data_.splice(recovery_data_.cbegin(), other_db_metric->recovery_data_);
    }
  }

  /**
   * @return the type of the metric this object is holding the data for
   */
  MetricsComponent GetMetricType() const override { return MetricsComponent::RECOVERY; }

  /**
   * Writes the data out to ofstreams
   * @param outfiles vector of ofstreams to write to that have been opened by the MetricsManager
   */
  void ToCSV(std::vector<std::ofstream> *const outfiles) final {
    TERRIER_ASSERT(outfiles->size() == FILES.size(), "Number of files passed to metric is wrong.");
    TERRIER_ASSERT(std::count_if(outfiles->cbegin(), outfiles->cend(),
                                 [](const std::ofstream &outfile) { return !outfile.is_open(); }) == 0,
                   "Not all files are open.");

    for (const auto &data : recovery_data_) {
      ((*outfiles)[0]) << data.now_ << "," << data.num_txns_ << "," << data.num_bytes_ << "," << data.elapsed_us_
                       << std::endl;
    }

    recovery_data_.clear();
  }

  /**
   * Files to use for writing to CSV.
   */
  static constexpr std::array<std::string_view, 1> FILES = {"./recovery_manager.csv"};
  /**
   * Columns to use for writing to CSV.
   */
  static constexpr std::array<std::string_view, 1> COLUMNS = {"now,num_txns,num_bytes,elapsed_us"};

 private:
  friend class RecoveryMetric;
  friend class storage::RecoveryTests;

  struct RecoveryData {
    RecoveryData(const uint64_t num_txns, const uint64_t num_bytes, const uint64_t elapsed_us)
        : now_(MetricsUtil::Now()), num_txns_(num_txns), num_bytes_(num_bytes), elapsed_us_(elapsed_us) {}
    const uint64_t now_;
    const uint64_t num_txns_;
    const uint64_t num_bytes_;
    const uint64_t elapsed_us_;
  };

  std::list<RecoveryData> recovery_data_;

  void RecordRecoveryData(const uint64_t num_txns, const uint64_t num_bytes, const uint64_t elapsed_us) {
    recovery_data_.emplace_front(num_txns, num_bytes, elapsed_us);
  }
};

/**
 * Metrics for the recovery of the system
 */
class RecoveryMetric : public AbstractMetric<RecoveryMetricRawData> {
 private:
  friend class MetricsStore;

  void RecordRecoveryData(const uint64_t num_txns, const uint64_t num_bytes, const uint64_t elapsed_us) {
    GetRawData()->RecordRecoveryData(num_txns, num_bytes, elapsed_us);
  }
};
}  // namespace terrier::metrics
