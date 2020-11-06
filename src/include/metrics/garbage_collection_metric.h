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
  void Aggregate(AbstractRawData *const other) override {
    auto other_db_metric = dynamic_cast<GarbageCollectionMetricRawData *>(other);
    if (!other_db_metric->gc_data_.empty()) {
      gc_data_.splice(gc_data_.cend(), other_db_metric->gc_data_);
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
    NOISEPAGE_ASSERT(outfiles->size() == FILES.size(), "Number of files passed to metric is wrong.");
    NOISEPAGE_ASSERT(std::count_if(outfiles->cbegin(), outfiles->cend(),
                                   [](const std::ofstream &outfile) { return !outfile.is_open(); }) == 0,
                     "Not all files are open.");

    auto &outfile = (*outfiles)[0];

    for (const auto &data : gc_data_) {
      outfile << data.txns_deallocated_ << ", " << data.txns_unlinked_ << ", " << data.buffer_unlinked_ << ", "
              << data.readonly_unlinked_ << ", " << data.interval_ << ", ";
      data.resource_metrics_.ToCSV(outfile);
      outfile << std::endl;
    }
    gc_data_.clear();
  }

  /**
   * Files to use for writing to CSV.
   */
  static constexpr std::array<std::string_view, 1> FILES = {"./gc.csv"};
  /**
   * Columns to use for writing to CSV.
   * Note: This includes the columns for the input feature, but not the output (resource counters)
   */
  static constexpr std::array<std::string_view, 1> FEATURE_COLUMNS = {
      "txns_deallocated, txns_unlinked, buffer_unlinked, readonly_unlinked, interval"};

 private:
  friend class GarbageCollectionMetric;
  FRIEND_TEST(MetricsTests, LoggingCSVTest);

  void RecordGCData(uint64_t txns_deallocated, uint64_t txns_unlinked, uint64_t buffer_unlinked,
                    uint64_t readonly_unlinked, const uint64_t interval,
                    const common::ResourceTracker::Metrics &resource_metrics) {
    gc_data_.emplace_back(txns_deallocated, txns_unlinked, buffer_unlinked, readonly_unlinked, interval,
                          resource_metrics);
  }

  struct GCData {
    GCData(uint64_t txns_deallocated, uint64_t txns_unlinked, uint64_t buffer_unlinked, uint64_t readonly_unlinked,
           const uint64_t interval, const common::ResourceTracker::Metrics &resource_metrics)
        : txns_deallocated_(txns_deallocated),
          txns_unlinked_(txns_unlinked),
          buffer_unlinked_(buffer_unlinked),
          readonly_unlinked_(readonly_unlinked),
          interval_(interval),
          resource_metrics_(resource_metrics) {}
    const uint64_t txns_deallocated_;
    const uint64_t txns_unlinked_;
    const uint64_t buffer_unlinked_;
    const uint64_t readonly_unlinked_;
    const uint64_t interval_;
    const common::ResourceTracker::Metrics resource_metrics_;
  };

  std::list<GCData> gc_data_;
};

/**
 * Metrics for the garbage collection components of the system: currently deallocation and unlinking
 */
class GarbageCollectionMetric : public AbstractMetric<GarbageCollectionMetricRawData> {
 private:
  friend class MetricsStore;

  void RecordGCData(uint64_t txns_deallocated, uint64_t txns_unlinked, uint64_t buffer_unlinked,
                    uint64_t readonly_unlinked, uint64_t interval,
                    const common::ResourceTracker::Metrics &resource_metrics) {
    GetRawData()->RecordGCData(txns_deallocated, txns_unlinked, buffer_unlinked, readonly_unlinked, interval,
                               resource_metrics);
  }
};
}  // namespace noisepage::metrics
