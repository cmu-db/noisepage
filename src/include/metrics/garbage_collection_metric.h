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
  void Aggregate(AbstractRawData *const other) override {
    auto other_db_metric = dynamic_cast<GarbageCollectionMetricRawData *>(other);
    if (!other_db_metric->deallocate_data_.empty()) {
      deallocate_data_.splice(deallocate_data_.cbegin(), other_db_metric->deallocate_data_);
    }
    if (!other_db_metric->unlink_data_.empty()) {
      unlink_data_.splice(unlink_data_.cbegin(), other_db_metric->unlink_data_);
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

    auto &serializer_outfile = (*outfiles)[0];
    auto &consumer_outfile = (*outfiles)[1];

    for (const auto &data : deallocate_data_) {
      serializer_outfile << data.num_processed_ << ", ";
      data.resource_metrics_.ToCSV(serializer_outfile);
      serializer_outfile << std::endl;
    }
    for (const auto &data : unlink_data_) {
      consumer_outfile << data.num_processed_ << ", " << data.num_buffers_ << ", " << data.num_readonly_ << ", ";
      data.resource_metrics_.ToCSV(consumer_outfile);
      consumer_outfile << std::endl;
    }
    deallocate_data_.clear();
    unlink_data_.clear();
  }

  /**
   * Files to use for writing to CSV.
   */
  static constexpr std::array<std::string_view, 2> FILES = {"./gc_deallocate.csv", "./gc_unlink.csv"};
  /**
   * Columns to use for writing to CSV.
   * Note: This includes the columns for the input feature, but not the output (resource counters)
   */
  static constexpr std::array<std::string_view, 2> FEATURE_COLUMNS = {"num_processed",
                                                                      "num_processed, num_buffers, num_readonly"};

 private:
  friend class GarbageCollectionMetric;
  FRIEND_TEST(MetricsTests, LoggingCSVTest);

  void RecordDeallocateData(const uint64_t num_processed, const common::ResourceTracker::Metrics &resource_metrics) {
    deallocate_data_.emplace_front(num_processed, resource_metrics);
  }

  void RecordUnlinkData(const uint64_t num_processed, const uint64_t num_buffers, const uint64_t num_readonly,
                        const common::ResourceTracker::Metrics &resource_metrics) {
    unlink_data_.emplace_front(num_processed, num_buffers, num_readonly, resource_metrics);
  }

  struct DeallocateData {
    DeallocateData(const uint64_t num_processed, const common::ResourceTracker::Metrics &resource_metrics)
        : num_processed_(num_processed), resource_metrics_(resource_metrics) {}
    const uint64_t num_processed_;
    const common::ResourceTracker::Metrics resource_metrics_;
  };

  struct UnlinkData {
    UnlinkData(const uint64_t num_processed, const uint64_t num_buffers, const uint64_t num_readonly,
               const common::ResourceTracker::Metrics &resource_metrics)
        : num_processed_(num_processed),
          num_buffers_(num_buffers),
          num_readonly_(num_readonly),
          resource_metrics_(resource_metrics) {}
    const uint64_t num_processed_;
    const uint64_t num_buffers_;
    const uint64_t num_readonly_;
    const common::ResourceTracker::Metrics resource_metrics_;
  };

  std::list<DeallocateData> deallocate_data_;
  std::list<UnlinkData> unlink_data_;
};

/**
 * Metrics for the garbage collection components of the system: currently deallocation and unlinking
 */
class GarbageCollectionMetric : public AbstractMetric<GarbageCollectionMetricRawData> {
 private:
  friend class MetricsStore;

  void RecordDeallocateData(const uint64_t num_processed, const common::ResourceTracker::Metrics &resource_metrics) {
    GetRawData()->RecordDeallocateData(num_processed, resource_metrics);
  }
  void RecordUnlinkData(const uint64_t num_processed, const uint64_t num_buffers, const uint64_t num_readonly,
                        const common::ResourceTracker::Metrics &resource_metrics) {
    GetRawData()->RecordUnlinkData(num_processed, num_buffers, num_readonly, resource_metrics);
  }
};
}  // namespace terrier::metrics
