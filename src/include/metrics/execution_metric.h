#pragma once

#include <algorithm>
#include <chrono>  //NOLINT
#include <fstream>
#include <list>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/resource_tracker.h"
#include "metrics/abstract_metric.h"
#include "metrics/metrics_util.h"
#include "transaction/transaction_defs.h"

namespace noisepage::metrics {

/**
 * Raw data object for holding stats collected for the execution engine
 */
class ExecutionMetricRawData : public AbstractRawData {
 public:
  void Aggregate(AbstractRawData *const other) override {
    auto other_db_metric = dynamic_cast<ExecutionMetricRawData *>(other);
    if (!other_db_metric->execution_data_.empty()) {
      execution_data_.splice(execution_data_.cend(), other_db_metric->execution_data_);
    }
  }

  /**
   * @return the type of the metric this object is holding the data for
   */
  MetricsComponent GetMetricType() const override { return MetricsComponent::EXECUTION; }

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

    for (const auto &data : execution_data_) {
      outfile << data.feature_ << ", " << static_cast<uint32_t>(data.execution_mode_) << ", ";
      data.resource_metrics_.ToCSV(outfile);
      outfile << std::endl;
    }
    execution_data_.clear();
  }

  /**
   * Files to use for writing to CSV.
   */
  static constexpr std::array<std::string_view, 1> FILES = {"./execution.csv"};
  /**
   * Columns to use for writing to CSV.
   * Note: This includes the columns for the input feature, but not the output (resource counters)
   */
  static constexpr std::array<std::string_view, 1> FEATURE_COLUMNS = {"feature"};

 private:
  friend class ExecutionMetric;
  FRIEND_TEST(MetricsTests, LoggingCSVTest);

  void RecordExecutionData(const char *feature, uint32_t len, uint8_t execution_mode,
                           const common::ResourceTracker::Metrics &resource_metrics) {
    execution_data_.emplace_back(feature, len, execution_mode, resource_metrics);
  }

  struct ExecutionData {
    ExecutionData(const char *name, uint32_t len, uint8_t execution_mode,
                  const common::ResourceTracker::Metrics &resource_metrics)
        : feature_(name, len), execution_mode_(execution_mode), resource_metrics_(resource_metrics) {}
    const std::string feature_;
    const uint8_t execution_mode_;
    const common::ResourceTracker::Metrics resource_metrics_;
  };

  std::list<ExecutionData> execution_data_;
};

/**
 * Metrics for the execution engine of the system collected at the pipeline level
 */
class ExecutionMetric : public AbstractMetric<ExecutionMetricRawData> {
 private:
  friend class MetricsStore;

  void RecordExecutionData(const char *feature, uint32_t len, uint8_t execution_mode,
                           const common::ResourceTracker::Metrics &resource_metrics) {
    GetRawData()->RecordExecutionData(feature, len, execution_mode, resource_metrics);
  }
};
}  // namespace noisepage::metrics
