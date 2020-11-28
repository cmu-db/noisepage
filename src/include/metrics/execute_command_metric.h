#pragma once

#include <algorithm>
#include <chrono>  //NOLINT
#include <fstream>
#include <list>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/resource_tracker.h"
#include "metrics/abstract_metric.h"
#include "metrics/metrics_util.h"
#include "self_driving/modeling/operating_unit.h"
#include "transaction/transaction_defs.h"

namespace noisepage::metrics {

/**
 * Raw data object for holding stats collected for processing the execute command
 */
class ExecuteCommandMetricRawData : public AbstractRawData {
 public:
  void Aggregate(AbstractRawData *const other) override {
    auto other_db_metric = dynamic_cast<ExecuteCommandMetricRawData *>(other);
    if (!other_db_metric->execute_command_data_.empty()) {
      execute_command_data_.splice(execute_command_data_.cend(), other_db_metric->execute_command_data_);
    }
  }

  /**
   * @return the type of the metric this object is holding the data for
   */
  MetricsComponent GetMetricType() const override { return MetricsComponent::EXECUTE_COMMAND; }

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

    for (auto &data : execute_command_data_) {
      outfile << (data.portal_name_size_) << ", ";

      data.resource_metrics_.ToCSV(outfile);
      outfile << std::endl;
    }
    execute_command_data_.clear();
  }

  /**
   * Files to use for writing to CSV.
   */
  static constexpr std::array<std::string_view, 1> FILES = {"./execute_command.csv"};

  /**
   * Columns to use for writing to CSV.
   */
  static constexpr std::array<std::string_view, 1> FEATURE_COLUMNS = {"protal_name_size"};

 private:
  friend class ExecuteCommandMetric;
  struct ExecuteCommandData;

  void RecordExecuteCommandData(uint64_t portal_name_size, const common::ResourceTracker::Metrics &resource_metrics) {
    execute_command_data_.emplace_front(portal_name_size, resource_metrics);
  }

  struct ExecuteCommandData {
    ExecuteCommandData(uint64_t portal_name_size, const common::ResourceTracker::Metrics &resource_metrics)
        : portal_name_size_(portal_name_size), resource_metrics_(resource_metrics) {}

    const uint64_t portal_name_size_;
    const common::ResourceTracker::Metrics resource_metrics_;
  };

  std::list<ExecuteCommandData> execute_command_data_;
};

/**
 * Metrics for the executeution engine of the system collected at the execute_command level
 */
class ExecuteCommandMetric : public AbstractMetric<ExecuteCommandMetricRawData> {
 private:
  friend class MetricsStore;

  void RecordExecuteCommandData(uint64_t portal_name_size, const common::ResourceTracker::Metrics &resource_metrics) {
    GetRawData()->RecordExecuteCommandData(portal_name_size, resource_metrics);
  }
};
}  // namespace noisepage::metrics
