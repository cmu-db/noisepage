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
 * Raw data object for holding stats collected for processing the bind command
 */
class BindCommandMetricRawData : public AbstractRawData {
 public:
  void Aggregate(AbstractRawData *const other) override {
    auto other_db_metric = dynamic_cast<BindCommandMetricRawData *>(other);
    if (!other_db_metric->bind_command_data_.empty()) {
      bind_command_data_.splice(bind_command_data_.cend(), other_db_metric->bind_command_data_);
    }
  }

  /**
   * @return the type of the metric this object is holding the data for
   */
  MetricsComponent GetMetricType() const override { return MetricsComponent::BIND_COMMAND; }

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

    for (auto &data : bind_command_data_) {
      outfile << (data.param_num_) << ", ";
      outfile << (data.query_text_size_) << ", ";

      data.resource_metrics_.ToCSV(outfile);
      outfile << std::endl;
    }
    bind_command_data_.clear();
  }

  /**
   * Files to use for writing to CSV.
   */
  static constexpr std::array<std::string_view, 1> FILES = {"./bind_command.csv"};

  /**
   * Columns to use for writing to CSV.
   */
  static constexpr std::array<std::string_view, 2> FEATURE_COLUMNS = {"param_num, query_text_size"};

 private:
  friend class BindCommandMetric;
  struct BindCommandData;

  void RecordBindCommandData(uint64_t param_num, uint64_t query_text_size,
                             const common::ResourceTracker::Metrics &resource_metrics) {
    bind_command_data_.emplace_front(param_num, query_text_size, resource_metrics);
  }

  struct BindCommandData {
    BindCommandData(uint64_t param_num, uint64_t query_text_size,
                    const common::ResourceTracker::Metrics &resource_metrics)
        : param_num_(param_num), query_text_size_(query_text_size), resource_metrics_(resource_metrics) {}

    const uint64_t param_num_;
    const uint64_t query_text_size_;
    const common::ResourceTracker::Metrics resource_metrics_;
  };

  std::list<BindCommandData> bind_command_data_;
};

/**
 * Metrics for the execution engine of the system collected at the bind_command level
 */
class BindCommandMetric : public AbstractMetric<BindCommandMetricRawData> {
 private:
  friend class MetricsStore;

  void RecordBindCommandData(uint64_t param_num, uint64_t query_text_size,
                             const common::ResourceTracker::Metrics &resource_metrics) {
    GetRawData()->RecordBindCommandData(param_num, query_text_size, resource_metrics);
  }
};
}  // namespace noisepage::metrics
