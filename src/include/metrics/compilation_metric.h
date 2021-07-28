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
#include "self_driving/modeling/compilation_operating_unit.h"

namespace noisepage::selfdriving::pilot {
class PilotUtil;
class Pilot;
}  // namespace noisepage::selfdriving::pilot

namespace noisepage::metrics {

/**
 * Raw data object for holding stats collected for the execution engine
 */
class CompilationMetricRawData : public AbstractRawData {
 public:
  void Aggregate(AbstractRawData *const other) override {
    auto other_db_metric = dynamic_cast<CompilationMetricRawData *>(other);
    if (!other_db_metric->compilation_data_.empty()) {
      compilation_data_.splice(compilation_data_.cend(), other_db_metric->compilation_data_);
    }
  }

  /**
   * @return the type of the metric this object is holding the data for
   */
  MetricsComponent GetMetricType() const override { return MetricsComponent::COMPILATION; }

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
    for (auto &data : compilation_data_) {
      outfile << data.query_id_.UnderlyingValue() << ", ";
      outfile << data.module_name_ << ", ";
      outfile << data.feature_.GetCodeSize() << ", ";
      outfile << data.feature_.GetDataSize() << ", ";
      outfile << data.feature_.GetFunctionsSize() << ", ";
      outfile << data.feature_.GetStaticLocalsSize() << ", ";

      data.resource_metrics_.ToCSV(outfile);
      outfile << std::endl;
    }
    compilation_data_.clear();
  }

  /**
   * Files to use for writing to CSV.
   */
  static constexpr std::array<std::string_view, 1> FILES = {"./compilation.csv"};

  /**
   * Columns to use for writing to CSV.
   * Note: This includes the columns for the input feature, but not the output (resource counters)
   */
  static constexpr std::array<std::string_view, 1> FEATURE_COLUMNS = {
      "query_id, name, code_size, data_size, functions_size, static_locals_size"};

 private:
  friend class CompilationMetric;
  friend class selfdriving::pilot::PilotUtil;
  friend class selfdriving::pilot::Pilot;
  FRIEND_TEST(MetricsTests, CompilationTest);
  struct CompilationData;

  void RecordCompilationData(execution::query_id_t query_id, const std::string &module_name,
                             const selfdriving::CompilationOperatingUnit &feature,
                             const common::ResourceTracker::Metrics &resource_metrics) {
    compilation_data_.emplace_back(query_id, module_name, feature, resource_metrics);
  }

  struct CompilationData {
    CompilationData(execution::query_id_t query_id, std::string module_name,
                    const selfdriving::CompilationOperatingUnit &feature,
                    const common::ResourceTracker::Metrics &resource_metrics)
        : query_id_(query_id),
          module_name_(std::move(module_name)),
          feature_(feature),
          resource_metrics_(resource_metrics) {}

    const execution::query_id_t query_id_;
    const std::string module_name_;
    const selfdriving::CompilationOperatingUnit feature_;
    const common::ResourceTracker::Metrics resource_metrics_;
  };

  std::list<CompilationData> compilation_data_;
};

/**
 * Metrics collected for compilation of queries
 */
class CompilationMetric : public AbstractMetric<CompilationMetricRawData> {
 private:
  friend class MetricsStore;

  void RecordCompilationMetric(execution::query_id_t query_id, std::string module_name,
                               const selfdriving::CompilationOperatingUnit &feature,
                               const common::ResourceTracker::Metrics &resource_metrics) {
    std::replace(module_name.begin(), module_name.end(), ',', '_');
    std::replace(module_name.begin(), module_name.end(), ';', '_');
    GetRawData()->RecordCompilationData(query_id, module_name, feature, resource_metrics);
  }
};
}  // namespace noisepage::metrics
