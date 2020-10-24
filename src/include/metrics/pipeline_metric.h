#pragma once

#include <algorithm>
#include <chrono>  //NOLINT
#include <fstream>
#include <list>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "brain/brain_util.h"
#include "brain/operating_unit.h"
#include "catalog/catalog_defs.h"
#include "common/resource_tracker.h"
#include "metrics/abstract_metric.h"
#include "metrics/metrics_util.h"
#include "transaction/transaction_defs.h"

namespace noisepage::metrics {

/**
 * Raw data object for holding stats collected for the execution engine
 */
class PipelineMetricRawData : public AbstractRawData {
 public:
  void Aggregate(AbstractRawData *const other) override {
    auto other_db_metric = dynamic_cast<PipelineMetricRawData *>(other);
    if (!other_db_metric->pipeline_data_.empty()) {
      pipeline_data_.splice(pipeline_data_.cend(), other_db_metric->pipeline_data_);
    }
  }

  /**
   * @return the type of the metric this object is holding the data for
   */
  MetricsComponent GetMetricType() const override { return MetricsComponent::EXECUTION_PIPELINE; }

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

    for (auto &data : pipeline_data_) {
      outfile << data.query_id_.UnderlyingValue() << ", ";
      outfile << data.pipeline_id_.UnderlyingValue() << ", ";
      outfile << data.features_.size() << ", ";
      outfile << data.GetFeatureVectorString() << ", ";
      outfile << static_cast<uint32_t>(data.execution_mode_) << ", ";
      outfile << data.GetEstRowsVectorString() << ", ";
      outfile << data.GetKeySizeVectorString() << ", ";
      outfile << data.GetNumKeysVectorString() << ", ";
      outfile << data.GetCardinalityVectorString() << ", ";
      outfile << data.GetMemFactorsVectorString() << ", ";
      outfile << data.GetNumLoopsVectorString() << ", ";
      outfile << data.GetNumConcurrentVectorString() << ", ";

      data.resource_metrics_.ToCSV(outfile);
      outfile << std::endl;
    }
    pipeline_data_.clear();
  }

  /**
   * Files to use for writing to CSV.
   */
  static constexpr std::array<std::string_view, 1> FILES = {"./pipeline.csv"};

  /**
   * Columns to use for writing to CSV.
   * Note: This includes the columns for the input feature, but not the output (resource counters)
   */
  static constexpr std::array<std::string_view, 1> FEATURE_COLUMNS = {
      "query_id, pipeline_id, num_features, features, exec_mode, num_rows, key_sizes, num_keys, "
      "est_cardinalities, mem_factor, num_loops, num_concurrent"};

 private:
  friend class PipelineMetric;
  FRIEND_TEST(MetricsTests, PipelineCSVTest);
  struct PipelineData;

  void RecordPipelineData(execution::query_id_t query_id, execution::pipeline_id_t pipeline_id, uint8_t execution_mode,
                          std::vector<brain::ExecutionOperatingUnitFeature> &&features,
                          const common::ResourceTracker::Metrics &resource_metrics) {
    pipeline_data_.emplace_back(query_id, pipeline_id, execution_mode, std::move(features), resource_metrics);
  }

  struct PipelineData {
    PipelineData(execution::query_id_t query_id, execution::pipeline_id_t pipeline_id, uint8_t execution_mode,
                 std::vector<brain::ExecutionOperatingUnitFeature> &&features,
                 const common::ResourceTracker::Metrics &resource_metrics)
        : query_id_(query_id),
          pipeline_id_(pipeline_id),
          execution_mode_(execution_mode),
          features_(features),
          resource_metrics_(resource_metrics) {}

    template <class T>
    std::string ConcatVectorToString(const std::vector<T> &vec) {
      std::stringstream sstream;
      for (auto itr = vec.begin(); itr != vec.end(); itr++) {
        sstream << (*itr);
        if (itr + 1 != vec.end()) {
          sstream << ";";
        }
      }
      return sstream.str();
    }

    std::string GetFeatureVectorString() {
      std::vector<std::string> types;
      for (auto &feature : features_) {
        types.emplace_back(
            brain::BrainUtil::ExecutionOperatingUnitTypeToString(feature.GetExecutionOperatingUnitType()));
      }
      return ConcatVectorToString<std::string>(types);
    }

    std::string GetEstRowsVectorString() {
      std::vector<size_t> est_rows;
      for (auto &feature : features_) {
        est_rows.emplace_back(feature.GetNumRows());
      }
      return ConcatVectorToString<size_t>(est_rows);
    }

    std::string GetCardinalityVectorString() {
      std::vector<size_t> cars;
      for (auto &feature : features_) {
        cars.emplace_back(feature.GetCardinality());
      }
      return ConcatVectorToString<size_t>(cars);
    }

    std::string GetKeySizeVectorString() {
      std::vector<size_t> sizes;
      for (auto &feature : features_) {
        sizes.emplace_back(feature.GetKeySize());
      }
      return ConcatVectorToString<size_t>(sizes);
    }

    std::string GetNumKeysVectorString() {
      std::vector<size_t> num_keys;
      for (auto &feature : features_) {
        num_keys.emplace_back(feature.GetNumKeys());
      }
      return ConcatVectorToString<size_t>(num_keys);
    }

    std::string GetMemFactorsVectorString() {
      std::vector<double> factors;
      for (auto &feature : features_) {
        factors.emplace_back(feature.GetMemFactor());
      }
      return ConcatVectorToString<double>(factors);
    }

    std::string GetNumLoopsVectorString() {
      std::vector<size_t> num_loops;
      for (auto &feature : features_) {
        num_loops.emplace_back(feature.GetNumLoops());
      }
      return ConcatVectorToString<size_t>(num_loops);
    }

    std::string GetNumConcurrentVectorString() {
      std::vector<size_t> num_concurrent;
      for (auto &feature : features_) {
        num_concurrent.emplace_back(feature.GetNumConcurrent());
      }
      return ConcatVectorToString<size_t>(num_concurrent);
    }

    const execution::query_id_t query_id_;
    const execution::pipeline_id_t pipeline_id_;
    const uint8_t execution_mode_;
    const std::vector<brain::ExecutionOperatingUnitFeature> features_;
    const common::ResourceTracker::Metrics resource_metrics_;
  };

  std::list<PipelineData> pipeline_data_;
};

/**
 * Metrics for the execution engine of the system collected at the pipeline level
 */
class PipelineMetric : public AbstractMetric<PipelineMetricRawData> {
 private:
  friend class MetricsStore;

  void RecordPipelineData(execution::query_id_t query_id, execution::pipeline_id_t pipeline_id, uint8_t execution_mode,
                          std::vector<brain::ExecutionOperatingUnitFeature> &&features,
                          const common::ResourceTracker::Metrics &resource_metrics) {
    GetRawData()->RecordPipelineData(query_id, pipeline_id, execution_mode, std::move(features), resource_metrics);
  }
};
}  // namespace noisepage::metrics
