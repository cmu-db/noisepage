#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "parser/expression/constant_value_expression.h"
#include "self_driving/forecasting/workload_forecast_segment.h"

namespace noisepage::selfdriving {

/**
 * A workload forecast prediction is described as:
 * map<cluster id, map<query id, vector of predictions for segments>>
 *
 * The first level associates queries to clusters. The second level describes the
 * forecasted number of queries per segment.
 */
using WorkloadForecastPrediction = std::unordered_map<uint64_t, std::unordered_map<uint64_t, std::vector<double>>>;

/**
 * A utility class meant to shuffle information between WorkloadForecast and Pilot.
 * The utility class describes the data associated with a given workload interval.
 */
class WorkloadMetadata {
 public:
  /** Map from query id to database id */
  std::unordered_map<execution::query_id_t, uint64_t> query_id_to_dboid_;

  /** Map from query id to query text */
  std::unordered_map<execution::query_id_t, std::string> query_id_to_text_;

  /** Map from query id to sample query parameters */
  std::unordered_map<execution::query_id_t, std::vector<std::vector<parser::ConstantValueExpression>>>
      query_id_to_params_;

  /** Map from query id to query parameter types */
  std::unordered_map<execution::query_id_t, std::vector<type::TypeId>> query_id_to_param_types_;
};

/**
 * Breaking predicted queries passed in by the Pilot into segments by their associated timestamps
 * Executing each query while extracting pipeline features
 */
class WorkloadForecast {
 public:
  /**
   * Constructor for WorkloadForecast from disk file
   * @param forecast_interval Interval used to partition the queries into segments
   * @param num_sample Number of samples for query parameters
   */
  explicit WorkloadForecast(uint64_t forecast_interval, uint64_t num_sample);

  /**
   * Constructor for WorkloadForecast from internal table inference results
   * @param inference Workload inference
   * @param metadata Workload metadata information
   */
  explicit WorkloadForecast(const WorkloadForecastPrediction &inference, WorkloadMetadata &&metadata);

  /**
   * Constructor for WorkloadForecast from on-disk inference results
   * @param inference Workload inference
   * @param forecast_interval Interval used to partition the queries into segments
   * @param num_sample Number of samples for query parameters
   */
  explicit WorkloadForecast(const WorkloadForecastPrediction &inference, uint64_t forecast_interval,
                            uint64_t num_sample);

  /**
   * Get number of forecasted segments
   * @return number of forecasted segments
   */
  uint64_t GetNumberOfSegments() { return num_forecast_segment_; }

 private:
  friend class PilotUtil;
  const WorkloadForecastSegment &GetSegmentByIndex(uint64_t segment_index) {
    NOISEPAGE_ASSERT(segment_index < num_forecast_segment_, "invalid index");
    return forecast_segments_[segment_index];
  }

  std::string GetQuerytextByQid(execution::query_id_t qid) {
    NOISEPAGE_ASSERT(workload_metadata_.query_id_to_text_.find(qid) != workload_metadata_.query_id_to_text_.end(),
                     "invalid qid");
    return workload_metadata_.query_id_to_text_.at(qid);
  }

  std::vector<std::vector<parser::ConstantValueExpression>> *GetQueryparamsByQid(execution::query_id_t qid) {
    NOISEPAGE_ASSERT(workload_metadata_.query_id_to_params_.find(qid) != workload_metadata_.query_id_to_params_.end(),
                     "invalid qid");
    return &(workload_metadata_.query_id_to_params_.at(qid));
  }

  std::vector<type::TypeId> *GetParamtypesByQid(execution::query_id_t qid) {
    NOISEPAGE_ASSERT(
        workload_metadata_.query_id_to_param_types_.find(qid) != workload_metadata_.query_id_to_param_types_.end(),
        "invalid qid");
    return &(workload_metadata_.query_id_to_param_types_.at(qid));
  }

  uint64_t GetDboidByQid(execution::query_id_t qid) {
    NOISEPAGE_ASSERT(workload_metadata_.query_id_to_dboid_.find(qid) != workload_metadata_.query_id_to_dboid_.end(),
                     "invalid qid");
    return workload_metadata_.query_id_to_dboid_.at(qid);
  }

  /**
   * Initializes segments from inference results
   * @param inference Inference results
   */
  void InitFromInference(const WorkloadForecastPrediction &inference);

  void LoadQueryTrace();
  void LoadQueryText();
  void CreateSegments();

  std::multimap<uint64_t, execution::query_id_t> query_timestamp_to_id_;
  uint64_t num_sample_;
  WorkloadMetadata workload_metadata_;

  std::vector<WorkloadForecastSegment> forecast_segments_;
  uint64_t num_forecast_segment_;
  uint64_t forecast_interval_;
};

}  // namespace noisepage::selfdriving
