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

using WorkloadForecastPrediction = std::unordered_map<uint64_t, std::unordered_map<uint64_t, std::vector<double>>>;

/**
 * Breaking predicted queries passed in by the Pilot into segments by their associated timestamps
 * Executing each query while extracting pipeline features
 */
class WorkloadForecast {
 public:
  /**
   * Constructor for WorkloadForecast
   * @param forecast_interval Interval used to partition the queries into segments
   *
   */
  explicit WorkloadForecast(uint64_t forecast_interval);

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
    NOISEPAGE_ASSERT(query_id_to_text_.find(qid) != query_id_to_text_.end(), "invalid qid");
    return query_id_to_text_.at(qid);
  }

  std::vector<std::vector<parser::ConstantValueExpression>> *GetQueryparamsByQid(execution::query_id_t qid) {
    NOISEPAGE_ASSERT(query_id_to_params_.find(qid) != query_id_to_params_.end(), "invalid qid");
    return &(query_id_to_params_.at(qid));
  }

  std::vector<type::TypeId> *GetParamtypesByQid(execution::query_id_t qid) {
    NOISEPAGE_ASSERT(query_id_to_param_types_.find(qid) != query_id_to_param_types_.end(), "invalid qid");
    return &(query_id_to_param_types_.at(qid));
  }

  uint64_t GetDboidByQid(execution::query_id_t qid) {
    NOISEPAGE_ASSERT(query_id_to_dboid_.find(qid) != query_id_to_dboid_.end(), "invalid qid");
    return query_id_to_dboid_.at(qid);
  }

  uint64_t GetOptimizerTimeout() { return optimizer_timeout_; }

  void LoadQueryTrace();
  void LoadQueryText();
  void CreateSegments();

  std::multimap<uint64_t, execution::query_id_t> query_timestamp_to_id_;
  std::unordered_map<execution::query_id_t, std::vector<std::vector<parser::ConstantValueExpression>>>
      query_id_to_params_;
  std::unordered_map<execution::query_id_t, std::vector<type::TypeId>> query_id_to_param_types_;
  std::unordered_map<execution::query_id_t, std::string> query_id_to_text_;
  std::unordered_map<std::string, execution::query_id_t> query_text_to_id_;
  std::unordered_map<execution::query_id_t, uint64_t> query_id_to_dboid_;
  uint64_t num_sample_{5};

  std::vector<WorkloadForecastSegment> forecast_segments_;
  uint64_t num_forecast_segment_;
  uint64_t forecast_interval_;
  uint64_t optimizer_timeout_{10000000};
};

}  // namespace noisepage::selfdriving
