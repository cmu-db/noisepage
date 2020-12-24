#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "parser/expression/constant_value_expression.h"
#include "self_driving/forecast/workload_forecast_segment.h"

namespace noisepage::selfdriving {

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

 private:
  friend class PilotUtil;

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
  uint64_t num_forecast_segment_{0};
  uint64_t forecast_interval_;
  uint64_t optimizer_timeout_{10000000};
};

}  // namespace noisepage::selfdriving
