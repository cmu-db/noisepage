#include "brain/forecast/workload_forecast.h"

#include <unordered_map>
#include <map>
#include <vector>
#include <string>
#include <utility>

namespace terrier::brain {

WorkloadForecast::WorkloadForecast(
    std::map<std::pair<execution::query_id_t, uint64_t>, uint64_t> query_id_to_timestamps,
    std::unordered_map<execution::query_id_t, std::vector<uint64_t>> num_executions,
    std::unordered_map<execution::query_id_t, const std::string> query_id_to_string,
    std::unordered_map<const std::string, execution::query_id_t> query_string_to_id,
    std::unordered_map<execution::query_id_t, std::vector<parser::ConstantValueExpression>> query_id_to_param,
    uint64_t forecast_interval)
    : query_id_to_string(query_id_to_string_),
      query_string_to_id(query_string_to_id_),
      query_id_to_param(query_id_to_param_),
      forecast_interval(forecast_interval_) {
  CreateSegments(query_id_to_timestamps, num_executions);
}

void WorkloadForecast::CreateSegments(
    std::map<std::pair<execution::query_id_t, uint64_t>, uint64_t> query_id_to_timestamps,
    std::unordered_map<execution::query_id_t, std::vector<uint64_t>> num_executions) {
  
  std::vector<WorkloadForecastSegment> segments;
  std::vector<execution::query_id_t> seg_qids;
  std::vector<uint64_t> seg_executions;
  uint64_t curr_time = query_id_to_timestamps.begin()->second;

  for (auto it = query_id_to_timestamps.begin(); it != query_id_to_timestamps.end(); it++) {
    if (it->second > curr_time + forecast_interval_){
      segments.push_back(WorkloadForecastSegment(seg_qids, seg_executions));
      curr_time = it->second;
      seg_qids.clear();
      seg_executions.clear();
    }
    seg_qids.push_back(it->first->first);
    seg_executions.push_back(num_executions[it->first->first][it->first->second]);
  }

  if (seg_qids.size() > 0) {
    segments.push_back(WorkloadForecastSegment(seg_qids, seg_executions));
  }
  forecast_segments_ = segments;
  num_forecast_segment_ = segments.size();
}


}  // namespace terrier::brain::forecast
