#pragma once

#include <unordered_map>
#include <map>
#include <vector>
#include <string>
#include <utility>

#include "execution/exec_defs.h"
#include "brain/forecast/workload_forecast_segment.h"
#include "parser/expression/constant_value_expression.h"

namespace terrier::brain {

/**
 * 
 */
class WorkloadForecast {
 public:
  /**
   * Constructor for 
   * @param 
   * @param forecast_interval the access observer attached to this GC. The GC reports every record gc-ed to the observer if
   *                          
   */
  WorkloadForecast(std::map<std::pair<execution::query_id_t, uint64_t>, uint64_t> query_id_to_timestamps,
                   std::unordered_map<execution::query_id_t, std::vector<uint64_t>> num_executions_,
                   std::unordered_map<execution::query_id_t, const std::string> query_id_to_string,
                   std::unordered_map<const std::string, execution::query_id_t> query_string_to_id,
                   std::unordered_map<execution::query_id_t, std::vector<parser::ConstantValueExpression>> query_id_to_param,
                   uint64_t forecast_interval);

 private:

  void CreateSegments();

  std::unordered_map<execution::query_id_t, const std::string> query_id_to_string_;
  std::unordered_map<const std::string, execution::query_id_t> query_string_to_id_;
  std::unordered_map<execution::query_id_t, std::vector<parser::ConstantValueExpression>> query_id_to_param_;
  std::vector<brain::WorkloadForecastSegment> forecast_segments_;
  uint64_t num_forecast_segment_;
  uint64_t forecast_interval_;
};

}  // namespace terrier::brain::forecast
