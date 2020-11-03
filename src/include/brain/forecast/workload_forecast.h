#pragma once

#include <unordered_map>
#include <vector>
#include <string>
#include <memory>
#include <utility>

#include "execution/exec_defs.h"
#include "execution/exec/execution_context.h"
#include "brain/forecast/workload_forecast_segment.h"
#include "parser/expression/constant_value_expression.h"
#include "common/action_context.h"
#include "common/error/exception.h"
#include "common/macros.h"
#include "common/managed_pointer.h"
#include "common/shared_latch.h"
#include "execution/exec_defs.h"
#include "execution/exec/execution_context.h"
#include "parser/expression/constant_value_expression.h"
#include "spdlog/fmt/fmt.h"
#include "execution/exec_defs.h"
#include "settings/settings_callbacks.h"
#include "settings/settings_manager.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "gflags/gflags.h"

namespace noisepage::brain {

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
  WorkloadForecast(
    std::map<uint64_t, std::pair<execution::query_id_t, uint64_t>> query_timestamp_to_id,
    std::unordered_map<execution::query_id_t, std::vector<uint64_t>> num_executions,
    std::unordered_map<execution::query_id_t, std::string> query_id_to_string,
    std::unordered_map<std::string, execution::query_id_t> query_string_to_id,
    std::unordered_map<execution::query_id_t, 
                       std::vector<std::vector<parser::ConstantValueExpression>>> query_id_to_param,
    uint64_t forecast_interval);

  void CreateSegments(std::map<uint64_t, std::pair<execution::query_id_t, uint64_t>>  query_timestamp_to_id,
                      std::unordered_map<execution::query_id_t, std::vector<uint64_t>> num_executions);
  
  void ExecuteSegments(const common::ManagedPointer<DBMain> db_main);

 private:

  std::unordered_map<execution::query_id_t, std::string> query_id_to_string_;
  std::unordered_map<std::string, execution::query_id_t> query_string_to_id_;
  std::unordered_map<execution::query_id_t, std::vector<std::vector<parser::ConstantValueExpression>>> query_id_to_param_;
  std::vector<WorkloadForecastSegment> forecast_segments_;
  uint64_t num_forecast_segment_;
  uint64_t forecast_interval_;
  uint64_t optimizer_timeout_ {10000000};
};

}  // namespace terrier::brain::forecast
