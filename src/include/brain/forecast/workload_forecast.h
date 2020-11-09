#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "brain/forecast/workload_forecast_segment.h"
#include "common/action_context.h"
#include "common/error/exception.h"
#include "common/macros.h"
#include "common/managed_pointer.h"
#include "common/shared_latch.h"
#include "execution/exec/execution_context.h"
#include "execution/exec_defs.h"
#include "gflags/gflags.h"
#include "parser/expression/constant_value_expression.h"
#include "settings/settings_callbacks.h"
#include "settings/settings_manager.h"
#include "spdlog/fmt/fmt.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"

namespace noisepage::brain {

/**
 * Breaking predicted queries passed in by the Pilot into segments by their associated timestamps
 * Executing each query while extracting pipeline features
 */
class WorkloadForecast {
 public:
  /**
   * Constructor for WorkloadForecast
   * @param query_timestamp_to_id Map from a timestamp to one query qid that has a record of this timestamp
   * @param num_executions Number of occurrence of this qid in the input
   * @param query_id_to_string Map from a qid to the query text
   * @param query_string_to_id Map from a query's text to the qid
   * @param query_id_to_param Map from qid to a constant number of parameters
   * @param query_id_to_dboid Map from qid to the database oid
   * @param forecast_interval Interval used to partition the queries into segments
   *
   */
  WorkloadForecast(std::map<uint64_t, std::pair<execution::query_id_t, uint64_t>> query_timestamp_to_id,
                   std::unordered_map<execution::query_id_t, std::vector<uint64_t>> num_executions,
                   std::unordered_map<execution::query_id_t, std::string> query_id_to_string,
                   std::unordered_map<std::string, execution::query_id_t> query_string_to_id,
                   std::unordered_map<execution::query_id_t, std::vector<std::vector<parser::ConstantValueExpression>>>
                       query_id_to_param,
                   std::unordered_map<execution::query_id_t, uint64_t> query_id_to_dboid, uint64_t forecast_interval);
  /**
   * Sort queries by their timestamp, then partition by forecast_interval
   * @param query_timestamp_to_id Map from a timestamp to a query and an index that denotes a specific set of parameter
   * @param num_executions Number of executions associated with each query and a set of parameter
   */
  void CreateSegments(std::map<uint64_t, std::pair<execution::query_id_t, uint64_t>> &query_timestamp_to_id,
                      std::unordered_map<execution::query_id_t, std::vector<uint64_t>> &num_executions);
  /**
   * Execute all queries and the constant number of parameters associated with each
   * @param db_main Managed pointer to db_main
   */
  void ExecuteSegments(common::ManagedPointer<DBMain> db_main);

 private:
  std::vector<parser::ConstantValueExpression> SampleParam(execution::query_id_t qid);

  std::unordered_map<execution::query_id_t, std::string> query_id_to_string_;
  std::unordered_map<std::string, execution::query_id_t> query_string_to_id_;
  std::unordered_map<execution::query_id_t, std::vector<std::vector<parser::ConstantValueExpression>>>
      query_id_to_param_;
  std::unordered_map<execution::query_id_t, uint64_t> query_id_to_dboid_;
  std::vector<WorkloadForecastSegment> forecast_segments_;
  uint64_t num_forecast_segment_;
  uint64_t forecast_interval_;
  uint64_t optimizer_timeout_{10000000};
};

}  // namespace noisepage::brain
