#include "brain/forecast/workload_forecast.h"

#include <unordered_map>
#include <map>
#include <vector>
#include <string>
#include <utility>

#include "brain/operating_unit.h"
#include "execution/exec_defs.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/execution_settings.h"
#include "execution/compiler/executable_query.h"
#include "execution/vm/vm_defs.h"
#include "brain/forecast/workload_forecast_segment.h"
#include "parser/expression/constant_value_expression.h"

namespace noisepage::brain {

WorkloadForecast::WorkloadForecast(
    std::map<uint64_t, std::pair<execution::query_id_t, uint64_t>> query_timestamp_to_id,
    std::unordered_map<execution::query_id_t, std::vector<uint64_t>> num_executions,
    std::unordered_map<execution::query_id_t, std::string> query_id_to_string,
    std::unordered_map<std::string, execution::query_id_t> query_string_to_id,
    std::unordered_map<execution::query_id_t, std::vector<std::vector<parser::ConstantValueExpression>>> query_id_to_param,
    uint64_t forecast_interval)
    : query_id_to_string_(query_id_to_string),
      query_string_to_id_(query_string_to_id),
      query_id_to_param_(query_id_to_param),
      forecast_interval_(forecast_interval) {
  CreateSegments(query_timestamp_to_id, num_executions);
  std::cout << "num_forecast_segment_" << num_forecast_segment_ << std::endl;
  for (auto it = forecast_segments_.begin(); it != forecast_segments_.end(); it ++) {
    (*it).Peek();
  }
}

void WorkloadForecast::ExecuteSegments(
      const common::ManagedPointer<execution::exec::ExecutionContext> exec_ctx,
      const execution::exec::ExecutionSettings &exec_settings) {
  auto qid = forecast_segments_[0].query_ids_[0];
  auto num_exec = forecast_segments_[0].num_executions_[0];
  std::cout << qid << "; num_exec: " << num_exec << std::endl;
  // auto exec_query =
  //     execution::compiler::ExecutableQuery(query_id_to_string_[qid], common::ManagedPointer(exec_ctx),
  //                                          false, 16, exec_settings);
  // std::cout << "configured exec_query \n" << std::flush;
  // exec_ctx->SetParams(common::ManagedPointer<const std::vector<parser::ConstantValueExpression>>(&query_id_to_param_[qid][0]));
  // std::cout << "SetParams succ \n" << std::flush;
  // exec_query.Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
  // std::cout << "Run query succ \n" << std::flush;
}

void WorkloadForecast::CreateSegments(
    std::map<uint64_t, std::pair<execution::query_id_t, uint64_t>> query_timestamp_to_id,
    std::unordered_map<execution::query_id_t, std::vector<uint64_t>> num_executions) {
  
  std::vector<WorkloadForecastSegment> segments;
  std::vector<execution::query_id_t> seg_qids;
  std::vector<uint64_t> seg_executions;
  uint64_t curr_time = query_timestamp_to_id.begin()->first;

  for (auto it = query_timestamp_to_id.begin(); it != query_timestamp_to_id.end(); it++) {
    if (it->first > curr_time + forecast_interval_){
      segments.push_back(WorkloadForecastSegment(seg_qids, seg_executions));
      curr_time = it->first;
      seg_qids.clear();
      seg_executions.clear();
    }
    seg_qids.push_back(it->second.first);
    seg_executions.push_back(num_executions[it->second.first][it->second.second]);
  }

  if (seg_qids.size() > 0) {
    segments.push_back(WorkloadForecastSegment(seg_qids, seg_executions));
  }
  forecast_segments_ = segments;
  num_forecast_segment_ = segments.size();
}


}  // namespace terrier::brain::forecast
