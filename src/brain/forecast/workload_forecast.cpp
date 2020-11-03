#include "brain/forecast/workload_forecast.h"

#include <unordered_map>
#include <map>
#include <memory>
#include <vector>
#include <string>
#include <utility>

#include "brain/operating_unit.h"
#include "execution/exec_defs.h"

#include "brain/forecast/workload_forecast_segment.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/execution_settings.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/executable_query.h"
#include "brain/forecast/workload_forecast_segment.h"
#include "parser/expression/constant_value_expression.h"
#include "common/action_context.h"
#include "common/error/exception.h"
#include "common/macros.h"
#include "common/managed_pointer.h"
#include "common/shared_latch.h"
#include "execution/exec_defs.h"
#include "execution/exec/execution_context.h"
#include "main/db_main.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/optimizer.h"
#include "traffic_cop/traffic_cop_util.h"
#include "transaction/transaction_manager.h"

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

void WorkloadForecast::ExecuteSegments(const common::ManagedPointer<DBMain> db_main) {
  auto qid = forecast_segments_[0].query_ids_[0];
  auto num_exec = forecast_segments_[0].num_executions_[0];
  std::cout << qid << "; num_exec: " << num_exec << std::endl;
  // std::cout << query_id_to_string_[qid] << std::endl << std::flush;
  auto stmt_list = parser::PostgresParser::BuildParseTree("UPDATE WAREHOUSE   SET W_YTD = W_YTD + 4290.490234375  WHERE W_ID = 1 ");

  auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
  transaction::TransactionContext *txn = txn_manager->BeginTransaction();
  std::cout << "1. Transaction began \n" << std::flush;

  // Creating exec_ctx
  auto catalog = db_main->GetCatalogLayer()->GetCatalog();
  catalog::db_oid_t db_oid = static_cast<catalog::db_oid_t>(1);
  std::cout << "1.3. Got DB oid \n" << std::flush;
  std::unique_ptr<catalog::CatalogAccessor> accessor = catalog->GetAccessor(common::ManagedPointer(txn), 
                                                                            db_oid, DISABLED);

  std::unique_ptr<optimizer::AbstractCostModel> cost_model = std::make_unique<optimizer::TrivialCostModel>();
  std::cout << "1.5. Got cost model \n" << std::flush;

  auto out_plan = trafficcop::TrafficCopUtil::Optimize(
        common::ManagedPointer(txn), common::ManagedPointer(accessor), common::ManagedPointer(stmt_list), db_oid,
        db_main->GetStatsStorage(), std::move(cost_model), optimizer_timeout_);
  std::cout << "2. Compiled out_plan \n" << std::flush;

  execution::exec::ExecutionSettings exec_settings{};
  exec_settings.UpdateFromSettingsManager(db_main->GetSettingsManager());

  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
        db_oid, common::ManagedPointer(txn), execution::exec::NoOpResultConsumer(), out_plan->GetOutputSchema().Get(),
        common::ManagedPointer(accessor), exec_settings, db_main->GetMetricsManager());
  std::cout << "3. Compiled exec context\n" << std::flush;

  auto exec_query = execution::compiler::CompilationContext::Compile(*out_plan, exec_settings, accessor.get(),
                                                                      execution::compiler::CompilationMode::OneShot);
  std::cout << "4. Compiled exec_query\n" << std::flush;

  auto units = std::make_unique<brain::PipelineOperatingUnits>();

  exec_query->SetPipelineOperatingUnits(std::move(units));
  std::cout << "5. Units Set Successfully \n" << std::flush;
  
  // exec_ctx->SetParams(common::ManagedPointer<const std::vector<parser::ConstantValueExpression>>(&query_id_to_param_[qid][0]));
  // std::cout << "SetParams succ \n" << std::flush;
  // exec_query.Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
  // std::cout << "Run query succ \n" << std::flush;
  txn_manager->Abort(txn);
  std::cout << "6. Transaction Aborted \n" << std::flush;

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
