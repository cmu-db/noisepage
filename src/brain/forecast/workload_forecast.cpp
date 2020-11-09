#include "brain/forecast/workload_forecast.h"

#include <map>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "binder/bind_node_visitor.h"
#include "brain/forecast/workload_forecast_segment.h"
#include "common/action_context.h"
#include "common/error/exception.h"
#include "common/macros.h"
#include "common/managed_pointer.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/executable_query.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/execution_settings.h"
#include "execution/exec_defs.h"
#include "main/db_main.h"
#include "metrics/metrics_store.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/optimizer.h"
#include "parser/expression/constant_value_expression.h"
#include "traffic_cop/traffic_cop_util.h"
#include "transaction/transaction_manager.h"

namespace noisepage::brain {

WorkloadForecast::WorkloadForecast(
    std::map<uint64_t, std::pair<execution::query_id_t, uint64_t>> query_timestamp_to_id,
    std::unordered_map<execution::query_id_t, std::vector<uint64_t>> num_executions,
    std::unordered_map<execution::query_id_t, std::string> query_id_to_string,
    std::unordered_map<std::string, execution::query_id_t> query_string_to_id,
    std::unordered_map<execution::query_id_t, std::vector<std::vector<parser::ConstantValueExpression>>>
        query_id_to_param,
    std::unordered_map<execution::query_id_t, uint64_t> query_id_to_dboid, uint64_t forecast_interval)
    : query_id_to_string_(std::move(query_id_to_string)),
      query_string_to_id_(std::move(query_string_to_id)),
      query_id_to_param_(std::move(query_id_to_param)),
      query_id_to_dboid_(std::move(query_id_to_dboid)),
      forecast_interval_(forecast_interval) {
  CreateSegments(query_timestamp_to_id, num_executions);
}

std::vector<parser::ConstantValueExpression> WorkloadForecast::SampleParam(execution::query_id_t qid) {
  return query_id_to_param_[qid][rand() % query_id_to_param_[qid].size()];
}

void WorkloadForecast::ExecuteSegments(common::ManagedPointer<DBMain> db_main) {
  auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
  auto catalog = db_main->GetCatalogLayer()->GetCatalog();
  transaction::TransactionContext *txn = nullptr;
  auto metrics_manager = db_main->GetMetricsManager();

  execution::exec::ExecutionSettings exec_settings{};
  exec_settings.UpdateFromSettingsManager(db_main->GetSettingsManager());

  execution::exec::NoOpResultConsumer consumer;
  execution::exec::OutputCallback callback = consumer;

  execution::query_id_t qid;
  catalog::db_oid_t db_oid;
  std::vector<type::TypeId> param_types;
  std::vector<parser::ConstantValueExpression> params;

  for (auto &it : query_id_to_param_) {
    qid = it.first;
    for (auto &params_it : query_id_to_param_[qid]) {
      params = params_it;
      for (auto &param_it : params) {
        param_types.push_back(param_it.GetReturnValueType());
      }

      txn = txn_manager->BeginTransaction();
      // std::cout << "1. Transaction began \n" << std::flush;
      auto stmt_list = parser::PostgresParser::BuildParseTree(query_id_to_string_[qid]);
      db_oid = static_cast<catalog::db_oid_t>(query_id_to_dboid_[qid]);
      // std::cout << "1.1. Got DB oid \n" << std::flush;
      std::unique_ptr<catalog::CatalogAccessor> accessor =
          catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

      auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid);
      binder.BindNameToNode(common::ManagedPointer(stmt_list), common::ManagedPointer(&params),
                            common::ManagedPointer(&param_types));

      // Creating exec_ctx
      std::unique_ptr<optimizer::AbstractCostModel> cost_model = std::make_unique<optimizer::TrivialCostModel>();

      auto out_plan = trafficcop::TrafficCopUtil::Optimize(
          common::ManagedPointer(txn), common::ManagedPointer(accessor), common::ManagedPointer(stmt_list), db_oid,
          db_main->GetStatsStorage(), std::move(cost_model), optimizer_timeout_);

      auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
          db_oid, common::ManagedPointer(txn), callback, out_plan->GetOutputSchema().Get(),
          common::ManagedPointer(accessor), exec_settings, db_main->GetMetricsManager());

      exec_ctx->SetParams(common::ManagedPointer<const std::vector<parser::ConstantValueExpression>>(&params));

      execution::compiler::ExecutableQuery::query_identifier.store(qid);
      auto exec_query = execution::compiler::CompilationContext::Compile(*out_plan, exec_settings, accessor.get(),
                                                                         execution::compiler::CompilationMode::OneShot);
      // std::cout << qid << ";\n " << std::flush;
      exec_query->Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
      // std::cout << "5. Run query succ \n" << std::flush;

      std::this_thread::sleep_for(std::chrono::seconds(1));

      txn_manager->Abort(txn);
      // std::cout << "6. Transaction Aborted \n" << std::flush;
      param_types.clear();
    }
  }

  // retrieve the features

  metrics_manager->Aggregate();
  const auto aggregated_data = reinterpret_cast<metrics::PipelineMetricRawData *>(
      metrics_manager->AggregatedMetrics()
          .at(static_cast<uint8_t>(metrics::MetricsComponent::EXECUTION_PIPELINE))
          .get());
  NOISEPAGE_ASSERT(aggregated_data->pipeline_data_.size() >= query_id_to_param_.size(),
                   "Expect at least one pipeline_metrics record for each query");
  //  for (auto it = aggregated_data->pipeline_data_.begin(); it != aggregated_data->pipeline_data_.end(); it++) {
  //    std::cout << "qid: " << it->query_id_ << "; ppl_id: " << it->pipeline_id_ << std::endl << std::flush;
  //  }
  metrics_manager->ToCSV();
}

void WorkloadForecast::CreateSegments(
    std::map<uint64_t, std::pair<execution::query_id_t, uint64_t>> &query_timestamp_to_id,
    std::unordered_map<execution::query_id_t, std::vector<uint64_t>> &num_executions) {
  std::vector<WorkloadForecastSegment> segments;

  std::unordered_map<execution::query_id_t, uint64_t> curr_segment;

  uint64_t curr_time = query_timestamp_to_id.begin()->first;

  for (auto &it : query_timestamp_to_id) {
    if (it.first > curr_time + forecast_interval_) {
      segments.emplace_back(curr_segment);
      curr_time = it.first;
      curr_segment.clear();
    }
    curr_segment[it.second.first] += num_executions[it.second.first][it.second.second];
  }

  if (!curr_segment.empty()) {
    segments.emplace_back(curr_segment);
  }
  forecast_segments_ = segments;
  num_forecast_segment_ = segments.size();
}

}  // namespace noisepage::brain
