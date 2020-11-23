#include "self_driving/modeling/pilot_util.h"

#include "binder/bind_node_visitor.h"
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
#include "parser/expression/constant_value_expression.h"
#include "traffic_cop/traffic_cop_util.h"
#include "transaction/transaction_manager.h"

namespace noisepage::selfdriving {

void PilotUtil::CollectPipelineFeatures(common::ManagedPointer<DBMain> db_main,
                                        common::ManagedPointer<selfdriving::WorkloadForecast> forecast) {
  auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
  auto catalog = db_main->GetCatalogLayer()->GetCatalog();
  transaction::TransactionContext *txn;
  auto metrics_manager = db_main->GetMetricsManager();

  execution::exec::ExecutionSettings exec_settings{};
  exec_settings.UpdateFromSettingsManager(db_main->GetSettingsManager());

  execution::exec::NoOpResultConsumer consumer;
  execution::exec::OutputCallback callback = consumer;

  execution::query_id_t qid;
  catalog::db_oid_t db_oid;

  for (auto &it : forecast->query_id_to_params_) {
    qid = it.first;
    for (auto &params : forecast->query_id_to_params_[qid]) {
      txn = txn_manager->BeginTransaction();

      auto stmt_list = parser::PostgresParser::BuildParseTree(forecast->query_id_to_text_[qid]);
      db_oid = static_cast<catalog::db_oid_t>(forecast->query_id_to_dboid_[qid]);
      // std::cout << "1.1. Got DB oid \n" << std::flush;
      std::unique_ptr<catalog::CatalogAccessor> accessor =
          catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

      auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid);
      binder.BindNameToNode(common::ManagedPointer(stmt_list), common::ManagedPointer(&params),
                            common::ManagedPointer(&(forecast->query_id_to_param_types_[qid])));

      // Creating exec_ctx
      std::unique_ptr<optimizer::AbstractCostModel> cost_model = std::make_unique<optimizer::TrivialCostModel>();

      auto out_plan = trafficcop::TrafficCopUtil::Optimize(
          common::ManagedPointer(txn), common::ManagedPointer(accessor), common::ManagedPointer(stmt_list), db_oid,
          db_main->GetStatsStorage(), std::move(cost_model), forecast->optimizer_timeout_);

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
    }
  }

  // retrieve the features

  metrics_manager->Aggregate();
  // Commented out since currently not performing any actions on aggregated data
  //  const auto aggregated_data = reinterpret_cast<metrics::PipelineMetricRawData *>(
  //      metrics_manager->AggregatedMetrics()
  //          .at(static_cast<uint8_t>(metrics::MetricsComponent::EXECUTION_PIPELINE))
  //          .get());
  //  NOISEPAGE_ASSERT(aggregated_data->pipeline_data_.size() >= query_id_to_param_.size(),
  //                   "Expect at least one pipeline_metrics record for each query");
  //  printf("Printing qid and pipeline id to sanity check pipeline metrics recorded");
  //  for (auto it = aggregated_data->pipeline_data_.begin(); it != aggregated_data->pipeline_data_.end(); it++) {
  //    printf("qid: %u; ppl_id: %u", static_cast<uint>(it->query_id_), static_cast<uint32_t>(it->pipeline_id_));
  //  }
  metrics_manager->ToCSV();
}

}  // namespace noisepage::selfdriving
