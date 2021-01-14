#include "self_driving/pilot_util.h"

#include "binder/bind_node_visitor.h"
#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "common/managed_pointer.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/executable_query.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/execution_settings.h"
#include "execution/exec_defs.h"
#include "loggers/selfdriving_logger.h"
#include "messenger/messenger.h"
#include "metrics/metrics_thread.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/postgresparser.h"
#include "self_driving/forecast/workload_forecast.h"
#include "self_driving/model_server/model_server_manager.h"
#include "self_driving/pilot/pilot.h"
#include "traffic_cop/traffic_cop_util.h"
#include "transaction/transaction_manager.h"

namespace noisepage::selfdriving {

void PilotUtil::ApplyAction(
    common::ManagedPointer<Pilot> pilot, const std::vector<uint64_t> &db_oids, const std::string &sql_query) {
  auto txn_manager = pilot->txn_manager_;
  auto catalog = pilot->catalog_;
  transaction::TransactionContext *txn;

  execution::exec::ExecutionSettings exec_settings{};
  exec_settings.UpdateFromSettingsManager(pilot->settings_manager_);
  execution::exec::NoOpResultConsumer consumer;
  execution::exec::OutputCallback callback = consumer;

  auto parse_tree = parser::PostgresParser::BuildParseTree(sql_query);
  catalog::db_oid_t db_oid;
  for (auto oid : db_oids) {
    std::unique_ptr<optimizer::AbstractCostModel> cost_model = std::make_unique<optimizer::TrivialCostModel>();
    txn = txn_manager->BeginTransaction();
    db_oid = static_cast<catalog::db_oid_t>(oid);
    auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

    auto binder = new binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid);
    binder->BindNameToNode(common::ManagedPointer(parse_tree), nullptr, nullptr);

    auto out_plan =
        trafficcop::TrafficCopUtil::Optimize(common::ManagedPointer(txn), common::ManagedPointer(accessor),
                                             common::ManagedPointer(parse_tree), db_oid, pilot->stats_storage_,
                                             std::move(cost_model), pilot->forecast_->optimizer_timeout_)
            ->TakePlanNodeOwnership();

    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
        db_oid, common::ManagedPointer(txn), callback, out_plan->GetOutputSchema().Get(),
        common::ManagedPointer(accessor), exec_settings, pilot->metrics_thread_->GetMetricsManager());

    auto exec_query = execution::compiler::CompilationContext::Compile(*out_plan, exec_settings, accessor.get(),
                                                                       execution::compiler::CompilationMode::OneShot);
    exec_query->Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    txn_manager->Abort(txn);
  }
}

std::vector<std::unique_ptr<planner::AbstractPlanNode>> PilotUtil::GetQueryPlans(
    common::ManagedPointer<Pilot> pilot, common::ManagedPointer<WorkloadForecast> forecast,
    uint64_t start_segment_index, uint64_t end_segment_index) {
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> plan_vecs;
  auto txn_manager = pilot->txn_manager_;
  auto catalog = pilot->catalog_;
  transaction::TransactionContext *txn = txn_manager->BeginTransaction();

  for (auto idx = start_segment_index; idx <= end_segment_index; idx++) {
    for (auto &it : forecast->forecast_segments_[idx].id_to_num_exec_) {
      auto stmt_list = parser::PostgresParser::BuildParseTree(forecast->query_id_to_text_[it.first]);
      auto db_oid = static_cast<catalog::db_oid_t>(forecast->query_id_to_dboid_[it.first]);
      auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

      auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid);
      binder.BindNameToNode(common::ManagedPointer(stmt_list), nullptr, nullptr);

      // Creating exec_ctx
      std::unique_ptr<optimizer::AbstractCostModel> cost_model = std::make_unique<optimizer::TrivialCostModel>();

      auto out_plan =
          trafficcop::TrafficCopUtil::Optimize(common::ManagedPointer(txn), common::ManagedPointer(accessor),
                                               common::ManagedPointer(stmt_list), db_oid, pilot->stats_storage_,
                                               std::move(cost_model), forecast->optimizer_timeout_)
              ->TakePlanNodeOwnership();
      plan_vecs.emplace_back(std::move(out_plan));
    }
  }
  txn_manager->Abort(txn);
  return plan_vecs;

}

uint64_t PilotUtil::ComputeCost(common::ManagedPointer<Pilot> pilot,
                                common::ManagedPointer<WorkloadForecast> forecast,
                                uint64_t start_segment_index, uint64_t end_segment_index) {
  std::map<std::pair<execution::query_id_t, execution::pipeline_id_t>,
           std::vector<std::vector<std::vector<double>>>> pipeline_to_prediction;
  pilot->ExecuteForecast(&pipeline_to_prediction, start_segment_index, end_segment_index);
  // TODO: Compute cost here
  return 0;
}

const std::list<metrics::PipelineMetricRawData::PipelineData> &PilotUtil::CollectPipelineFeatures(
    common::ManagedPointer<selfdriving::Pilot> pilot, common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
    uint64_t start_segment_index, uint64_t end_segment_index) {
  auto txn_manager = pilot->txn_manager_;
  auto catalog = pilot->catalog_;
  transaction::TransactionContext *txn;
  auto metrics_manager = pilot->metrics_thread_->GetMetricsManager();

  execution::exec::ExecutionSettings exec_settings{};
  exec_settings.UpdateFromSettingsManager(pilot->settings_manager_);

  execution::exec::NoOpResultConsumer consumer;
  execution::exec::OutputCallback callback = consumer;

  catalog::db_oid_t db_oid;

  std::unordered_set<execution::query_id_t> qids;
  for (auto i = start_segment_index; i <= end_segment_index; i++) {
    for (auto &it : forecast->forecast_segments_[i].id_to_num_exec_) {
      qids.insert(it.first);
    }
  }

  for (const auto &qid : qids) {
    for (auto &params : forecast->query_id_to_params_[qid]) {
      txn = txn_manager->BeginTransaction();
      auto stmt_list = parser::PostgresParser::BuildParseTree(forecast->query_id_to_text_[qid]);
      db_oid = static_cast<catalog::db_oid_t>(forecast->query_id_to_dboid_[qid]);
      std::unique_ptr<catalog::CatalogAccessor> accessor =
          catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

      auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid);
      binder.BindNameToNode(common::ManagedPointer(stmt_list), common::ManagedPointer(&params),
                            common::ManagedPointer(&(forecast->query_id_to_param_types_[qid])));

      // Creating exec_ctx
      std::unique_ptr<optimizer::AbstractCostModel> cost_model = std::make_unique<optimizer::TrivialCostModel>();

      auto out_plan =
          trafficcop::TrafficCopUtil::Optimize(common::ManagedPointer(txn), common::ManagedPointer(accessor),
                                               common::ManagedPointer(stmt_list), db_oid, pilot->stats_storage_,
                                               std::move(cost_model), forecast->optimizer_timeout_)
              ->TakePlanNodeOwnership();

      auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
          db_oid, common::ManagedPointer(txn), callback, out_plan->GetOutputSchema().Get(),
          common::ManagedPointer(accessor), exec_settings, metrics_manager);

      exec_ctx->SetParams(common::ManagedPointer<const std::vector<parser::ConstantValueExpression>>(&params));

      execution::compiler::ExecutableQuery::query_identifier.store(qid);
      auto exec_query = execution::compiler::CompilationContext::Compile(*out_plan, exec_settings, accessor.get(),
                                                                         execution::compiler::CompilationMode::OneShot);
      exec_query->Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
      std::this_thread::sleep_for(std::chrono::seconds(1));
      txn_manager->Abort(txn);
    }
  }

  // retrieve the features
  metrics_manager->Aggregate();

  // Commented out since currently not performing any actions on aggregated data
  auto aggregated_data = reinterpret_cast<metrics::PipelineMetricRawData *>(
      metrics_manager->AggregatedMetrics()
          .at(static_cast<uint8_t>(metrics::MetricsComponent::EXECUTION_PIPELINE))
          .get());
  SELFDRIVING_LOG_INFO("Printing qid and pipeline id to sanity check pipeline metrics recorded");
  for (auto it = aggregated_data->pipeline_data_.begin(); it != aggregated_data->pipeline_data_.end(); it++) {
    SELFDRIVING_LOG_INFO(
        fmt::format("qid: {}; ppl_id: {}", static_cast<uint>(it->query_id_), static_cast<uint32_t>(it->pipeline_id_)));
  }

  return aggregated_data->pipeline_data_;
}

void PilotUtil::InferenceWithFeatures(
    const std::string &model_save_path, common::ManagedPointer<modelserver::ModelServerManager> model_server_manager,
    const std::list<metrics::PipelineMetricRawData::PipelineData> &pipeline_data,
    std::map<std::pair<execution::query_id_t, execution::pipeline_id_t>, std::vector<std::vector<std::vector<double>>>>
        *pipeline_to_prediction) {
  std::unordered_map<ExecutionOperatingUnitType, std::vector<std::vector<double>>> ou_to_features;
  std::list<std::tuple<execution::query_id_t, execution::pipeline_id_t,
                       std::vector<std::pair<ExecutionOperatingUnitType, uint64_t>>>>
      pipeline_to_ou_position;

  PilotUtil::GroupFeaturesByOU(&pipeline_to_ou_position, pipeline_data, &ou_to_features);
  NOISEPAGE_ASSERT(model_server_manager->ModelServerStarted(), "Model Server should have been started");
  std::string project_build_path = getenv(Pilot::BUILD_ABS_PATH);
  std::unordered_map<ExecutionOperatingUnitType, std::vector<std::vector<double>>> inference_result;
  for (auto &ou_map_it : ou_to_features) {
    auto res = model_server_manager->DoInference(
        selfdriving::OperatingUnitUtil::ExecutionOperatingUnitTypeToString(ou_map_it.first),
        project_build_path + model_save_path, ou_map_it.second);
    if (!res.second) {
      throw PILOT_EXCEPTION("Inference through model server manager has error", common::ErrorCode::ERRCODE_WARNING);
    }
    inference_result.emplace(ou_map_it.first, res.first);
  }

  // populate pipeline_to_prediction using pipeline_to_ou_position and inference_result
  std::vector<std::vector<double>> ppl_res;
  for (auto ppl_to_ou : pipeline_to_ou_position) {
    for (auto ou_it : std::get<2>(ppl_to_ou)) {
      ppl_res.push_back(inference_result.at(ou_it.first)[ou_it.second]);
    }
    auto ppl_key = std::make_pair(std::get<0>(ppl_to_ou), std::get<1>(ppl_to_ou));
    if (pipeline_to_prediction->find(ppl_key) == pipeline_to_prediction->end()) {
      pipeline_to_prediction->emplace(ppl_key, std::vector<std::vector<std::vector<double>>>());
    }
    pipeline_to_prediction->at(ppl_key).emplace_back(std::move(ppl_res));
  }
}

void PilotUtil::GroupFeaturesByOU(
    std::list<std::tuple<execution::query_id_t, execution::pipeline_id_t,
                         std::vector<std::pair<ExecutionOperatingUnitType, uint64_t>>>> *pipeline_to_ou_position,
    const std::list<metrics::PipelineMetricRawData::PipelineData> &pipeline_data,
    std::unordered_map<ExecutionOperatingUnitType, std::vector<std::vector<double>>> *ou_to_features) {
  for (const auto &data_it : pipeline_data) {
    std::vector<std::pair<ExecutionOperatingUnitType, uint64_t>> ou_positions;
    for (const auto &ou_it : data_it.features_) {
      if (ou_to_features->find(ou_it.GetExecutionOperatingUnitType()) == ou_to_features->end()) {
        ou_positions.emplace_back(ou_it.GetExecutionOperatingUnitType(), 0);
        ou_to_features->emplace(ou_it.GetExecutionOperatingUnitType(), std::vector<std::vector<double>>());
      } else {
        ou_positions.emplace_back(ou_it.GetExecutionOperatingUnitType(),
                                  ou_to_features->at(ou_it.GetExecutionOperatingUnitType()).size());
      }
      auto predictors = ou_it.GetAllAttributes();
      predictors.insert(predictors.begin(), data_it.execution_mode_);
      ou_to_features->at(ou_it.GetExecutionOperatingUnitType()).push_back(predictors);
    }
    pipeline_to_ou_position->emplace_back(data_it.query_id_, data_it.pipeline_id_, std::move(ou_positions));
  }
}

}  // namespace noisepage::selfdriving
