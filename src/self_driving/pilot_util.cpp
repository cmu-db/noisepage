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
#include "metrics/metrics_util.h"
#include "network/postgres/statement.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/postgresparser.h"
#include "parser/variable_set_statement.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "self_driving/forecast/workload_forecast.h"
#include "self_driving/model_server/model_server_manager.h"
#include "self_driving/modeling/operating_unit.h"
#include "self_driving/pilot/pilot.h"
#include "settings/settings_manager.h"
#include "traffic_cop/traffic_cop_util.h"
#include "transaction/transaction_manager.h"

namespace noisepage::selfdriving {

void PilotUtil::ApplyAction(common::ManagedPointer<Pilot> pilot, const std::string &sql_query,
                            catalog::db_oid_t db_oid) {
  auto txn_manager = pilot->txn_manager_;
  auto catalog = pilot->catalog_;
  transaction::TransactionContext *txn;

  execution::exec::ExecutionSettings exec_settings{};
  exec_settings.UpdateFromSettingsManager(pilot->settings_manager_);
  execution::exec::NoOpResultConsumer consumer;
  execution::exec::OutputCallback callback = consumer;

  std::string query = sql_query;
  auto parse_tree = parser::PostgresParser::BuildParseTree(sql_query);
  auto statement = std::make_unique<network::Statement>(std::move(query), std::move(parse_tree));
  if (statement->GetQueryType() == network::QueryType::QUERY_SET) {
    const auto &set_stmt = statement->RootStatement().CastManagedPointerTo<parser::VariableSetStatement>();
    pilot->settings_manager_->SetParameter(set_stmt->GetParameterName(), set_stmt->GetValues());
  } else {
    std::unique_ptr<optimizer::AbstractCostModel> cost_model = std::make_unique<optimizer::TrivialCostModel>();
    txn = txn_manager->BeginTransaction();

    auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

    auto out_plan = PilotUtil::GetOutPlan(txn, common::ManagedPointer(accessor), nullptr, nullptr,
                                          common::ManagedPointer(parse_tree), db_oid, pilot->stats_storage_,
                                          pilot->forecast_->optimizer_timeout_);

    auto exec_query = execution::compiler::CompilationContext::Compile(*out_plan, exec_settings, accessor.get(),
                                                                       execution::compiler::CompilationMode::OneShot);

    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
        db_oid, common::ManagedPointer(txn), callback, out_plan->GetOutputSchema().Get(),
        common::ManagedPointer(accessor), exec_settings, pilot->metrics_thread_->GetMetricsManager());

    exec_query->Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }
}

std::unique_ptr<planner::AbstractPlanNode> PilotUtil::GetOutPlan(
    transaction::TransactionContext *txn, common::ManagedPointer<catalog::CatalogAccessor> accessor,
    common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
    common::ManagedPointer<std::vector<type::TypeId>> param_types,
    common::ManagedPointer<parser::ParseResult> stmt_list, catalog::db_oid_t db_oid,
    common::ManagedPointer<optimizer::StatsStorage> stats_storage, uint64_t optimizer_timeout) {
  auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid);
  binder.BindNameToNode(stmt_list, params, param_types);
  // Creating exec_ctx
  std::unique_ptr<optimizer::AbstractCostModel> cost_model = std::make_unique<optimizer::TrivialCostModel>();

  auto out_plan = trafficcop::TrafficCopUtil::Optimize(common::ManagedPointer(txn), common::ManagedPointer(accessor),
                                                       common::ManagedPointer(stmt_list), db_oid, stats_storage,
                                                       std::move(cost_model), optimizer_timeout)
                      ->TakePlanNodeOwnership();
  return out_plan;
}

void PilotUtil::GetQueryPlans(common::ManagedPointer<Pilot> pilot, common::ManagedPointer<WorkloadForecast> forecast,
                              uint64_t end_segment_index,
                              std::vector<std::unique_ptr<planner::AbstractPlanNode>> *plan_vecs) {
  auto txn_manager = pilot->txn_manager_;
  auto catalog = pilot->catalog_;
  transaction::TransactionContext *txn = txn_manager->BeginTransaction();

  for (auto idx = 0; idx <= end_segment_index; idx++) {
    for (auto &it : forecast->forecast_segments_[idx].id_to_num_exec_) {
      auto stmt_list = parser::PostgresParser::BuildParseTree(forecast->query_id_to_text_[it.first]);
      auto db_oid = static_cast<catalog::db_oid_t>(forecast->query_id_to_dboid_[it.first]);
      auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

      auto out_plan = PilotUtil::GetOutPlan(
          txn, common::ManagedPointer(accessor), common::ManagedPointer(&forecast->query_id_to_params_[it.first][0]),
          common::ManagedPointer(&(forecast->query_id_to_param_types_[it.first])), common::ManagedPointer(stmt_list),
          db_oid, pilot->stats_storage_, forecast->optimizer_timeout_);

      plan_vecs->emplace_back(std::move(out_plan));
    }
  }
  txn_manager->Abort(txn);
}

uint64_t PilotUtil::ComputeCost(common::ManagedPointer<Pilot> pilot, common::ManagedPointer<WorkloadForecast> forecast,
                                uint64_t start_segment_index, uint64_t end_segment_index) {
  // Compute cost as average latency of queries weighted by their num of exec
  // Keep track of the ou inference results (a vector of doubles) for each ous associated with the pipeline
  // under different parameters for each pipeline
  // Each element of the outermost vector is the ou inferences for one set of parameters
  std::map<std::pair<execution::query_id_t, execution::pipeline_id_t>, std::vector<std::vector<std::vector<double>>>>
      pipeline_to_prediction;
  pilot->ExecuteForecast(&pipeline_to_prediction, start_segment_index, end_segment_index);

  std::vector<std::pair<execution::query_id_t, double>> query_cost;
  execution::query_id_t prev_qid = pipeline_to_prediction.begin()->first.first;
  query_cost.emplace_back(prev_qid, 0);

  for (auto ppl_to_pred : pipeline_to_prediction) {
    auto ppl_sum = 0;
    for (auto ppl_res : ppl_to_pred.second) {
      for (auto ou_res : ppl_res) {
        // sum up the latency of ous
        ppl_sum += ou_res[ou_res.size() - 1];
      }
    }
    // record average cost of this pipeline among the same queries with diff param
    if (prev_qid == ppl_to_pred.first.first) {
      query_cost.back().second += ppl_sum / ppl_to_pred.second.size();
    } else {
      query_cost.emplace_back(ppl_to_pred.first.first, ppl_sum / ppl_to_pred.second.size());
      prev_qid = ppl_to_pred.first.first;
    }
  }
  uint128_t total_cost = 0, num_queries = 0;
  for (auto qcost : query_cost) {
    for (auto i = start_segment_index; i <= end_segment_index; i++) {
      total_cost += forecast->forecast_segments_[i].id_to_num_exec_[qcost.first] * qcost.second;
      num_queries += forecast->forecast_segments_[i].id_to_num_exec_[qcost.first];
    }
  }
  NOISEPAGE_ASSERT(num_queries > 0, "expect more then one query");
  return total_cost / num_queries;
}

const std::list<metrics::PipelineMetricRawData::PipelineData> &PilotUtil::CollectPipelineFeatures(
    common::ManagedPointer<selfdriving::Pilot> pilot, common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
    uint64_t start_segment_index, uint64_t end_segment_index, std::vector<execution::query_id_t> *pipeline_qids) {
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
    txn = txn_manager->BeginTransaction();
    auto stmt_list = parser::PostgresParser::BuildParseTree(forecast->query_id_to_text_[qid]);
    db_oid = static_cast<catalog::db_oid_t>(forecast->query_id_to_dboid_[qid]);
    std::unique_ptr<catalog::CatalogAccessor> accessor =
        catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

    auto out_plan = PilotUtil::GetOutPlan(
        txn, common::ManagedPointer(accessor), common::ManagedPointer(&(forecast->query_id_to_params_[qid][0])),
        common::ManagedPointer(&(forecast->query_id_to_param_types_[qid])), common::ManagedPointer(stmt_list), db_oid,
        pilot->stats_storage_, forecast->optimizer_timeout_);

    execution::compiler::ExecutableQuery::query_identifier.store(qid);
    auto exec_query = execution::compiler::CompilationContext::Compile(*out_plan, exec_settings, accessor.get(),
                                                                       execution::compiler::CompilationMode::OneShot);
    pipeline_qids->push_back(qid);
    for (auto &params : forecast->query_id_to_params_[qid]) {
      auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
          db_oid, common::ManagedPointer(txn), callback, out_plan->GetOutputSchema().Get(),
          common::ManagedPointer(accessor), exec_settings, metrics_manager);

      exec_ctx->SetParams(common::ManagedPointer<const std::vector<parser::ConstantValueExpression>>(&params));
      exec_query->Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
    }
    txn_manager->Abort(txn);
  }

  // retrieve the features
  metrics_manager->Aggregate();

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

void PilotUtil::InferenceWithFeatures(const std::string &model_save_path,
                                      common::ManagedPointer<modelserver::ModelServerManager> model_server_manager,
                                      const std::vector<execution::query_id_t> &pipeline_qids,
                                      const std::list<metrics::PipelineMetricRawData::PipelineData> &pipeline_data,
                                      std::map<std::pair<execution::query_id_t, execution::pipeline_id_t>,
                                               std::vector<std::vector<std::vector<double>>>> *pipeline_to_prediction) {
  std::unordered_map<ExecutionOperatingUnitType, std::vector<std::vector<double>>> ou_to_features;
  std::list<std::tuple<execution::query_id_t, execution::pipeline_id_t,
                       std::vector<std::pair<ExecutionOperatingUnitType, uint64_t>>>>
      pipeline_to_ou_position;

  PilotUtil::GroupFeaturesByOU(&pipeline_to_ou_position, pipeline_qids, pipeline_data, &ou_to_features);
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
    const std::vector<execution::query_id_t> &pipeline_qids,
    const std::list<metrics::PipelineMetricRawData::PipelineData> &pipeline_data,
    std::unordered_map<ExecutionOperatingUnitType, std::vector<std::vector<double>>> *ou_to_features) {
  auto pipeline_idx = 0;

  if (pipeline_data.empty()) return;
  execution::query_id_t prev_qid = pipeline_data.begin()->query_id_;
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
      std::vector<double> predictors;
      predictors.push_back(metrics::MetricsUtil::GetHardwareContext().cpu_mhz_);
      predictors.push_back(data_it.execution_mode_);
      ou_it.GetAllAttributes(&predictors);
      ou_to_features->at(ou_it.GetExecutionOperatingUnitType()).push_back(predictors);
    }

    if (data_it.query_id_ != prev_qid) {
      pipeline_idx += 1;
      prev_qid = data_it.query_id_;
    }
    SELFDRIVING_LOG_INFO(fmt::format("Fixed qid: {}; ppl_id: {}", static_cast<uint>(pipeline_qids.at(pipeline_idx)),
                                     static_cast<uint32_t>(data_it.pipeline_id_)));
    pipeline_to_ou_position->emplace_back(pipeline_qids.at(pipeline_idx), data_it.pipeline_id_,
                                          std::move(ou_positions));
  }
}

}  // namespace noisepage::selfdriving
