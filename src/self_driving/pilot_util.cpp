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
#include "execution/sql/ddl_executors.h"
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

  // Initialize transaction context, and necessary arguments for query plan generation
  transaction::TransactionContext *txn;

  execution::exec::ExecutionSettings exec_settings{};
  exec_settings.UpdateFromSettingsManager(pilot->settings_manager_);
  execution::exec::NoOpResultConsumer consumer;
  execution::exec::OutputCallback callback = consumer;

  std::string query = sql_query;
  SELFDRIVING_LOG_INFO("Applying action: {}", sql_query);
  auto parse_tree = parser::PostgresParser::BuildParseTree(sql_query);
  auto statement = std::make_unique<network::Statement>(std::move(query), std::move(parse_tree));

  auto query_type = statement->GetQueryType();

  if (query_type == network::QueryType::QUERY_SET) {
    // The set statements are executed differently (through settings_manager_)
    const auto &set_stmt = statement->RootStatement().CastManagedPointerTo<parser::VariableSetStatement>();
    pilot->settings_manager_->SetParameter(set_stmt->GetParameterName(), set_stmt->GetValues());
  } else {
    // For all other actions, we apply the action by running it as query in a committed transaction
    txn = txn_manager->BeginTransaction();

    auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

    // Parameters are also specified in the query string, hence we have no parameters nor parameter types here
    auto out_plan =
        PilotUtil::GenerateQueryPlan(txn, common::ManagedPointer(accessor), nullptr, nullptr, statement->ParseResult(),
                                     db_oid, pilot->stats_storage_, pilot->forecast_->GetOptimizerTimeout());

    if (query_type == network::QueryType::QUERY_DROP_INDEX) {
      // Drop index does not need execution of compiled query
      execution::sql::DDLExecutors::DropIndexExecutor(common::ManagedPointer<planner::AbstractPlanNode>(out_plan)
                                                          .CastManagedPointerTo<planner::DropIndexPlanNode>(),
                                                      common::ManagedPointer<catalog::CatalogAccessor>(accessor));
    } else {
      if (query_type == network::QueryType::QUERY_CREATE_INDEX) {
        // TODO(lin): We actually don't need to populate the index tuples after creating the index placeholder for the
        //  "what-if" API. But since we need to execute the query to get the features, we need to compile and execute
        //  the query for now.
        execution::sql::DDLExecutors::CreateIndexExecutor(common::ManagedPointer<planner::AbstractPlanNode>(out_plan)
                                                              .CastManagedPointerTo<planner::CreateIndexPlanNode>(),
                                                          common::ManagedPointer<catalog::CatalogAccessor>(accessor));
      }

      auto exec_query = execution::compiler::CompilationContext::Compile(*out_plan, exec_settings, accessor.get(),
                                                                         execution::compiler::CompilationMode::OneShot);

      auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
          db_oid, common::ManagedPointer(txn), callback, out_plan->GetOutputSchema().Get(),
          common::ManagedPointer(accessor), exec_settings, pilot->metrics_thread_->GetMetricsManager());

      exec_query->Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
    }
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }
}

std::unique_ptr<planner::AbstractPlanNode> PilotUtil::GenerateQueryPlan(
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
                              uint64_t end_segment_index, transaction::TransactionContext *txn,
                              std::vector<std::unique_ptr<planner::AbstractPlanNode>> *plan_vecs) {
  std::unordered_set<execution::query_id_t> qids;
  auto catalog = pilot->catalog_;
  for (uint64_t idx = 0; idx <= end_segment_index; idx++) {
    for (auto &it : forecast->GetSegmentByIndex(idx).GetIdToNumexec()) {
      qids.insert(it.first);
    }
  }
  for (auto qid : qids) {
    auto stmt_list = parser::PostgresParser::BuildParseTree(forecast->GetQuerytextByQid(qid));
    auto db_oid = static_cast<catalog::db_oid_t>(forecast->GetDboidByQid(qid));
    auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

    auto out_plan = PilotUtil::GenerateQueryPlan(
        txn, common::ManagedPointer(accessor), common::ManagedPointer(&(forecast->GetQueryparamsByQid(qid)->at(0))),
        common::ManagedPointer(forecast->GetParamtypesByQid(qid)), common::ManagedPointer(stmt_list), db_oid,
        pilot->stats_storage_, forecast->GetOptimizerTimeout());

    plan_vecs->emplace_back(std::move(out_plan));
  }
}

double PilotUtil::ComputeCost(common::ManagedPointer<Pilot> pilot, common::ManagedPointer<WorkloadForecast> forecast,
                              uint64_t start_segment_index, uint64_t end_segment_index) {
  // Compute cost as total latency of queries based on their num of exec
  // pipeline_to_prediction maps each pipeline to a vector of ou inference results for all ous of this pipeline
  // (where each entry corresponds to a different query param)
  // Each element of the outermost vector is a vector of ou prediction (each being a double vector) for one set of
  // parameters
  std::map<std::pair<execution::query_id_t, execution::pipeline_id_t>, std::vector<std::vector<std::vector<double>>>>
      pipeline_to_prediction;
  pilot->ExecuteForecast(&pipeline_to_prediction, start_segment_index, end_segment_index);

  std::vector<std::pair<execution::query_id_t, double>> query_cost;
  execution::query_id_t prev_qid = pipeline_to_prediction.begin()->first.first;
  query_cost.emplace_back(prev_qid, 0);

  for (auto const &pipeline_to_pred : pipeline_to_prediction) {
    double pipeline_sum = 0;
    for (auto const &pipeline_res : pipeline_to_pred.second) {
      for (auto ou_res : pipeline_res) {
        // sum up the latency of ous
        pipeline_sum += ou_res[ou_res.size() - 1];
      }
    }
    // record average cost of this pipeline among the same queries with diff param
    if (prev_qid == pipeline_to_pred.first.first) {
      query_cost.back().second += pipeline_sum / pipeline_to_pred.second.size();
    } else {
      query_cost.emplace_back(pipeline_to_pred.first.first, pipeline_sum / pipeline_to_pred.second.size());
      prev_qid = pipeline_to_pred.first.first;
    }
  }
  double total_cost = 0, num_queries = 0;
  for (auto qcost : query_cost) {
    for (auto i = start_segment_index; i <= end_segment_index; i++) {
      total_cost += forecast->GetSegmentByIndex(i).GetIdToNumexec().at(qcost.first) * qcost.second;
      num_queries += forecast->GetSegmentByIndex(i).GetIdToNumexec().at(qcost.first);
    }
  }
  NOISEPAGE_ASSERT(num_queries > 0, "expect more then one query");
  return total_cost;
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
    for (auto &it : forecast->GetSegmentByIndex(i).GetIdToNumexec()) {
      qids.insert(it.first);
    }
  }

  for (const auto &qid : qids) {
    // Begin transaction to get the executable query
    txn = txn_manager->BeginTransaction();
    auto stmt_list = parser::PostgresParser::BuildParseTree(forecast->GetQuerytextByQid(qid));
    db_oid = static_cast<catalog::db_oid_t>(forecast->GetDboidByQid(qid));
    std::unique_ptr<catalog::CatalogAccessor> accessor =
        catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

    auto out_plan = PilotUtil::GenerateQueryPlan(
        txn, common::ManagedPointer(accessor), common::ManagedPointer(&(forecast->GetQueryparamsByQid(qid)->at(0))),
        common::ManagedPointer(forecast->GetParamtypesByQid(qid)), common::ManagedPointer(stmt_list), db_oid,
        pilot->stats_storage_, forecast->GetOptimizerTimeout());

    execution::compiler::ExecutableQuery::query_identifier.store(qid);
    auto exec_query = execution::compiler::CompilationContext::Compile(*out_plan, exec_settings, accessor.get(),
                                                                       execution::compiler::CompilationMode::OneShot);
    txn_manager->Abort(txn);

    pipeline_qids->push_back(qid);

    for (auto &params : *(forecast->GetQueryparamsByQid(qid))) {
      txn = txn_manager->BeginTransaction();
      auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
          db_oid, common::ManagedPointer(txn), callback, out_plan->GetOutputSchema().Get(),
          common::ManagedPointer(accessor), exec_settings, metrics_manager);

      exec_ctx->SetParams(common::ManagedPointer<const std::vector<parser::ConstantValueExpression>>(&params));
      exec_query->Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
      txn_manager->Abort(txn);
    }
  }

  // retrieve the features
  metrics_manager->Aggregate();

  auto aggregated_data = reinterpret_cast<metrics::PipelineMetricRawData *>(
      metrics_manager->AggregatedMetrics()
          .at(static_cast<uint8_t>(metrics::MetricsComponent::EXECUTION_PIPELINE))
          .get());
  SELFDRIVING_LOG_DEBUG("Printing qid and pipeline id to sanity check pipeline metrics recorded");
  for (auto it = aggregated_data->pipeline_data_.begin(); it != aggregated_data->pipeline_data_.end(); it++) {
    SELFDRIVING_LOG_DEBUG(fmt::format("qid: {}; pipeline_id: {}", static_cast<uint>(it->query_id_),
                                      static_cast<uint32_t>(it->pipeline_id_)));
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
  std::unordered_map<ExecutionOperatingUnitType, std::vector<std::vector<double>>> inference_result;
  for (auto &ou_map_it : ou_to_features) {
    auto res = model_server_manager->DoInference(
        selfdriving::OperatingUnitUtil::ExecutionOperatingUnitTypeToString(ou_map_it.first), model_save_path,
        ou_map_it.second);
    if (!res.second) {
      throw PILOT_EXCEPTION("Inference through model server manager has error", common::ErrorCode::ERRCODE_WARNING);
    }
    inference_result.emplace(ou_map_it.first, res.first);
  }

  // populate pipeline_to_prediction using pipeline_to_ou_position and inference_result
  for (auto pipeline_to_ou : pipeline_to_ou_position) {
    std::vector<std::vector<double>> pipeline_result;
    for (auto ou_it : std::get<2>(pipeline_to_ou)) {
      // for a fixed set of query parameter, collect all of this pipeline's associated operating unit's prediction
      // result, prediction result of one ou is a double vector
      pipeline_result.push_back(inference_result.at(ou_it.first)[ou_it.second]);
    }
    auto pipeline_key = std::make_pair(std::get<0>(pipeline_to_ou), std::get<1>(pipeline_to_ou));
    if (pipeline_to_prediction->find(pipeline_key) == pipeline_to_prediction->end()) {
      // collect this pipeline's ou predictions under different query parameters
      pipeline_to_prediction->emplace(pipeline_key, std::vector<std::vector<std::vector<double>>>());
    }
    pipeline_to_prediction->at(pipeline_key).emplace_back(std::move(pipeline_result));
  }
}

void PilotUtil::GroupFeaturesByOU(
    std::list<std::tuple<execution::query_id_t, execution::pipeline_id_t,
                         std::vector<std::pair<ExecutionOperatingUnitType, uint64_t>>>> *pipeline_to_ou_position,
    const std::vector<execution::query_id_t> &pipeline_qids,
    const std::list<metrics::PipelineMetricRawData::PipelineData> &pipeline_data,
    std::unordered_map<ExecutionOperatingUnitType, std::vector<std::vector<double>>> *ou_to_features) {
  // if no pipeline data is recorded, there's no work to be done
  if (pipeline_data.empty()) return;

  // Otherwise, we look over all entries in pipeline_data
  // prev_qid and pipeline_idx is to keep mapping the current qid to the qid at pipeline_idx in pipeline_qids to
  // account for multiple executions of the same query
  auto pipeline_idx = 0;
  execution::query_id_t prev_qid = pipeline_data.begin()->query_id_;
  for (const auto &data_it : pipeline_data) {
    std::vector<std::pair<ExecutionOperatingUnitType, uint64_t>> ou_positions;
    // for each operating unit of the pipeline, we push it to the corresponding collection in ou_to_features for block
    // inference
    for (const auto &ou_it : data_it.features_) {
      // ou_to_positions will allow us to retrieve the inference results of all ous of a pipeline later
      if (ou_to_features->find(ou_it.GetExecutionOperatingUnitType()) == ou_to_features->end()) {
        ou_positions.emplace_back(ou_it.GetExecutionOperatingUnitType(), 0);
        ou_to_features->emplace(ou_it.GetExecutionOperatingUnitType(), std::vector<std::vector<double>>());
      } else {
        ou_positions.emplace_back(ou_it.GetExecutionOperatingUnitType(),
                                  ou_to_features->at(ou_it.GetExecutionOperatingUnitType()).size());
      }

      // To perform the inference, the predictors for each ou is structured according to the order in
      // https://github.com/cmu-db/noisepage/blob/master/script/model/type.py#L83-L92
      std::vector<double> predictors;
      predictors.push_back(metrics::MetricsUtil::GetHardwareContext().cpu_mhz_);
      predictors.push_back(data_it.execution_mode_);
      ou_it.GetAllAttributes(&predictors);
      ou_to_features->at(ou_it.GetExecutionOperatingUnitType()).push_back(predictors);
    }

    // If we observe a change in qid in the recorded pipeline data, we also advance to the next qid in pipeline_qids
    if (data_it.query_id_ != prev_qid) {
      pipeline_idx += 1;
      prev_qid = data_it.query_id_;
    }
    SELFDRIVING_LOG_TRACE(fmt::format("Fixed qid: {}; pipeline_id: {}",
                                      static_cast<uint>(pipeline_qids.at(pipeline_idx)),
                                      static_cast<uint32_t>(data_it.pipeline_id_)));
    pipeline_to_ou_position->emplace_back(pipeline_qids.at(pipeline_idx), data_it.pipeline_id_,
                                          std::move(ou_positions));
  }
}

}  // namespace noisepage::selfdriving
