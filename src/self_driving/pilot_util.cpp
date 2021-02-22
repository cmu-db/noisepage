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
#include "util/query_exec_util.h"

namespace noisepage::selfdriving {

void PilotUtil::ApplyAction(common::ManagedPointer<Pilot> pilot, const std::string &sql_query,
                            catalog::db_oid_t db_oid) {
  auto txn_manager = pilot->txn_manager_;
  auto catalog = pilot->catalog_;
  util::QueryExecUtil util(db_oid, txn_manager, catalog, pilot->settings_manager_, pilot->stats_storage_,
                           pilot->settings_manager_->GetInt(settings::Param::task_execution_timeout));
  util.BeginTransaction();
  util.SetCostModelFunction([]() { return std::make_unique<optimizer::TrivialCostModel>(); });

  bool is_query_set = false;
  {
    std::string query = sql_query;
    auto parse_tree = parser::PostgresParser::BuildParseTree(sql_query);
    auto statement = std::make_unique<network::Statement>(std::move(query), std::move(parse_tree));
    is_query_set = statement->GetQueryType() == network::QueryType::QUERY_SET;
  }

  if (is_query_set) {
    util.ExecuteDDL(sql_query);
  } else {
    // Parameters are also specified in the query string, hence we have no parameters nor parameter types here
    size_t idx = util.CompileQuery(sql_query, nullptr, nullptr);
    util.ExecuteQuery(idx, nullptr, nullptr, nullptr);
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  // Commit
  util.EndTransaction(true);
}

void PilotUtil::GetQueryPlans(common::ManagedPointer<Pilot> pilot, common::ManagedPointer<WorkloadForecast> forecast,
                              uint64_t end_segment_index, transaction::TransactionContext *txn,
                              std::vector<std::unique_ptr<planner::AbstractPlanNode>> *plan_vecs) {
  std::unordered_set<execution::query_id_t> qids;
  for (uint64_t idx = 0; idx <= end_segment_index; idx++) {
    for (auto &it : forecast->GetSegmentByIndex(idx).GetIdToNumexec()) {
      qids.insert(it.first);
    }
  }

  util::QueryExecUtil query_exec_util(catalog::INVALID_DATABASE_OID, pilot->txn_manager_, pilot->catalog_,
                                      pilot->settings_manager_, pilot->stats_storage_,
                                      pilot->settings_manager_->GetInt(settings::Param::task_execution_timeout));
  query_exec_util.UseTransaction(common::ManagedPointer(txn));
  query_exec_util.SetCostModelFunction([]() { return std::make_unique<optimizer::TrivialCostModel>(); });
  for (auto qid : qids) {
    auto query_text = forecast->GetQuerytextByQid(qid);
    auto db_oid = static_cast<catalog::db_oid_t>(forecast->GetDboidByQid(qid));
    query_exec_util.SetDatabase(db_oid);

    auto params = common::ManagedPointer(&(forecast->GetQueryparamsByQid(qid)->at(0)));
    auto param_types = common::ManagedPointer(forecast->GetParamtypesByQid(qid));
    auto result = query_exec_util.PlanStatement(query_text, params, param_types);
    plan_vecs->emplace_back(std::move(result.second));
  }
}

uint64_t PilotUtil::ComputeCost(common::ManagedPointer<Pilot> pilot, common::ManagedPointer<WorkloadForecast> forecast,
                                uint64_t start_segment_index, uint64_t end_segment_index) {
  // Compute cost as average latency of queries weighted by their num of exec
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
    auto pipeline_sum = 0;
    for (auto const &pipeline_res : pipeline_to_pred.second) {
      for (auto ou_res : pipeline_res) {
        // sum up the latency of ous
        pipeline_sum += ou_res[ou_res.size() - 1];
      }
    }
    // record average cost of this pipeline among the same queries with diff param
    if (prev_qid == pipeline_to_pred.first.first) {
      query_cost.back().second += static_cast<double>(pipeline_sum) / pipeline_to_pred.second.size();
    } else {
      query_cost.emplace_back(pipeline_to_pred.first.first, pipeline_sum / pipeline_to_pred.second.size());
      prev_qid = pipeline_to_pred.first.first;
    }
  }
  double total_cost = 0;
  uint128_t num_queries = 0;
  for (auto qcost : query_cost) {
    for (auto i = start_segment_index; i <= end_segment_index; i++) {
      total_cost += forecast->GetSegmentByIndex(i).GetIdToNumexec().at(qcost.first) * qcost.second;
      num_queries += forecast->GetSegmentByIndex(i).GetIdToNumexec().at(qcost.first);
    }
  }
  NOISEPAGE_ASSERT(num_queries > 0, "expect more then one query");
  return total_cost / num_queries;
}

const std::list<metrics::PipelineMetricRawData::PipelineData> &PilotUtil::CollectPipelineFeatures(
    common::ManagedPointer<selfdriving::Pilot> pilot, common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
    uint64_t start_segment_index, uint64_t end_segment_index, std::vector<execution::query_id_t> *pipeline_qids) {
  std::unordered_set<execution::query_id_t> qids;
  for (auto i = start_segment_index; i <= end_segment_index; i++) {
    for (auto &it : forecast->GetSegmentByIndex(i).GetIdToNumexec()) {
      qids.insert(it.first);
    }
  }

  util::QueryExecUtil query_exec_util(catalog::INVALID_DATABASE_OID, pilot->txn_manager_, pilot->catalog_,
                                      pilot->settings_manager_, pilot->stats_storage_,
                                      pilot->settings_manager_->GetInt(settings::Param::task_execution_timeout));
  query_exec_util.SetCostModelFunction([]() { return std::make_unique<optimizer::TrivialCostModel>(); });
  auto metrics_manager = pilot->metrics_thread_->GetMetricsManager();
  for (const auto &qid : qids) {
    // Begin transaction to get the executable query
    catalog::db_oid_t db_oid = static_cast<catalog::db_oid_t>(forecast->GetDboidByQid(qid));
    query_exec_util.BeginTransaction();
    query_exec_util.SetDatabase(db_oid);
    auto query_text = forecast->GetQuerytextByQid(qid);

    auto params = common::ManagedPointer(&(forecast->GetQueryparamsByQid(qid)->at(0)));
    auto param_types = common::ManagedPointer(forecast->GetParamtypesByQid(qid));
    size_t idx = query_exec_util.CompileQuery(query_text, params, param_types);
    pipeline_qids->push_back(qid);

    for (auto &params : *(forecast->GetQueryparamsByQid(qid))) {
      query_exec_util.ExecuteQuery(idx, nullptr, common::ManagedPointer(&params), metrics_manager);
    }

    query_exec_util.EndTransaction(false);
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
  std::string project_build_path = getenv(Pilot::BUILD_ABS_PATH);
  std::unordered_map<ExecutionOperatingUnitType, std::vector<std::vector<double>>> inference_result;
  for (auto &ou_map_it : ou_to_features) {
    auto res = model_server_manager->InferMiniRunnerModel(
        selfdriving::OperatingUnitUtil::ExecutionOperatingUnitTypeToString(ou_map_it.first),
        project_build_path + model_save_path, ou_map_it.second);
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
