#include "self_driving/planning/pilot_util.h"

#include "binder/bind_node_visitor.h"
#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "common/managed_pointer.h"
#include "execution/compiler/compilation_context.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/execution_settings.h"
#include "execution/exec_defs.h"
#include "execution/sql/ddl_executors.h"
#include "loggers/selfdriving_logger.h"
#include "messenger/messenger.h"
#include "metrics/metrics_thread.h"
#include "metrics/metrics_util.h"
#include "network/network_util.h"
#include "network/postgres/statement.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "parser/delete_statement.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/insert_statement.h"
#include "parser/postgresparser.h"
#include "parser/variable_set_statement.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "self_driving/forecasting/workload_forecast.h"
#include "self_driving/model_server/model_server_manager.h"
#include "self_driving/modeling/operating_unit.h"
#include "self_driving/planning/action/create_index_action.h"
#include "self_driving/planning/action/drop_index_action.h"
#include "self_driving/planning/mcts/action_state.h"
#include "self_driving/planning/pilot.h"
#include "self_driving/planning/planning_context.h"
#include "settings/settings_manager.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"
#include "task/task.h"
#include "task/task_manager.h"
#include "util/query_exec_util.h"

namespace noisepage::selfdriving::pilot {

void PilotUtil::ApplyAction(const pilot::PlanningContext &planning_context, const std::string &sql_query,
                            catalog::db_oid_t db_oid, bool what_if) {
  SELFDRIVING_LOG_INFO("Applying action: {}", sql_query);

  // Certain actions (currently ChangeKnobAction) is not associated with a particular database. For those actions we
  // just pick a random database in PlanningContext
  if (db_oid == catalog::INVALID_DATABASE_OID) db_oid = *planning_context.GetDBOids().begin();

  auto &query_exec_util = planning_context.GetQueryExecUtil();
  if (what_if)
    query_exec_util->UseTransaction(db_oid, planning_context.GetTxnContext(db_oid));
  else
    query_exec_util->BeginTransaction(db_oid);

  bool is_query_ddl;
  {
    std::string query = sql_query;
    auto parse_tree = parser::PostgresParser::BuildParseTree(sql_query);
    auto statement = std::make_unique<network::Statement>(std::move(query), std::move(parse_tree));
    is_query_ddl = network::NetworkUtil::DDLQueryType(statement->GetQueryType()) ||
                   statement->GetQueryType() == network::QueryType::QUERY_SET;
  }

  if (is_query_ddl) {
    query_exec_util->ExecuteDDL(sql_query, what_if);
  } else {
    // Parameters are also specified in the query string, hence we have no parameters nor parameter types here
    execution::exec::ExecutionSettings settings{};
    if (query_exec_util->CompileQuery(sql_query, nullptr, nullptr, std::make_unique<optimizer::TrivialCostModel>(),
                                      std::nullopt, settings)) {
      query_exec_util->ExecuteQuery(sql_query, nullptr, nullptr, nullptr, settings);
    }
  }

  query_exec_util->ClearPlan(sql_query);
  if (what_if)
    query_exec_util->UseTransaction(db_oid, nullptr);
  else
    query_exec_util->EndTransaction(true);
}

void PilotUtil::GetQueryPlans(const PlanningContext &planning_context,
                              common::ManagedPointer<WorkloadForecast> forecast, uint64_t end_segment_index,
                              std::vector<std::unique_ptr<planner::AbstractPlanNode>> *plan_vecs) {
  std::unordered_set<execution::query_id_t> qids;
  for (uint64_t idx = 0; idx <= end_segment_index; idx++) {
    for (auto &it : forecast->GetSegmentByIndex(idx).GetIdToNumexec()) {
      qids.insert(it.first);
    }
  }

  // Use QueryExecUtil since we want the abstract plan nodes.
  // We don't care about compilation right now.
  auto &query_exec_util = planning_context.GetQueryExecUtil();
  for (auto qid : qids) {
    auto query_text = forecast->GetQuerytextByQid(qid);
    auto db_oid = forecast->GetDboidByQid(qid);
    query_exec_util->UseTransaction(db_oid, planning_context.GetTxnContext(db_oid));

    auto params = common::ManagedPointer(&(forecast->GetQueryparamsByQid(qid)->at(0)));
    auto param_types = common::ManagedPointer(forecast->GetParamtypesByQid(qid));
    auto result = query_exec_util->PlanStatement(query_text, params, param_types,
                                                 std::make_unique<optimizer::TrivialCostModel>());
    plan_vecs->emplace_back(std::move(result->OptimizeResult()->TakePlanNodeOwnership()));
    query_exec_util->ClearPlan(query_text);
    query_exec_util->UseTransaction(db_oid, nullptr);
  }
}

void PilotUtil::SumFeatureInPlace(std::vector<double> *feature, const std::vector<double> &delta_feature,
                                  double normalization) {
  for (size_t i = 0; i < feature->size(); i++) {
    (*feature)[i] += delta_feature[i] / normalization;
  }
}

std::vector<double> PilotUtil::GetInterferenceFeature(const std::vector<double> &feature,
                                                      const std::vector<double> &normalized_feat_sum) {
  std::vector<double> interference_feat;
  for (size_t i = 0; i < feature.size(); i++) {
    // normalize the output of ou_model by the elapsed time
    interference_feat.emplace_back(feature[i] / (feature[feature.size() - 1] + 1e-2));
  }

  // append with the normalized feature for this segment
  interference_feat.insert(interference_feat.end(), normalized_feat_sum.begin(), normalized_feat_sum.end());
  NOISEPAGE_ASSERT(interference_feat.size() == 18, "expect 18 nonzero elements in interference feature");
  interference_feat.resize(INTERFERENCE_DIMENSION, 0.0);
  return interference_feat;
}

double PilotUtil::ComputeCost(const PlanningContext &planning_context,
                              common::ManagedPointer<WorkloadForecast> forecast, uint64_t start_segment_index,
                              uint64_t end_segment_index) {
  // Compute cost as total latency of queries based on their num of exec
  // query id, <num_param of this query executed, total number of collected ous for this query>
  std::map<execution::query_id_t, std::pair<uint8_t, uint64_t>> query_info;
  // This is to record the start index of ou records belonging to a segment in input to the interference model
  std::map<uint32_t, uint64_t> segment_to_offset;
  std::vector<std::vector<double>> interference_result_matrix;

  PilotUtil::ExecuteForecast(planning_context, forecast, start_segment_index, end_segment_index, &query_info,
                             &segment_to_offset, &interference_result_matrix);

  double total_cost = 0.0;

  for (auto seg_idx = start_segment_index; seg_idx <= end_segment_index; seg_idx++) {
    std::vector<std::pair<execution::query_id_t, double>> query_cost;

    auto query_ou_offset = segment_to_offset[seg_idx];

    // separately get the cost of each query, averaged over diff set of params, for this segment
    // iterate through the sorted list of qids for this segment
    for (auto id_to_num_exec : forecast->GetSegmentByIndex(seg_idx).GetIdToNumexec()) {
      double curr_query_cost = 0.0;
      for (auto ou_idx = query_ou_offset; ou_idx < query_ou_offset + query_info[id_to_num_exec.first].second;
           ou_idx++) {
        curr_query_cost +=
            interference_result_matrix.at(ou_idx).back() / static_cast<double>(query_info[id_to_num_exec.first].first);
      }
      total_cost += static_cast<double>(id_to_num_exec.second) * curr_query_cost;
      query_ou_offset += query_info[id_to_num_exec.first].second;
    }
  }

  return total_cost;
}

std::unique_ptr<metrics::PipelineMetricRawData> PilotUtil::CollectPipelineFeatures(
    const PlanningContext &planning_context, common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
    uint64_t start_segment_index, uint64_t end_segment_index, std::vector<execution::query_id_t> *pipeline_qids,
    const bool execute_query) {
  std::unordered_set<execution::query_id_t> qids;
  for (auto i = start_segment_index; i <= end_segment_index; i++) {
    for (auto &it : forecast->GetSegmentByIndex(i).GetIdToNumexec()) {
      qids.insert(it.first);
    }
  }

  auto metrics_manager = planning_context.GetMetricsThread()->GetMetricsManager();
  std::unique_ptr<metrics::PipelineMetricRawData> aggregated_data = nullptr;
  bool old_metrics_enable = false;
  uint8_t old_sample_rate = 0;
  if (!execute_query) {
    aggregated_data = std::make_unique<metrics::PipelineMetricRawData>();
  } else {
    // record previous parameters to be restored at the end of this function
    auto settings_manager = planning_context.GetSettingsManager();
    old_metrics_enable = settings_manager->GetBool(settings::Param::pipeline_metrics_enable);
    old_sample_rate = settings_manager->GetInt64(settings::Param::pipeline_metrics_sample_rate);
    if (!old_metrics_enable) metrics_manager->EnableMetric(metrics::MetricsComponent::EXECUTION_PIPELINE);
    metrics_manager->SetMetricSampleRate(metrics::MetricsComponent::EXECUTION_PIPELINE, 100);
  }

  for (const auto &qid : qids) {
    catalog::db_oid_t db_oid = forecast->GetDboidByQid(qid);
    auto query_text = forecast->GetQuerytextByQid(qid);

    std::vector<type::TypeId> *param_types = forecast->GetParamtypesByQid(qid);
    std::vector<std::vector<parser::ConstantValueExpression>> *params = forecast->GetQueryparamsByQid(qid);
    pipeline_qids->push_back(qid);

    if (execute_query) {
      // Execute the queries to get features with counters
      common::Future<task::DummyResult> sync;
      execution::exec::ExecutionSettings settings{};
      settings.is_counters_enabled_ = true;
      settings.is_pipeline_metrics_enabled_ = true;
      // Forcefully reoptimize all the queries and set the query identifier to use
      // If copying params and param_types gets expensive, we might have to tweak the Task::TaskDML
      // constructor to allow specifying pointer params or a custom planning task.
      // FIXME(lin): if we want to revive this code path, we need to pass in the transaction context from
      //  PlanningContext to get the latest modifications (e.g. index changes). We punt on this since we focus on
      //  using the stats to get the features instead of executing queries. We might eventually delete this code path
      //  as well.
      planning_context.GetTaskManager()->AddTask(std::make_unique<task::TaskDML>(
          db_oid, query_text, std::make_unique<optimizer::TrivialCostModel>(),
          std::vector<std::vector<parser::ConstantValueExpression>>(*params), std::vector<type::TypeId>(*param_types),
          nullptr, metrics_manager, settings, true, true, std::make_optional<execution::query_id_t>(qid),
          common::ManagedPointer(&sync)));
      auto future_result = sync.WaitFor(Pilot::FUTURE_TIMEOUT);
      if (!future_result.has_value()) {
        throw PILOT_EXCEPTION("Future timed out.", common::ErrorCode::ERRCODE_IO_ERROR);
      }
    } else {
      // Just compile the queries (generate the bytecodes) to get features with statistics
      auto &query_util = planning_context.GetQueryExecUtil();
      query_util->UseTransaction(db_oid, planning_context.GetTxnContext(db_oid));
      // TODO(lin): Do we need to pass in any settings?
      execution::exec::ExecutionSettings settings{};
      for (auto &param : *params) {
        query_util->CompileQuery(query_text, common::ManagedPointer(&param), common::ManagedPointer(param_types),
                                 std::make_unique<optimizer::TrivialCostModel>(), std::nullopt, settings);
        auto executable_query = query_util->GetExecutableQuery(query_text);

        auto ous = executable_query->GetPipelineOperatingUnits();

        // used just a placeholder
        const auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();

        for (auto &iter : ous->GetPipelineFeatureMap()) {
          // TODO(lin): Get the execution mode from settings manager when we can support changing it...
          aggregated_data->RecordPipelineData(
              qid, iter.first, 0, std::vector<ExecutionOperatingUnitFeature>(iter.second), resource_metrics);
        }
        query_util->ClearPlan(query_text);
      }

      query_util->UseTransaction(db_oid, nullptr);
    }
  }

  if (execute_query) {
    // retrieve the features
    metrics_manager->Aggregate();

    aggregated_data.reset(reinterpret_cast<metrics::PipelineMetricRawData *>(
        metrics_manager->AggregatedMetrics()
            .at(static_cast<uint8_t>(metrics::MetricsComponent::EXECUTION_PIPELINE))
            .release()));

    // restore the old parameters
    metrics_manager->SetMetricSampleRate(metrics::MetricsComponent::EXECUTION_PIPELINE, old_sample_rate);
    if (!old_metrics_enable) metrics_manager->DisableMetric(metrics::MetricsComponent::EXECUTION_PIPELINE);
  }
  SELFDRIVING_LOG_DEBUG("Printing qid and pipeline id to sanity check pipeline metrics recorded");
  for (auto it = aggregated_data->pipeline_data_.begin(); it != aggregated_data->pipeline_data_.end(); it++) {
    SELFDRIVING_LOG_DEBUG(fmt::format("qid: {}; pipeline_id: {}", static_cast<uint>(it->query_id_),
                                      static_cast<uint32_t>(it->pipeline_id_)));
  }

  return aggregated_data;
}

void PilotUtil::OUModelInference(const std::string &model_save_path,
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
    auto res = model_server_manager->InferOUModel(
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

void PilotUtil::InterferenceModelInference(
    const std::string &interference_model_save_path,
    common::ManagedPointer<modelserver::ModelServerManager> model_server_manager,
    const std::map<std::pair<execution::query_id_t, execution::pipeline_id_t>,
                   std::vector<std::vector<std::vector<double>>>> &pipeline_to_prediction,
    common::ManagedPointer<selfdriving::WorkloadForecast> forecast, uint64_t start_segment_index,
    uint64_t end_segment_index, std::map<execution::query_id_t, std::pair<uint8_t, uint64_t>> *query_info,
    std::map<uint32_t, uint64_t> *segment_to_offset, std::vector<std::vector<double>> *interference_result_matrix) {
  std::vector<std::pair<execution::query_id_t, std::vector<double>>> query_feat_sum;
  execution::query_id_t curr_qid = execution::INVALID_QUERY_ID;

  auto feat_dim = pipeline_to_prediction.begin()->second.back().back().size();

  // Compute the sum of ous for a query, averaged over diff set of params
  for (auto const &pipeline_to_pred : pipeline_to_prediction) {
    std::vector<double> pipeline_sum(feat_dim, 0.0);
    auto num_ou_for_ppl = 0;

    if (curr_qid != pipeline_to_pred.first.first) {
      curr_qid = pipeline_to_pred.first.first;
      query_feat_sum.emplace_back(curr_qid, std::vector<double>(feat_dim, 0.0));
      query_info->emplace(curr_qid, std::make_pair(pipeline_to_pred.second.size(), 0));
    }

    for (auto const &pipeline_res : pipeline_to_pred.second) {
      for (const auto &ou_res : pipeline_res) {
        // sum up the ou prediction results of all ous in a pipeline
        SumFeatureInPlace(&pipeline_sum, ou_res, 1);
      }
      num_ou_for_ppl += pipeline_res.size();
    }
    // record average feat sum of this pipeline among the same queries with diff param
    SumFeatureInPlace(&query_feat_sum.back().second, pipeline_sum, pipeline_to_pred.second.size());
    query_info->at(curr_qid).second += num_ou_for_ppl;
  }

  // Populate interference_features matrix:
  // Compute sum of all ous in a segment, normalized by its interval
  std::vector<std::vector<double>> interference_features;

  for (auto i = start_segment_index; i <= end_segment_index; i++) {
    std::vector<double> normalized_feat_sum(feat_dim, 0.0);
    auto id_to_num_exec = forecast->GetSegmentByIndex(i).GetIdToNumexec();

    segment_to_offset->emplace(i, interference_features.size());

    for (auto const &id_to_query_sum : query_feat_sum) {
      if (id_to_num_exec.find(id_to_query_sum.first) != id_to_num_exec.end()) {
        // account for number of exec of this query
        // and normalize the ou_sum in an interval by the length of this interval
        SumFeatureInPlace(&normalized_feat_sum, id_to_query_sum.second,
                          forecast->GetForecastInterval() / id_to_num_exec[id_to_query_sum.first]);
      }
    }
    // curr_feat_sum now holds the sum of ous for queries contained in this segment (averaged over diff set of param)
    // multiplied by number of execution of the query containing it and normalized by its interval
    for (auto const &pipeline_to_pred : pipeline_to_prediction) {
      if (id_to_num_exec.find(pipeline_to_pred.first.first) == id_to_num_exec.end()) {
        continue;
      }
      for (auto const &pipeline_res : pipeline_to_pred.second) {
        for (const auto &ou_res : pipeline_res) {
          interference_features.emplace_back(GetInterferenceFeature(ou_res, normalized_feat_sum));
        }
      }
    }
  }

  auto interference_result =
      model_server_manager->InferInterferenceModel(interference_model_save_path, interference_features);
  NOISEPAGE_ASSERT(interference_result.second, "Inference through interference model has error");
  *interference_result_matrix = interference_result.first;
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

MemoryInfo PilotUtil::ComputeMemoryInfo(const PlanningContext &planning_context, const WorkloadForecast *forecast) {
  MemoryInfo memory_info;
  PilotUtil::ComputeTableSizeRatios(planning_context, forecast, &memory_info);
  PilotUtil::ComputeTableIndexSizes(planning_context, &memory_info);
  return memory_info;
}

void PilotUtil::ComputeTableSizeRatios(const PlanningContext &planning_context, const WorkloadForecast *forecast,
                                       MemoryInfo *memory_info) {
  auto task_manager = planning_context.GetTaskManager();
  NOISEPAGE_ASSERT(task_manager, "ComputeTableSizeRatios() requires task manager");

  // Maps from db-table oid to the number of rows (acquired from pg_statistic)
  std::unordered_map<db_table_oid_pair, uint64_t, DBTableOidPairHasher> table_sizes;

  // Get <table_id, num_rows> pairs with num_rows > 0
  auto query = fmt::format(
      "select starelid, max(stanumrows) as num_rows from pg_statistic group by starelid "
      "having max(stanumrows) > 0;");

  // Get stats from all databases in the forecasted workload
  for (auto db_oid : planning_context.GetDBOids()) {
    auto to_row_fn = [&table_sizes, db_oid, memory_info](const std::vector<execution::sql::Val *> &values) {
      auto table_oid = static_cast<execution::sql::Integer *>(values[0])->val_;
      auto num_rows = static_cast<execution::sql::Integer *>(values[1])->val_;

      table_sizes[std::make_pair(db_oid, catalog::table_oid_t(table_oid))] = num_rows;
      // Currently only track tables with more than 0 rows in the stats
      memory_info->table_oids_.emplace_back(std::make_pair(db_oid, table_oid));
    };

    common::Future<task::DummyResult> sync;
    task_manager->AddTask(std::make_unique<task::TaskDML>(db_oid, query,
                                                          std::make_unique<optimizer::TrivialCostModel>(), false,
                                                          to_row_fn, common::ManagedPointer(&sync)));

    auto future_result = sync.WaitFor(Pilot::FUTURE_TIMEOUT);
    if (!future_result.has_value()) {
      throw PILOT_EXCEPTION("Future timed out.", common::ErrorCode::ERRCODE_IO_ERROR);
    }
    UNUSED_ATTRIBUTE bool success = future_result->second;
    NOISEPAGE_ASSERT(success, "Failed reading from pg_statistic");
  }

  // Calculating queries' changes in the number of rows
  auto &metadata = forecast->GetWorkloadMetadata();
  // <query_id, <table_id, number of rows changed by that query in that table>>
  std::unordered_map<execution::query_id_t, std::pair<db_table_oid_pair, int64_t>> query_row_changes;
  for (const auto &[query_id, query_text] : metadata.query_id_to_text_) {
    // Parse the query
    auto parse_result = parser::PostgresParser::BuildParseTree(query_text);
    if (parse_result->NumStatements() == 0) continue;
    auto statement = parse_result->GetStatement(0);

    // Identify the table and the number of inserts/deletes
    int64_t num_row_delta = 0;
    catalog::table_oid_t table_oid = catalog::INVALID_TABLE_OID;
    catalog::db_oid_t db_oid = metadata.query_id_to_dboid_.at(query_id);
    auto accessor = planning_context.GetCatalogAccessor(db_oid);
    if (statement->GetType() == parser::StatementType::INSERT) {
      auto insert_statement = reinterpret_cast<parser::InsertStatement *>(statement.Get());
      table_oid = accessor->GetTableOid(insert_statement->GetInsertionTable()->GetTableName());
      // Number of values is the number of rows inserted
      num_row_delta = insert_statement->GetValues()->size();
    }

    if (statement->GetType() == parser::StatementType::DELETE) {
      auto delete_statement = reinterpret_cast<parser::DeleteStatement *>(statement.Get());
      table_oid = accessor->GetTableOid(delete_statement->GetDeletionTable()->GetTableName());
      if (delete_statement->GetDeleteCondition() != nullptr) {
        // TODO(lin): right now assuming only deleting one row when there's a condition. Need cardinality estimation
        //  to get a more accurate estimation
        num_row_delta = -1;
      } else {
        auto oid_pair = std::make_pair(db_oid, table_oid);
        if (table_sizes.find(oid_pair) != table_sizes.end())
          // Delete all the tuples when without condition
          num_row_delta = -table_sizes[oid_pair];
      }
    }
    auto oid_pair = std::make_pair(db_oid, table_oid);
    if (num_row_delta != 0 && table_sizes.find(oid_pair) != table_sizes.end())
      query_row_changes[query_id] = std::make_pair(oid_pair, num_row_delta);
  }

  // Compute table size ratios given the workload forecast
  for (uint64_t idx = 0; idx < forecast->GetNumberOfSegments(); ++idx) {
    // Get the table num row changes in this segment
    auto &id_to_num_exec = forecast->GetSegmentByIndex(idx).GetIdToNumexec();
    std::unordered_map<db_table_oid_pair, double, DBTableOidPairHasher> table_size_deltas;
    for (const auto &[query_id, table_id_to_delta] : query_row_changes) {
      auto table_id = table_id_to_delta.first;
      if (table_size_deltas.find(table_id) == table_size_deltas.end()) table_size_deltas[table_id] = 0;
      table_size_deltas[table_id] += table_id_to_delta.second * id_to_num_exec.at(query_id);
    }

    // Calculate the table size change ratio for this segment
    for (const auto &[table_id, size_delta] : table_size_deltas) {
      double new_table_size = size_delta + static_cast<int64_t>(table_sizes[table_id]);
      if (new_table_size < 0)
        memory_info->segment_table_size_ratios_[idx][table_id] = 0;
      else
        memory_info->segment_table_size_ratios_[idx][table_id] = new_table_size / table_sizes[table_id];
    }
  }
}

void PilotUtil::ComputeTableIndexSizes(const PlanningContext &planning_context, MemoryInfo *memory_info) {
  // Traverse all databases to find the info (since we don't know which table belongs to which database)
  for (auto db_oid : planning_context.GetDBOids()) {
    auto accessor = planning_context.GetCatalogAccessor(db_oid);
    if (accessor == nullptr) continue;
    for (auto table_oid : memory_info->table_oids_) {
      // Get table memory size
      auto sql_table = accessor->GetTable(table_oid.second);
      if (sql_table == nullptr) continue;
      size_t table_memory = sql_table->EstimateHeapUsage();
      memory_info->table_memory_bytes_[table_oid] = table_memory;
      memory_info->initial_memory_bytes_ += table_memory;

      // Get index memory size
      auto index_oids = accessor->GetIndexOids(table_oid.second);
      for (auto index_oid : index_oids) {
        auto index = accessor->GetIndex(index_oid);
        std::string index_name(accessor->GetIndexName(index_oid));
        size_t index_memory = index->EstimateHeapUsage();
        memory_info->table_index_memory_bytes_[table_oid][index_name] = index_memory;
        memory_info->initial_memory_bytes_ += index_memory;
      }
    }
  }
}

void PilotUtil::EstimateCreateIndexAction(const PlanningContext &planning_context, CreateIndexAction *create_action,
                                          DropIndexAction *drop_action) {
  // Just compile the queries (generate the bytecodes) to get features with statistics
  std::string query_text = create_action->GetSQLCommand();

  auto &query_util = planning_context.GetQueryExecUtil();
  auto db_oid = create_action->GetDatabaseOid();
  query_util->UseTransaction(db_oid, planning_context.GetTxnContext(db_oid));

  // First need to insert the index entry into the catalog so that we can correclty generate the query plan
  query_util->ExecuteDDL(query_text, true);

  auto executable_query = query_util->GetExecutableQuery(query_text);

  auto ous = executable_query->GetPipelineOperatingUnits();

  // used just a placeholder
  const auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();

  // query id shouldn't matter
  execution::query_id_t qid(1);
  std::vector<execution::query_id_t> pipeline_qids{qid};
  std::unique_ptr<metrics::PipelineMetricRawData> aggregated_data = std::make_unique<metrics::PipelineMetricRawData>();
  for (auto &iter : ous->GetPipelineFeatureMap()) {
    // TODO(lin): Interpret mode by default. May want to add that as an option (knob) for the action
    aggregated_data->RecordPipelineData(qid, iter.first, 0, std::vector<ExecutionOperatingUnitFeature>(iter.second),
                                        resource_metrics);
  }
  query_util->ClearPlan(query_text);

  // Execute drop index to reverse catalog changes
  query_text = drop_action->GetSQLCommand();
  query_util->ExecuteDDL(query_text, true);
  query_util->ClearPlan(query_text);

  query_util->UseTransaction(db_oid, nullptr);

  // pipeline_to_prediction maps each pipeline to a vector of ou inference results for all ous of this pipeline
  // (where each entry corresponds to a different query param)
  // Each element of the outermost vector is a vector of ou prediction (each being a double vector) for one set of
  // parameters
  std::map<std::pair<execution::query_id_t, execution::pipeline_id_t>, std::vector<std::vector<std::vector<double>>>>
      pipeline_to_prediction;

  // Then we perform inference through model server to get ou prediction results for all pipelines
  PilotUtil::OUModelInference(planning_context.GetOuModelSavePath(), planning_context.GetModelServerManager(),
                              pipeline_qids, aggregated_data->pipeline_data_, &pipeline_to_prediction);

  auto pred_dim = pipeline_to_prediction.begin()->second.back().back().size();

  // Compute the sum of all ous
  std::vector<double> pipeline_sum(pred_dim, 0.0);
  for (auto const &pipeline_to_pred : pipeline_to_prediction) {
    for (auto const &pipeline_res : pipeline_to_pred.second) {
      for (const auto &ou_res : pipeline_res) {
        // sum up the ou prediction results of all ous in a pipeline
        SumFeatureInPlace(&pipeline_sum, ou_res, 1);
      }
    }
  }

  create_action->SetEstimatedMetrics(std::move(pipeline_sum));
}

size_t PilotUtil::CalculateMemoryConsumption(const MemoryInfo &memory_info, const ActionState &action_state,
                                             uint64_t segment_index,
                                             const std::map<action_id_t, std::unique_ptr<AbstractAction>> &action_map) {
  auto &created_indexes = action_state.GetCreatedIndexes();
  auto &dropped_indexes = action_state.GetDroppedIndexes();
  auto &index_action_map = action_state.GetIndexActionMap();

  // First count the table sizes
  std::unordered_map<db_table_oid_pair, double, DBTableOidPairHasher> table_memory_sizes;
  for (auto &[table_id, size] : memory_info.table_memory_bytes_) table_memory_sizes[table_id] = size;

  // Then count the index sizes for each table, if they're not dropped
  for (auto &[table_id, index_sizes] : memory_info.table_index_memory_bytes_) {
    for (auto &[index_name, size] : index_sizes) {
      if (dropped_indexes.find(index_name) != dropped_indexes.end()) table_memory_sizes[table_id] += size;
    }
  }

  // Then count the newly added indexes
  for (auto &name : created_indexes) {
    auto const &action = action_map.at(index_action_map.at(name));
    double size = action->GetEstimatedMemoryBytes();
    NOISEPAGE_ASSERT(action->GetActionType() == ActionType::CREATE_INDEX, "This must be a create index action");
    auto db_id = action->GetDatabaseOid();
    auto table_id = reinterpret_cast<CreateIndexAction *>(action.get())->GetTableOid();
    table_memory_sizes[std::make_pair(db_id, table_id)] += size;
  }

  double total_memory = 0;
  auto &table_size_ratios = memory_info.segment_table_size_ratios_.at(segment_index);
  // Adjust the sizes based on the forecasted table size ratio and calculate the sum
  for (auto [table_id, size] : table_memory_sizes) {
    // We won't store the size ratio if the table size doesn't change
    if (table_size_ratios.find(table_id) == table_size_ratios.end())
      total_memory += size;
    else
      total_memory += size * table_size_ratios.at(table_id);
  }

  return total_memory;
}

std::string PilotUtil::ConfigToString(const std::set<action_id_t> &config_set) {
  std::string set_string = "{";
  for (auto action : config_set) {
    set_string += fmt::format(" action_id {} ", action);
  }
  set_string += "}";
  return set_string;
}

void PilotUtil::ExecuteForecast(const PlanningContext &planning_context,
                                common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
                                uint64_t start_segment_index, uint64_t end_segment_index,
                                std::map<execution::query_id_t, std::pair<uint8_t, uint64_t>> *query_info,
                                std::map<uint32_t, uint64_t> *segment_to_offset,
                                std::vector<std::vector<double>> *interference_result_matrix) {
  NOISEPAGE_ASSERT(forecast != nullptr, "Need forecast_ initialized.");
  // first we make sure the pipeline metrics flag as well as the counters is enabled. Also set the sample rate to be 0
  // so that every query execution is being recorded

  std::vector<execution::query_id_t> pipeline_qids;
  // Collect pipeline metrics of forecasted queries within the interval of segments
  auto pipeline_data =
      PilotUtil::CollectPipelineFeatures(planning_context, common::ManagedPointer(forecast), start_segment_index,
                                         end_segment_index, &pipeline_qids, !Pilot::WHAT_IF);

  // pipeline_to_prediction maps each pipeline to a vector of ou inference results for all ous of this pipeline
  // (where each entry corresponds to a different query param)
  // Each element of the outermost vector is a vector of ou prediction (each being a double vector) for one set of
  // parameters
  std::map<std::pair<execution::query_id_t, execution::pipeline_id_t>, std::vector<std::vector<std::vector<double>>>>
      pipeline_to_prediction;

  // Then we perform inference through model server to get ou prediction results for all pipelines
  PilotUtil::OUModelInference(planning_context.GetOuModelSavePath(), planning_context.GetModelServerManager(),
                              pipeline_qids, pipeline_data->pipeline_data_, &pipeline_to_prediction);

  PilotUtil::InterferenceModelInference(planning_context.GetInterferenceModelSavePath(),
                                        planning_context.GetModelServerManager(), pipeline_to_prediction,
                                        common::ManagedPointer(forecast), start_segment_index, end_segment_index,
                                        query_info, segment_to_offset, interference_result_matrix);
}

}  // namespace noisepage::selfdriving::pilot
