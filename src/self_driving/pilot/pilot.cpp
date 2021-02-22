#include "self_driving/pilot/pilot.h"

#include <cstdio>
#include <memory>
#include <utility>

#include "common/action_context.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/executable_query.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/execution_settings.h"
#include "execution/exec/output.h"
#include "execution/exec_defs.h"
#include "execution/vm/vm_defs.h"
#include "loggers/selfdriving_logger.h"
#include "messenger/messenger.h"
#include "metrics/metrics_thread.h"
#include "network/postgres/statement.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/statistics/stats_storage.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/output_schema.h"
#include "self_driving/forecast/workload_forecast.h"
#include "self_driving/model_server/model_server_manager.h"
#include "self_driving/pilot/mcts/monte_carlo_tree_search.h"
#include "self_driving/pilot_util.h"
#include "settings/settings_manager.h"
#include "transaction/transaction_manager.h"
#include "util/query_exec_util.h"

namespace noisepage::selfdriving {

uint64_t Pilot::planning_iteration_ = 0;

Pilot::Pilot(std::string model_save_path, std::string forecast_model_save_path,
             common::ManagedPointer<catalog::Catalog> catalog,
             common::ManagedPointer<metrics::MetricsThread> metrics_thread,
             common::ManagedPointer<modelserver::ModelServerManager> model_server_manager,
             common::ManagedPointer<settings::SettingsManager> settings_manager,
             common::ManagedPointer<optimizer::StatsStorage> stats_storage,
             common::ManagedPointer<transaction::TransactionManager> txn_manager, uint64_t workload_forecast_interval)
    : model_save_path_(std::move(model_save_path)),
      forecast_model_save_path_(std::move(forecast_model_save_path)),
      catalog_(catalog),
      metrics_thread_(metrics_thread),
      model_server_manager_(model_server_manager),
      settings_manager_(settings_manager),
      stats_storage_(stats_storage),
      txn_manager_(txn_manager),
      workload_forecast_interval_(workload_forecast_interval) {
  forecast_ = nullptr;

  auto *txn = txn_manager_->BeginTransaction();
  auto db_oid = catalog_->GetDatabaseOid(common::ManagedPointer(txn), catalog::DEFAULT_DATABASE);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  query_exec_util_ =
      std::make_unique<util::QueryExecUtil>(db_oid, txn_manager_, catalog_, settings_manager_, stats_storage_,
                                            settings_manager_->GetInt(settings::Param::task_execution_timeout));

  while (!model_server_manager_->ModelServerStarted()) {
  }
}

void Pilot::PerformForecasterTrain() {
  std::vector<std::string> models{"LSTM"};
  std::string input_path{metrics::QueryTraceMetricRawData::FILES[1]};
  modelserver::ModelServerFuture<std::string> future;
  model_server_manager_->TrainForecastModel(models, input_path, forecast_model_save_path_, workload_forecast_interval_,
                                            common::ManagedPointer(&future));
  future.Wait();
}

std::pair<WorkloadMetadata, bool> Pilot::RetrieveWorkloadMetadata(uint64_t iteration) {
  WorkloadMetadata metadata;
  auto to_row_fn = [&metadata](const std::vector<execution::sql::Val *> &values) {
    auto db_oid = static_cast<execution::sql::Integer *>(values[1])->val_;
    auto qid = execution::query_id_t(static_cast<execution::sql::Integer *>(values[2])->val_);
    metadata.query_id_to_dboid_[qid] = db_oid;

    execution::sql::StringVal *text_val = static_cast<execution::sql::StringVal *>(values[3]);
    // We do this since the string has been quoted by the metric
    metadata.query_id_to_text_[qid] = std::string(text_val->StringView().data() + 1, text_val->StringView().size() - 2);

    execution::sql::StringVal *param_types = static_cast<execution::sql::StringVal *>(values[4]);
    std::vector<type::TypeId> types;
    {
      auto json_decomp = nlohmann::json::parse(param_types->StringView().data(),
                                               param_types->StringView().data() + param_types->StringView().size());
      for (auto &elem : json_decomp) {
        types.push_back(type::TypeUtil::TypeIdFromString(elem));
      }
      metadata.query_id_to_param_types_[qid] = types;
    }

    execution::sql::StringVal *param_val = static_cast<execution::sql::StringVal *>(values[5]);
    {
      std::vector<parser::ConstantValueExpression> cves;
      auto json_decomp = nlohmann::json::parse(param_val->StringView().data(),
                                               param_val->StringView().data() + param_val->StringView().size());
      for (size_t i = 0; i < json_decomp.size(); i++) {
        cves.emplace_back(parser::ConstantValueExpression::FromString(json_decomp[i], types[i]));
      }
      metadata.query_id_to_params_[qid].emplace_back(std::move(cves));
    }
  };

  auto query = fmt::format("SELECT * FROM noisepage_forecast_parameters WHERE iteration = {}", iteration);
  query_exec_util_->BeginTransaction();
  bool result = query_exec_util_->ExecuteDML(query, nullptr, nullptr, to_row_fn, nullptr);
  query_exec_util_->EndTransaction(result);
  return std::make_pair(std::move(metadata), result);
}

void Pilot::RecordWorkloadForecastPrediction(uint64_t iteration,
                                             const selfdriving::WorkloadForecastPrediction &prediction,
                                             const WorkloadMetadata &metadata) {
  // CREATE TABLE noisepage_forecast_clusters(iteration INT, cluster_id INT, query_id INT, db_id INT);
  // CREATE TABLE noisepage_forecast_forecasts(iteration INT, query_id INT, interval INT, rate REAL);
  std::string statements[2] = {"INSERT INTO noisepage_forecast_clusters (?, ?, ?, ?)",
                               "INSERT INTO noisepage_forecast_forecasts (?, ?, ?, ?)"};
  std::vector<size_t> statement_indexes;
  auto *txn = txn_manager_->BeginTransaction();
  auto db_oid = catalog_->GetDatabaseOid(common::ManagedPointer(txn), catalog::DEFAULT_DATABASE);
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

  query_exec_util_->BeginTransaction();
  query_exec_util_->SetCostModelFunction([]() { return std::make_unique<optimizer::TrivialCostModel>(); });
  for (size_t i = 0; i < 2; i++) {
    std::vector<type::TypeId> types;
    std::vector<parser::ConstantValueExpression> params;
    if (i == 0) {
      types = {type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER};
    } else {
      types = {type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::REAL};
    }

    for (auto &type : types) {
      params.emplace_back(type);
    }

    statement_indexes.emplace_back(
        query_exec_util_->CompileQuery(statements[i], common::ManagedPointer(&params), common::ManagedPointer(&types)));
  }

  // Execute all the forecast_clusters inserts
  {
    std::vector<parser::ConstantValueExpression> clusters_params(4);
    clusters_params[0] = parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(iteration));
    for (auto &cluster : prediction) {
      clusters_params[1] =
          parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(cluster.first));
      for (auto &qid_info : cluster.second) {
        execution::query_id_t qid{static_cast<uint32_t>(qid_info.first)};
        if (metadata.query_id_to_dboid_.find(qid) != metadata.query_id_to_dboid_.end()) {
          clusters_params[2] =
              parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(qid_info.first));
          clusters_params[3] = parser::ConstantValueExpression(
              type::TypeId::INTEGER, execution::sql::Integer(metadata.query_id_to_dboid_.find(qid)->second));

          query_exec_util_->ExecuteQuery(statement_indexes[0], nullptr, common::ManagedPointer(&clusters_params),
                                         nullptr);
        }
      }
    }
  }

  // Execute all forecaste_forecasts inserts
  {
    std::vector<parser::ConstantValueExpression> forecasts_params(4);
    forecasts_params[0] = parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(iteration));
    for (auto &cluster : prediction) {
      for (auto &qid_info : cluster.second) {
        forecasts_params[1] =
            parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(cluster.first));
        for (size_t interval = 0; interval < qid_info.second.size(); interval++) {
          forecasts_params[2] =
              parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(interval));
          forecasts_params[3] = parser::ConstantValueExpression(type::TypeId::INTEGER,
                                                                execution::sql::Integer(qid_info.second[interval]));

          query_exec_util_->ExecuteQuery(statement_indexes[1], nullptr, common::ManagedPointer(&forecasts_params),
                                         nullptr);
        }
      }
    }
  }

  query_exec_util_->EndTransaction(true);
}

void Pilot::PerformPlanning() {
  // We do the inference by having the python process read in the query_trace.csv file.
  // However, for the sampled parameters and query information, we will actually pull
  // that data directly from the internal SQL tables.
  //
  // Due to that, we will rename the contents of the QueryTraceMetricRawData files to
  // a timestamp-appended form (to aid in debugging at least).

  // Suspend the metrics thread while we are handling the data (snapshot).
  metrics_thread_->PauseMetrics();
  metrics_thread_->GetMetricsManager()->Aggregate();
  metrics_thread_->GetMetricsManager()->ToOutput();
  {
    auto raw = reinterpret_cast<metrics::QueryTraceMetricRawData *>(
        metrics_thread_->GetMetricsManager()
            ->AggregatedMetrics()
            .at(static_cast<uint8_t>(metrics::MetricsComponent::QUERY_TRACE))
            .get());
    if (raw) {
      raw->ResetAggregation(common::ManagedPointer(query_exec_util_));
    }
  }

  auto iteration = Pilot::planning_iteration_++;
  std::string input_path{metrics::QueryTraceMetricRawData::FILES[1]};

  // Infer forecast model
  std::vector<std::string> models{"LSTM"};
  auto result = model_server_manager_->InferForecastModel(input_path, forecast_model_save_path_, models, NULL,
                                                          workload_forecast_interval_);
  if (!result.second) {
    SELFDRIVING_LOG_ERROR("Forecast model inference failed");
    metrics_thread_->ResumeMetrics();
    return;
  }

  // Retrieve query information from internal tables
  auto metrics_output = metrics_thread_->GetMetricsManager()->GetMetricOutput(metrics::MetricsComponent::QUERY_TRACE);
  if (metrics_output == metrics::MetricsOutput::DB || metrics_output == metrics::MetricsOutput::CSV_DB) {
    auto metadata_result = RetrieveWorkloadMetadata(iteration);
    if (!metadata_result.second) {
      SELFDRIVING_LOG_ERROR("Failed to read from internal trace metadata tables");
      metrics_thread_->ResumeMetrics();
      return;
    }

    // Record forecast into internal tables
    RecordWorkloadForecastPrediction(iteration, result.first, metadata_result.first);

    // Construct workload forecast
    forecast_ = std::make_unique<selfdriving::WorkloadForecast>(result.first, metadata_result.first);
  } else {
    auto sample = settings_manager_->GetInt(settings::Param::forecast_sample_limit);
    forecast_ = std::make_unique<selfdriving::WorkloadForecast>(workload_forecast_interval_, sample);
  }

  // Copy file for backup -- future will not use this data
  for (size_t i = 0; i < 2; i++) {
    std::string input_path{metrics::QueryTraceMetricRawData::FILES[i]};
    auto filename = fmt::format("{}_{}", input_path.c_str(), iteration);
    std::rename(input_path.c_str(), filename.c_str());
  }

  // Perform planning
  std::vector<std::pair<const std::string, catalog::db_oid_t>> best_action_seq;
  Pilot::ActionSearch(&best_action_seq);

  metrics_thread_->ResumeMetrics();
}

void Pilot::ActionSearch(std::vector<std::pair<const std::string, catalog::db_oid_t>> *best_action_seq) {
  auto num_segs = forecast_->GetNumberOfSegments();
  auto end_segment_index = std::min(action_planning_horizon_ - 1, num_segs - 1);

  auto mcst =
      pilot::MonteCarloTreeSearch(common::ManagedPointer(this), common::ManagedPointer(forecast_), end_segment_index);
  mcst.BestAction(simulation_number_, best_action_seq);
  for (uint64_t i = 0; i < best_action_seq->size(); i++) {
    SELFDRIVING_LOG_INFO(fmt::format("Action Selected: timestamp: {}; action string: {} applied to database {}", i,
                                     best_action_seq->at(i).first,
                                     static_cast<uint32_t>(best_action_seq->at(i).second)));
  }
  PilotUtil::ApplyAction(common::ManagedPointer(this), best_action_seq->begin()->first,
                         best_action_seq->begin()->second);
}

void Pilot::ExecuteForecast(std::map<std::pair<execution::query_id_t, execution::pipeline_id_t>,
                                     std::vector<std::vector<std::vector<double>>>> *pipeline_to_prediction,
                            uint64_t start_segment_index, uint64_t end_segment_index) {
  NOISEPAGE_ASSERT(forecast_ != nullptr, "Need forecast_ initialized.");
  // first we make sure the pipeline metrics flag as well as the counters is enabled. Also set the sample rate to be 0
  // so that every query execution is being recorded

  // record previous parameters to be restored at the end of this function
  const bool old_metrics_enable = settings_manager_->GetBool(settings::Param::pipeline_metrics_enable);
  const bool old_counters_enable = settings_manager_->GetBool(settings::Param::counters_enable);
  const auto old_sample_rate = settings_manager_->GetInt64(settings::Param::pipeline_metrics_sample_rate);

  auto action_context = std::make_unique<common::ActionContext>(common::action_id_t(1));
  if (!old_metrics_enable) {
    settings_manager_->SetBool(settings::Param::pipeline_metrics_enable, true, common::ManagedPointer(action_context),
                               EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(2));
  if (!old_counters_enable) {
    settings_manager_->SetBool(settings::Param::counters_enable, true, common::ManagedPointer(action_context),
                               EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(3));
  settings_manager_->SetInt(settings::Param::pipeline_metrics_sample_rate, 100, common::ManagedPointer(action_context),
                            EmptySetterCallback);

  std::vector<execution::query_id_t> pipeline_qids;
  // Collect pipeline metrics of forecasted queries within the interval of segments
  auto pipeline_data = PilotUtil::CollectPipelineFeatures(common::ManagedPointer<selfdriving::Pilot>(this),
                                                          common::ManagedPointer(forecast_), start_segment_index,
                                                          end_segment_index, &pipeline_qids);
  // Then we perform inference through model server to get ou prediction results for all pipelines
  PilotUtil::InferenceWithFeatures(model_save_path_, model_server_manager_, pipeline_qids, pipeline_data,
                                   pipeline_to_prediction);

  // restore the old parameters
  action_context = std::make_unique<common::ActionContext>(common::action_id_t(4));
  if (!old_metrics_enable) {
    settings_manager_->SetBool(settings::Param::pipeline_metrics_enable, false, common::ManagedPointer(action_context),
                               EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(5));
  if (!old_counters_enable) {
    settings_manager_->SetBool(settings::Param::counters_enable, false, common::ManagedPointer(action_context),
                               EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(6));
  settings_manager_->SetInt(settings::Param::pipeline_metrics_sample_rate, old_sample_rate,
                            common::ManagedPointer(action_context), EmptySetterCallback);
}

}  // namespace noisepage::selfdriving
