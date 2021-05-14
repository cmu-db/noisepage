#include "self_driving/planning/pilot.h"

#include <memory>
#include <utility>

#include "common/action_context.h"
#include "common/error/error_code.h"
#include "execution/compiler/compilation_context.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/execution_settings.h"
#include "execution/exec_defs.h"
#include "loggers/selfdriving_logger.h"
#include "messenger/messenger.h"
#include "metrics/metrics_thread.h"
#include "network/postgres/statement.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/statistics/stats_storage.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "self_driving/model_server/model_server_manager.h"
#include "self_driving/planning/mcts/monte_carlo_tree_search.h"
#include "self_driving/planning/pilot_util.h"
#include "settings/settings_manager.h"
#include "task/task_manager.h"
#include "transaction/transaction_manager.h"
#include "util/query_exec_util.h"

namespace noisepage::selfdriving {

Pilot::Pilot(std::string ou_model_save_path, std::string interference_model_save_path,
             std::string forecast_model_save_path, common::ManagedPointer<catalog::Catalog> catalog,
             common::ManagedPointer<metrics::MetricsThread> metrics_thread,
             common::ManagedPointer<modelserver::ModelServerManager> model_server_manager,
             common::ManagedPointer<settings::SettingsManager> settings_manager,
             common::ManagedPointer<optimizer::StatsStorage> stats_storage,
             common::ManagedPointer<transaction::TransactionManager> txn_manager,
             std::unique_ptr<util::QueryExecUtil> query_exec_util,
             common::ManagedPointer<task::TaskManager> task_manager, uint64_t workload_forecast_interval,
             uint64_t sequence_length, uint64_t horizon_length)
    : ou_model_save_path_(std::move(ou_model_save_path)),
      interference_model_save_path_(std::move(interference_model_save_path)),
      catalog_(catalog),
      metrics_thread_(metrics_thread),
      model_server_manager_(model_server_manager),
      settings_manager_(settings_manager),
      stats_storage_(stats_storage),
      txn_manager_(txn_manager),
      query_exec_util_(std::move(query_exec_util)),
      task_manager_(task_manager),
      forecaster_(std::move(forecast_model_save_path), metrics_thread, model_server_manager, settings_manager,
                  task_manager, workload_forecast_interval, sequence_length, horizon_length) {
  forecast_ = nullptr;
  while (!model_server_manager_->ModelServerStarted()) {
  }
}

void Pilot::PerformPlanning() {
  // Suspend the metrics thread while we are handling the data (snapshot).
  metrics_thread_->PauseMetrics();

  // Populate the workload forecast
  auto metrics_output = metrics_thread_->GetMetricsManager()->GetMetricOutput(metrics::MetricsComponent::QUERY_TRACE);
  bool metrics_in_db =
      metrics_output == metrics::MetricsOutput::DB || metrics_output == metrics::MetricsOutput::CSV_AND_DB;
  forecast_ = forecaster_.LoadWorkloadForecast(
      metrics_in_db ? Forecaster::WorkloadForecastInitMode::INTERNAL_TABLES_WITH_INFERENCE
                    : Forecaster::WorkloadForecastInitMode::DISK_WITH_INFERENCE);
  if (forecast_ == nullptr) {
    SELFDRIVING_LOG_ERROR("Unable to initialize the WorkloadForecast information");
    metrics_thread_->ResumeMetrics();
    return;
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
  mcst.BestAction(simulation_number_, best_action_seq,
                  settings_manager_->GetInt64(settings::Param::pilot_memory_constraint));
  for (uint64_t i = 0; i < best_action_seq->size(); i++) {
    SELFDRIVING_LOG_INFO(fmt::format("Action Selected: Time Interval: {}; Action Command: {} Applied to Database {}", i,
                                     best_action_seq->at(i).first,
                                     static_cast<uint32_t>(best_action_seq->at(i).second)));
  }
  PilotUtil::ApplyAction(common::ManagedPointer(this), best_action_seq->begin()->first,
                         best_action_seq->begin()->second, false);
}

void Pilot::ExecuteForecast(uint64_t start_segment_index, uint64_t end_segment_index,
                            std::map<execution::query_id_t, std::pair<uint8_t, uint64_t>> *query_info,
                            std::map<uint32_t, uint64_t> *segment_to_offset,
                            std::vector<std::vector<double>> *interference_result_matrix) {
  NOISEPAGE_ASSERT(forecast_ != nullptr, "Need forecast_ initialized.");
  // first we make sure the pipeline metrics flag as well as the counters is enabled. Also set the sample rate to be 0
  // so that every query execution is being recorded

  std::vector<execution::query_id_t> pipeline_qids;
  // Collect pipeline metrics of forecasted queries within the interval of segments
  auto pipeline_data = PilotUtil::CollectPipelineFeatures(common::ManagedPointer<selfdriving::Pilot>(this),
                                                          common::ManagedPointer(forecast_), start_segment_index,
                                                          end_segment_index, &pipeline_qids, !WHAT_IF);

  // pipeline_to_prediction maps each pipeline to a vector of ou inference results for all ous of this pipeline
  // (where each entry corresponds to a different query param)
  // Each element of the outermost vector is a vector of ou prediction (each being a double vector) for one set of
  // parameters
  std::map<std::pair<execution::query_id_t, execution::pipeline_id_t>, std::vector<std::vector<std::vector<double>>>>
      pipeline_to_prediction;

  // Then we perform inference through model server to get ou prediction results for all pipelines
  PilotUtil::OUModelInference(ou_model_save_path_, model_server_manager_, pipeline_qids, pipeline_data->pipeline_data_,
                              &pipeline_to_prediction);

  PilotUtil::InterferenceModelInference(interference_model_save_path_, model_server_manager_, pipeline_to_prediction,
                                        common::ManagedPointer(forecast_), start_segment_index, end_segment_index,
                                        query_info, segment_to_offset, interference_result_matrix);
}

}  // namespace noisepage::selfdriving
