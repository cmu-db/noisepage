#include "self_driving/planning/pilot.h"

#include <memory>
#include <utility>

#include "common/error/error_code.h"
#include "execution/compiler/compilation_context.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/execution_settings.h"
#include "execution/exec_defs.h"
#include "loggers/selfdriving_logger.h"
#include "messenger/messenger.h"
#include "metrics/metrics_thread.h"
#include "network/postgres/statement.h"
#include "optimizer/statistics/stats_storage.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "self_driving/model_server/model_server_manager.h"
#include "self_driving/planning/mcts/monte_carlo_tree_search.h"
#include "self_driving/planning/pilot_util.h"
#include "self_driving/planning/seq_tuning/sequence_tuning.h"
#include "settings/settings_manager.h"
#include "task/task_manager.h"
#include "transaction/transaction_manager.h"
#include "util/query_exec_util.h"
#include "util/self_driving_recording_util.h"

namespace noisepage::selfdriving::pilot {

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
    : planning_context_(std::move(ou_model_save_path), std::move(interference_model_save_path), catalog, metrics_thread,
                        model_server_manager, settings_manager, stats_storage, txn_manager, std::move(query_exec_util),
                        task_manager),
      forecaster_(std::move(forecast_model_save_path), metrics_thread, model_server_manager, settings_manager,
                  task_manager, workload_forecast_interval, sequence_length, horizon_length) {
  forecast_ = nullptr;
  while (!planning_context_.GetModelServerManager()->ModelServerStarted()) {
  }
}

void Pilot::PerformPlanning() {
  // Suspend the metrics thread while we are handling the data (snapshot).
  auto metrics_thread = planning_context_.GetMetricsThread();
  metrics_thread->PauseMetrics();

  // Populate the workload forecast
  auto metrics_output = metrics_thread->GetMetricsManager()->GetMetricOutput(metrics::MetricsComponent::QUERY_TRACE);
  bool metrics_in_db =
      metrics_output == metrics::MetricsOutput::DB || metrics_output == metrics::MetricsOutput::CSV_AND_DB;
  forecast_ = forecaster_.LoadWorkloadForecast(
      metrics_in_db ? Forecaster::WorkloadForecastInitMode::INTERNAL_TABLES_WITH_INFERENCE
                    : Forecaster::WorkloadForecastInitMode::DISK_WITH_INFERENCE);
  if (forecast_ == nullptr) {
    SELFDRIVING_LOG_ERROR("Unable to initialize the WorkloadForecast information");
    metrics_thread->ResumeMetrics();
    return;
  }

  // Perform planning
  if (planning_context_.GetSettingsManager()->GetBool(settings::Param::enable_seq_tuning)) {
    std::vector<std::set<std::pair<const std::string, catalog::db_oid_t>>> best_actions_seq;
    Pilot::ActionSearchBaseline(&best_actions_seq);
  } else {
    std::vector<ActionTreeNode> best_action_seq;
    Pilot::ActionSearch(&best_action_seq);
  }

  metrics_thread->ResumeMetrics();
}

void Pilot::ActionSearch(std::vector<ActionTreeNode> *best_action_seq) {
  // Put every database into planning_context to create transaction context and catalog accessor
  std::set<catalog::db_oid_t> db_oids = forecast_->GetDBOidSet();
  for (auto db_oid : db_oids) planning_context_.AddDatabase(db_oid);

  auto memory_info = PilotUtil::ComputeMemoryInfo(planning_context_, forecast_.get());
  planning_context_.SetMemoryInfo(std::move(memory_info));

  auto num_segs = forecast_->GetNumberOfSegments();
  auto end_segment_index = std::min(action_planning_horizon_ - 1, num_segs - 1);
  auto mcst = MonteCarloTreeSearch(planning_context_, common::ManagedPointer(forecast_), end_segment_index);
  mcst.RunSimulation(simulation_number_,
                     planning_context_.GetSettingsManager()->GetInt64(settings::Param::pilot_memory_constraint));

  // Record the top 3 at each level along the "best action path".
  // TODO(wz2): May want to improve this at a later time.
  std::vector<std::vector<ActionTreeNode>> layered_action;
  mcst.BestAction(&layered_action, 3);
  for (size_t i = 0; i < layered_action.size(); i++) {
    ActionTreeNode &action = layered_action[i].front();
    best_action_seq->emplace_back(action);

    SELFDRIVING_LOG_INFO(fmt::format("Action Selected: Time Interval: {}; Action Command: {} Applied to Database {}", i,
                                     best_action_seq->at(i).GetActionText(),
                                     static_cast<uint32_t>(best_action_seq->at(i).GetDbOid())));
  }

  uint64_t timestamp = metrics::MetricsUtil::Now();
  util::SelfDrivingRecordingUtil::RecordBestActions(timestamp, layered_action, planning_context_.GetTaskManager());

  ActionTreeNode &best_action = (*best_action_seq)[0];
  util::SelfDrivingRecordingUtil::RecordAppliedAction(timestamp, best_action.GetActionId(), best_action.GetCost(),
                                                      best_action.GetDbOid(), best_action.GetActionText(),
                                                      planning_context_.GetTaskManager());

  // Invalidate database and memory information
  planning_context_.ClearDatabases();
  planning_context_.SetMemoryInfo(MemoryInfo());

  // Apply the best action WITHOUT "what-if"
  PilotUtil::ApplyAction(planning_context_, best_action.GetActionText(), best_action.GetDbOid(), false);
}

void Pilot::ActionSearchBaseline(
    std::vector<std::set<std::pair<const std::string, catalog::db_oid_t>>> *best_actions_seq) {
  std::set<catalog::db_oid_t> db_oids = forecast_->GetDBOidSet();
  for (auto db_oid : db_oids) planning_context_.AddDatabase(db_oid);

  auto memory_info = PilotUtil::ComputeMemoryInfo(planning_context_, forecast_.get());
  planning_context_.SetMemoryInfo(std::move(memory_info));

  auto num_segs = forecast_->GetNumberOfSegments();
  auto end_segment_index = std::min(action_planning_horizon_ - 1, num_segs - 1);

  auto seq_tunining = SequenceTuning(planning_context_, common::ManagedPointer(forecast_), end_segment_index);

  std::vector<std::set<std::pair<const std::string, catalog::db_oid_t>>> best_action_set_seq;
  seq_tunining.BestAction(planning_context_.GetSettingsManager()->GetInt64(settings::Param::pilot_memory_constraint),
                          &best_action_set_seq);

  for (uint64_t action_set_idx = 0; action_set_idx < best_action_set_seq.size(); action_set_idx++) {
    auto action_set = best_action_set_seq.at(action_set_idx);
    for (auto const &action UNUSED_ATTRIBUTE : action_set) {
      SELFDRIVING_LOG_INFO(fmt::format("Action Selected: Time Interval: {}; Action Command: {} Applied to Database {}",
                                       action_set_idx, action.first, static_cast<uint32_t>(action.second)));
    }
    best_actions_seq->emplace_back(action_set);
  }

  // Invalidate database and memory information
  planning_context_.ClearDatabases();
  planning_context_.SetMemoryInfo(MemoryInfo());

  for (auto const &action : *best_actions_seq->begin())
    PilotUtil::ApplyAction(planning_context_, action.first, action.second, false);
}

}  // namespace noisepage::selfdriving::pilot
