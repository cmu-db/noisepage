#include "self_driving/pilot/pilot.h"

#include <memory>
#include <utility>

#include "common/action_context.h"
#include "execution/exec_defs.h"
#include "messenger/messenger.h"
#include "metrics/metrics_thread.h"
#include "optimizer/statistics/stats_storage.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "self_driving/forecast/workload_forecast.h"
#include "self_driving/model_server/model_server_manager.h"
#include "self_driving/pilot_util.h"
#include "self_driving/pilot/mcst/monte_carlo_search_tree.h"
#include "settings/settings_manager.h"

namespace noisepage::selfdriving {

Pilot::Pilot(std::string model_save_path, common::ManagedPointer<catalog::Catalog> catalog,
             common::ManagedPointer<metrics::MetricsThread> metrics_thread,
             common::ManagedPointer<modelserver::ModelServerManager> model_server_manager,
             common::ManagedPointer<settings::SettingsManager> settings_manager,
             common::ManagedPointer<optimizer::StatsStorage> stats_storage,
             common::ManagedPointer<transaction::TransactionManager> txn_manager, uint64_t workload_forecast_interval)
    : model_save_path_(std::move(model_save_path)),
      catalog_(catalog),
      metrics_thread_(metrics_thread),
      model_server_manager_(model_server_manager),
      settings_manager_(settings_manager),
      stats_storage_(stats_storage),
      txn_manager_(txn_manager),
      workload_forecast_interval_(workload_forecast_interval) {
  forecast_ = nullptr;
  while (!model_server_manager_->ModelServerStarted()) {
  }
}

void Pilot::PerformPlanning() {
  forecast_ = std::make_unique<WorkloadForecast>(workload_forecast_interval_);

  metrics_thread_->PauseMetrics();
  std::map<pilot::action_id_t, std::unique_ptr<pilot::AbstractAction>> best_action_maps;
  std::vector<pilot::action_id_t> best_action_seq;
  Pilot::ActionSearch(&best_action_maps, &best_action_seq);
  metrics_thread_->ResumeMetrics();
}

void Pilot::ActionSearch(std::map<pilot::action_id_t, std::unique_ptr<pilot::AbstractAction>> *best_action_map,
                         std::vector<pilot::action_id_t> *best_action_seq) {
  for (auto i = 0; i + action_planning_horizon_ <= forecast_->GetNumberOfSegments(); i++) {
    std::vector<std::unique_ptr<planner::AbstractPlanNode>> plans =
        PilotUtil::GetQueryPlans(common::ManagedPointer(this), common::ManagedPointer(forecast_), i,
                                 i + action_planning_horizon_ - 1);
    auto mcst =
        pilot::MonteCarloSearchTree(common::ManagedPointer(this), common::ManagedPointer(forecast_), plans,
                                    action_planning_horizon_, simulation_number_, i);
    auto best_action = mcst.BestAction(best_action_map, best_action_seq);

    std::vector<uint64_t> curr_oids;
    for (auto oid : forecast_->forecast_segments_[i].GetDBOids()) {
      if (std::find(curr_oids.begin(), curr_oids.end(), oid) == curr_oids.end())
        curr_oids.push_back(oid);
    }

    PilotUtil::ApplyAction(common::ManagedPointer(this), curr_oids, best_action);
  }
}

void Pilot::ExecuteForecast(
    std::map<std::pair<execution::query_id_t, execution::pipeline_id_t>, std::vector<std::vector<std::vector<double>>>>
        *pipeline_to_prediction, uint64_t start_segment_index, uint64_t end_segment_index) {
  NOISEPAGE_ASSERT(forecast_ != nullptr, "Need forecast_ initialized.");
  bool oldval = settings_manager_->GetBool(settings::Param::pipeline_metrics_enable);
  bool oldcounter = settings_manager_->GetBool(settings::Param::counters_enable);
  uint64_t oldintv = settings_manager_->GetInt64(settings::Param::pipeline_metrics_interval);

  auto action_context = std::make_unique<common::ActionContext>(common::action_id_t(1));
  if (!oldval) {
    settings_manager_->SetBool(settings::Param::pipeline_metrics_enable, true, common::ManagedPointer(action_context),
                               EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(2));
  if (!oldcounter) {
    settings_manager_->SetBool(settings::Param::counters_enable, true, common::ManagedPointer(action_context),
                               EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(3));
  settings_manager_->SetInt(settings::Param::pipeline_metrics_interval, 0, common::ManagedPointer(action_context),
                            EmptySetterCallback);

  auto pipeline_data = PilotUtil::CollectPipelineFeatures(common::ManagedPointer<selfdriving::Pilot>(this),
                                                          common::ManagedPointer(forecast_),
                                                          start_segment_index, end_segment_index);

  PilotUtil::InferenceWithFeatures(model_save_path_, model_server_manager_, pipeline_data,
                                   pipeline_to_prediction);

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(4));
  if (!oldval) {
    settings_manager_->SetBool(settings::Param::pipeline_metrics_enable, false, common::ManagedPointer(action_context),
                               EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(5));
  if (!oldcounter) {
    settings_manager_->SetBool(settings::Param::counters_enable, false, common::ManagedPointer(action_context),
                               EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(6));
  settings_manager_->SetInt(settings::Param::pipeline_metrics_interval, oldintv, common::ManagedPointer(action_context),
                            EmptySetterCallback);
}

}  // namespace noisepage::selfdriving
