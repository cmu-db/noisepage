#include "self_driving/pilot/pilot.h"

#include <memory>
#include <utility>

#include "common/action_context.h"
#include "execution/exec_defs.h"
#include "messenger/messenger.h"
#include "metrics/metrics_thread.h"
#include "optimizer/statistics/stats_storage.h"
#include "self_driving/forecast/workload_forecast.h"
#include "self_driving/model_server/model_server_manager.h"
#include "self_driving/pilot_util.h"
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
  ExecuteForecast();
  metrics_thread_->ResumeMetrics();
}

void Pilot::ExecuteForecast() {
  NOISEPAGE_ASSERT(forecast_ != nullptr, "Need forecast_ initialized.");
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

  auto pipeline_data = PilotUtil::CollectPipelineFeatures(common::ManagedPointer<selfdriving::Pilot>(this),
                                                          common::ManagedPointer(forecast_));
  std::list<std::tuple<execution::query_id_t, execution::pipeline_id_t, std::vector<std::vector<double>>>>
      pipeline_to_prediction;
  PilotUtil::InferenceWithFeatures(model_save_path_, model_server_manager_, pipeline_data, &pipeline_to_prediction);

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
