#include "self_driving/pilot/pilot.h"

#include <memory>
#include <utility>

#include "common/action_context.h"
#include "common/macros.h"
#include "common/managed_pointer.h"
#include "parser/expression/constant_value_expression.h"
#include "self_driving/forecast/workload_forecast.h"
#include "self_driving/pilot_util.h"
#include "settings/settings_manager.h"

namespace noisepage::selfdriving {

Pilot::Pilot(common::ManagedPointer<catalog::Catalog> catalog,
             common::ManagedPointer<metrics::MetricsThread> metrics_thread,
             common::ManagedPointer<modelserver::ModelServerManager> model_server_manager,
             common::ManagedPointer<settings::SettingsManager> settings_manager,
             common::ManagedPointer<optimizer::StatsStorage> stats_storage,
             common::ManagedPointer<transaction::TransactionManager> txn_manager, uint64_t workload_forecast_interval)
    : catalog_(catalog),
      metrics_thread_(metrics_thread),
      ms_manager_(model_server_manager),
      settings_manager_(settings_manager),
      stats_storage_(stats_storage),
      txn_manager_(txn_manager),
      workload_forecast_interval_(workload_forecast_interval) {
  forecast_ = nullptr;
  while (!ms_manager_->ModelServerStarted()) {
  }

  modelserver::ModelServerFuture<std::string> future;
  std::vector<std::string> models{"lr", "gbm"};
  std::string project_build_path = getenv(Pilot::BUILD_ABS_PATH);
  ms_manager_->TrainWith(models, std::string(project_build_path) + "/../script/model/mini_data_dev4",
                         project_build_path + Pilot::SAVE_PATH,
                         common::ManagedPointer<modelserver::ModelServerFuture<std::string>>(&future));

  auto wait_res = future.Wait();
  // Do inference through model server, get cost
  NOISEPAGE_ASSERT(wait_res.second, "Model Server Training failed");  // Training succeeds
}

void Pilot::PerformPlanning() {
  forecast_ = std::make_unique<WorkloadForecast>(workload_forecast_interval_);

  metrics_thread_->PauseMetrics();
  ExecuteForecast();
  metrics_thread_->ResumeMetrics();
}

void Pilot::ExecuteForecast() {
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
                                                          common::ManagedPointer(forecast_));
  std::list<std::tuple<execution::query_id_t, execution::pipeline_id_t, std::vector<std::vector<double>>>>
      pipeline_to_prediction;
  PilotUtil::InferenceWithFeatures(ms_manager_, pipeline_data, &pipeline_to_prediction);

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
