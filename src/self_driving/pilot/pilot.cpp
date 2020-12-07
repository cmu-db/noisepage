#include "self_driving/pilot/pilot.h"

#include <memory>
#include <utility>

#include "common/action_context.h"
#include "common/macros.h"
#include "common/managed_pointer.h"
#include "main/db_main.h"
#include "parser/expression/constant_value_expression.h"
#include "self_driving/forecast/workload_forecast.h"
#include "self_driving/pilot_util.h"
#include "settings/settings_manager.h"

namespace noisepage::selfdriving {

Pilot::Pilot(common::ManagedPointer<DBMain> db_main, uint64_t workload_forecast_interval)
    : db_main_(db_main), workload_forecast_interval_(workload_forecast_interval) {
  forecast_ = nullptr;
}

void Pilot::PerformPlanning() {
  forecast_ = std::make_unique<WorkloadForecast>(workload_forecast_interval_);

  db_main_->GetMetricsThread()->PauseMetrics();
  ExecuteForecast();
  db_main_->GetMetricsThread()->ResumeMetrics();
}

void Pilot::ExecuteForecast() {
  NOISEPAGE_ASSERT(forecast_ != nullptr, "Need forecast_ initialized.");

  auto settings_manager = db_main_->GetSettingsManager();
  bool oldval = settings_manager->GetBool(settings::Param::pipeline_metrics_enable);
  bool oldcounter = settings_manager->GetBool(settings::Param::counters_enable);
  uint64_t oldintv = settings_manager->GetInt64(settings::Param::pipeline_metrics_interval);

  auto action_context = std::make_unique<common::ActionContext>(common::action_id_t(1));
  if (!oldval) {
    settings_manager->SetBool(settings::Param::pipeline_metrics_enable, true, common::ManagedPointer(action_context),
                              EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(2));
  if (!oldcounter) {
    settings_manager->SetBool(settings::Param::counters_enable, true, common::ManagedPointer(action_context),
                              EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(3));
  settings_manager->SetInt(settings::Param::pipeline_metrics_interval, 0, common::ManagedPointer(action_context),
                           EmptySetterCallback);

  PilotUtil::CollectPipelineFeatures(db_main_, common::ManagedPointer(forecast_));

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(4));
  if (!oldval) {
    settings_manager->SetBool(settings::Param::pipeline_metrics_enable, false, common::ManagedPointer(action_context),
                              EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(5));
  if (!oldcounter) {
    settings_manager->SetBool(settings::Param::counters_enable, false, common::ManagedPointer(action_context),
                              EmptySetterCallback);
  }

  action_context = std::make_unique<common::ActionContext>(common::action_id_t(6));
  settings_manager->SetInt(settings::Param::pipeline_metrics_interval, oldintv, common::ManagedPointer(action_context),
                           EmptySetterCallback);
}

}  // namespace noisepage::selfdriving
