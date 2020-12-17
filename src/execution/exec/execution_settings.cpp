#include "execution/exec/execution_settings.h"

#include "settings/settings_manager.h"

namespace noisepage::execution::exec {

void ExecutionSettings::UpdateFromSettingsManager(common::ManagedPointer<settings::SettingsManager> settings) {
  if (settings) {
    is_parallel_execution_enabled_ = settings->GetBool(settings::Param::parallel_execution);
    number_of_parallel_execution_threads_ = settings->GetInt(settings::Param::num_parallel_execution_threads);
    is_counters_enabled_ = settings->GetBool(settings::Param::counters_enable);
    is_pipeline_metrics_enabled_ = settings->GetBool(settings::Param::pipeline_metrics_enable);
  }
}

}  // namespace noisepage::execution::exec
