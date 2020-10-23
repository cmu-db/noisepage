#include "execution/exec/execution_settings.h"
#include "settings/settings_manager.h"

namespace terrier::execution::exec {

void ExecutionSettings::UpdateFromSettingsManager(common::ManagedPointer<settings::SettingsManager> settings) {
  if (settings) {
    number_of_parallel_execution_threads_ = settings->GetInt(settings::Param::num_parallel_execution_threads);
    is_counters_enabled_ = settings->GetBool(settings::Param::counters_enable);
    is_pipeline_metrics_enabled_ = settings->GetBool(settings::Param::pipeline_metrics_enable);
  }
}

}  // namespace terrier::execution::exec
