#include "execution/exec/execution_settings.h"
#include "settings/settings_manager.h"

namespace terrier::execution::exec {

void ExecutionSettings::UpdateFromSettingsManager(common::ManagedPointer<settings::SettingsManager> settings) {
  if (settings) {
    if (settings->GetBool(settings::Param::override_num_threads)) {
      number_of_threads_ = settings->GetInt(settings::Param::num_threads);
    }

    is_counters_enabled_ = settings->GetBool(settings::Param::counters_enable);
    is_pipeline_metrics_enabled_ = settings->GetBool(settings::Param::metrics_pipeline);
  }
}

}  // namespace terrier::execution::exec
