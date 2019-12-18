#include <gflags/gflags.h>

#include <memory>
#include <unordered_map>
#include <utility>

#include "loggers/loggers_util.h"
#include "main/db_main.h"
#include "settings/settings_manager.h"

int main(int argc, char *argv[]) {
  // initialize loggers
  // Parse Setting Values
  ::google::SetUsageMessage("Usage Info: \n");
  ::google::ParseCommandLineFlags(&argc, &argv, true);

  terrier::LoggersHandle
      loggers_handle;  // This is sort of a dummy object to make sure the debug loggers get shut down last. We
  // otherwise can't enforce this while relying on destruction order for the components because
  // explicitly shutting down the loggers in DBMain's destructor would happen first.

  // initialize stat registry
  auto main_stat_reg =
      std::make_unique<terrier::common::StatisticsRegistry>();  // TODO(Matt): do we still want this thing?

  std::unordered_map<terrier::settings::Param, terrier::settings::ParamInfo> param_map;
  terrier::settings::SettingsManager::ConstructParamMap(param_map);

  auto db_main = terrier::DBMain::Builder()
                     .SetSettingsParameterMap(std::move(param_map))
                     .SetUseSettingsManager(true)
                     .SetUseMetrics(true)
                     .SetUseMetricsThread(true)
                     .SetUseLogging(true)
                     .SetUseGC(true)
                     .SetUseCatalog(true)
                     .SetUseGCThread(true)
                     .SetUseTrafficCop(true)
                     .SetUseNetwork(true)
                     .Build();

  db_main->Run();
}
