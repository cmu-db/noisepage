#include <gflags/gflags.h>

#include <memory>
#include <unordered_map>
#include <utility>

#include "execution/sql/ddl_executors.h"
#include "loggers/loggers_util.h"
#include "main/db_main.h"
#include "planner/plannodes/create_table_plan_node.h"
#include "settings/settings_manager.h"
#include "type/transient_value_factory.h"

int main(int argc, char *argv[]) {
  // initialize loggers
  // Parse Setting Values
  ::google::SetUsageMessage("Usage Info: \n");
  ::google::ParseCommandLineFlags(&argc, &argv, true);

  terrier::LoggersUtil::Initialize();

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
                     .SetUseStatsStorage(true)
                     .SetUseExecution(true)
                     .SetUseTrafficCop(true)
                     .SetUseNetwork(true)
                     .Build();

  db_main->Run();

  terrier::LoggersUtil::ShutDown();
}
