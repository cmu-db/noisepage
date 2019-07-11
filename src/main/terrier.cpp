#include <gflags/gflags.h>
#include <memory>
#include <random>
#include <unordered_map>
#include <utility>
#include "main/db_main.h"
#include "settings/settings_manager.h"

/*
 * Define gflags configurations.
 * This will expand to a list of code like:
 * DEFINE_int32(port, 15721, "Terrier port (default: 15721)");
 */
#define __SETTING_GFLAGS_DEFINE__      // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_GFLAGS_DEFINE__       // NOLINT

int main(int argc, char *argv[]) {
  // initialize loggers
  // Parse Setting Values
  ::google::SetUsageMessage("Usage Info: \n");
  ::google::ParseCommandLineFlags(&argc, &argv, true);
  std::unordered_map<terrier::settings::Param, terrier::settings::ParamInfo> param_map;

  // initialize stat registry
  auto main_stat_reg = std::make_shared<terrier::common::StatisticsRegistry>();

  terrier::settings::SettingsManager::ConstructParamMap(param_map);
  terrier::DBMain db(std::move(param_map));
  db.Run();
}
