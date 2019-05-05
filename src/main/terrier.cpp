#include <gflags/gflags.h>
#include <memory>
#include <unordered_map>
#include <utility>
#include "main/db_main.h"

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

  /*
   * Populate gflag values to param map.
   * This will expand to a list of code like:
   * param_map_.emplace(
   *     terrier::settings::Param::port,
   *     terrier::settings::ParamInfo(port, terrier::type::TransientValueFactory::GetInteger(FLAGS_port),
   *                                  "Terrier port (default: 15721)",
   *                                  terrier::type::TransientValueFactory::GetInteger(15721), is_mutable));
   */

#define __SETTING_POPULATE__           // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_POPULATE__            // NOLINT

  terrier::DBMain db(std::move(param_map));
  db.Init();
  db.Run();
}
