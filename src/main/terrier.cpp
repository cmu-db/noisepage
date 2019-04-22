#include <gflags/gflags.h>
#include "main/db_main.h"

#define __SETTING_GFLAGS_DEFINE__      // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_GFLAGS_DEFINE__       // NOLINT

int main(int argc, char **argv) {
  // Parse Setting Values
  ::google::SetUsageMessage("Usage Info: \n");
  ::google::ParseCommandLineFlags(&argc, &argv, true);
  std::unordered_map<terrier::settings::Param, terrier::settings::ParamInfo> param_map_;

  // A very ugly way to populate setting values to the map
#define __SETTING_POPULATE__           // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_POPULATE__            // NOLINT

  terrier::DBMain db(std::move(param_map_));
  db.Init();
  db.Run();
}
