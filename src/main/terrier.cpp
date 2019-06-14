#include <gflags/gflags.h>
#include <memory>
#include <unordered_map>
#include <utility>
#include "main/db_main.h"
#include "settings/settings_manager.h"
#include "dependency/di_help.h"

/*
 * Define gflags configurations.
 * This will expand to a list of code like:
 * DEFINE_int32(port, 15721, "Terrier port (default: 15721)");
 */
#define __SETTING_GFLAGS_DEFINE__      // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_GFLAGS_DEFINE__       // NOLINT

class Foo {
 public:
  virtual ~Foo() = default;
};

class Bar : public Foo {};

int main(int argc, char *argv[]) {
  // initialize loggers
  // Parse Setting Values
//  ::google::SetUsageMessage("Usage Info: \n");
//  ::google::ParseCommandLineFlags(&argc, &argv, true);
//  std::unordered_map<terrier::settings::Param, terrier::settings::ParamInfo> param_map;
//
//  // initialize stat registry
//  auto main_stat_reg = std::make_shared<terrier::common::StatisticsRegistry>();
//
//  terrier::settings::SettingsManager::ConstructParamMap(param_map);
//  terrier::DBMain db(std::move(param_map));
//  db.Init();
//  db.Run();
  auto injector = [] {
    return boost::di::make_injector<terrier::di::StrictBinding>(terrier::di::bind<Foo>()
        .in(terrier::di::terrier_module).to<Bar>());
  };

  auto injector1 = injector();
  auto injector2 = injector();
  auto injector3 = injector();


  printf("%p\n", injector1.create<Foo *>());
  printf("%p\n", injector1.create<terrier::common::ManagedPointer<Foo>>().operator->());
  printf("%p\n", &injector1.create<const Foo &>());
  printf("%p\n", &injector2.create<const Foo &>());
  printf("%p\n", &injector2.create<const Foo &>());
  printf("%p\n", injector1.create<terrier::common::ManagedPointer<Foo>>().operator->());
  printf("%p\n", injector2.create<terrier::common::ManagedPointer<Foo>>().operator->());
  printf("%p\n", injector3.create<terrier::common::ManagedPointer<Foo>>().operator->());
}
