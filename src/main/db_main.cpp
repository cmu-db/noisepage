#include "main/db_main.h"

#define __SETTING_GFLAGS_DEFINE__      // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_GFLAGS_DEFINE__       // NOLINT

namespace terrier {

void DBMain::Run() {
  running_ = true;
  network_layer_->GetServer()->RunServer();

  {
    std::unique_lock<std::mutex> lock(network_layer_->GetServer()->RunningMutex());
    network_layer_->GetServer()->RunningCV().wait(lock, [=] { return !(network_layer_->GetServer()->Running()); });
  }
}

void DBMain::ForceShutdown() {
  if (running_) {
    network_layer_->GetServer()->StopServer();
  }
}

DBMain::~DBMain() { ForceShutdown(); }

}  // namespace terrier
