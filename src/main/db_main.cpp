#include "main/db_main.h"

#define __SETTING_GFLAGS_DEFINE__    // NOLINT
#include "settings/settings_defs.h"  // NOLINT
#undef __SETTING_GFLAGS_DEFINE__     // NOLINT

#include "execution/execution_util.h"
#include "storage/recovery/replication_log_provider.h"

namespace noisepage {

void DBMain::Run() {
  NOISEPAGE_ASSERT(network_layer_ != DISABLED, "Trying to run without a NetworkLayer.");
  const auto server = network_layer_->GetServer();
  try {
    server->RunServer();
  } catch (NetworkProcessException &e) {
    return;
  }
  {
    std::unique_lock<std::mutex> lock(server->RunningMutex());
    server->RunningCV().wait(lock, [=] { return !(server->Running()); });
  }
}

void DBMain::ForceShutdown() {
  if (!force_shutdown_called_) {
    if (replication_manager_ != DISABLED) {
      replication_manager_->GetReplicationLogProvider()->EndReplication();
    }
    if (recovery_manager_ != DISABLED) {
      recovery_manager_->WaitForRecoveryToFinish();
    }
    if (network_layer_ != DISABLED && network_layer_->GetServer()->Running()) {
      network_layer_->GetServer()->StopServer();
    }
  }
  force_shutdown_called_ = true;
}

DBMain::~DBMain() { ForceShutdown(); }

DBMain::ExecutionLayer::ExecutionLayer() { execution::ExecutionUtil::InitTPL(); }

DBMain::ExecutionLayer::~ExecutionLayer() { execution::ExecutionUtil::ShutdownTPL(); }

}  // namespace noisepage
