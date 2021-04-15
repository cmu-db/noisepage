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
  // Testing code needs to wait until the DBMS has successfully started up before sending queries to it.
  // Currently, DBMS startup is detected by scraping the DBMS's stdout for a startup message.
  // This startup message cannot be printed with the logging subsystem because logging can be disabled.
  // This is the only permitted use of cout in the system -- please use logging instead for any other uses.
  std::cout << fmt::format("NoisePage - Self-Driving Database Management System [port={}] [PID={}]",  // NOLINT
                           network_layer_->GetServer()->GetPort(), ::getpid())
            << std::endl;
  {
    std::unique_lock<std::mutex> lock(server->RunningMutex());
    server->RunningCV().wait(lock, [=] { return !(server->Running()); });
  }
}

void DBMain::ForceShutdown() {
  if (replication_manager_ != DISABLED) {
    GetLogManager()->EndReplication();
    if (!replication_manager_->IsPrimary()) {
      replication_manager_->GetAsReplica()->GetReplicationLogProvider()->EndReplication();
    }
  }
  if (recovery_manager_ != DISABLED && recovery_manager_->IsRecoveryTaskRunning()) {
    recovery_manager_->WaitForRecoveryToFinish();
  }
  if (network_layer_ != DISABLED && network_layer_->GetServer()->Running()) {
    network_layer_->GetServer()->StopServer();
  }
}

DBMain::~DBMain() { ForceShutdown(); }

DBMain::ExecutionLayer::ExecutionLayer(const std::string &bytecode_handlers_path) {
  execution::ExecutionUtil::InitTPL(bytecode_handlers_path);
}

DBMain::ExecutionLayer::~ExecutionLayer() { execution::ExecutionUtil::ShutdownTPL(); }

}  // namespace noisepage
