#include "main/db_main.h"

#define __SETTING_GFLAGS_DEFINE__    // NOLINT
#include "settings/settings_defs.h"  // NOLINT
#undef __SETTING_GFLAGS_DEFINE__     // NOLINT

#include "common/future.h"
#include "execution/execution_util.h"
#include "loggers/common_logger.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "storage/recovery/replication_log_provider.h"
#include "task/task.h"

namespace noisepage {

void DBMain::TryLoadStartupDDL() {
  // Load startup ddls
  std::vector<std::string> startup_ddls;
  if (settings_manager_ != nullptr) {
    auto input = settings_manager_->GetString(settings::Param::startup_ddl_path);
    std::ifstream ddl_file(input);
    if (ddl_file.is_open() && ddl_file.good()) {
      std::string input_line;
      while (std::getline(ddl_file, input_line)) {
        if (input_line.empty()) {
          // Skip the empty line
          continue;
        }

        if (input_line.size() > 2 && input_line[0] == '-' && input_line[1] == '-') {
          // Skip comments of form '-- comment'
          continue;
        }

        startup_ddls.emplace_back(std::move(input_line));
      }
    }
  } else {
    COMMON_LOG_WARN("TryLoadStartupDDL() invoked without SettingsManager");
  }

  if (!startup_ddls.empty() && task_manager_ != nullptr) {
    for (auto &ddl : startup_ddls) {
      common::Future<task::DummyResult> sync;
      task_manager_->AddTask(
          std::make_unique<task::TaskDDL>(catalog::INVALID_DATABASE_OID, ddl, common::ManagedPointer(&sync)));

      auto future_result = sync.DangerousWait();
      NOISEPAGE_ASSERT(future_result.second, "Error encountered executing startup DDL.");
      (void)future_result;
    }
  } else if (task_manager_ == nullptr) {
    COMMON_LOG_WARN("TryLoadStartupDDL() invoked without TaskManager");
  }
}

void DBMain::Run() {
  TryLoadStartupDDL();
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

  // Shutdown the following resources to safely release the task manager.
  (void)pilot_thread_.reset();
  (void)pilot_.reset();
  (void)metrics_thread_.reset();
  (void)task_manager_.reset();

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
