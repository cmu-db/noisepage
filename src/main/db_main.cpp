#include "main/db_main.h"

#define __SETTING_GFLAGS_DEFINE__    // NOLINT
#include "settings/settings_defs.h"  // NOLINT
#undef __SETTING_GFLAGS_DEFINE__     // NOLINT

#include "common/error/exception.h"
#include "common/future.h"
#include "execution/execution_util.h"
#include "loggers/common_logger.h"
#include "task/task.h"

namespace noisepage {

void DBMain::TryLoadStartupDDL() {
  NOISEPAGE_ASSERT(settings_manager_ != nullptr, "Startup DDL requires settings manager for startup DDL path.");
  NOISEPAGE_ASSERT(task_manager_ != nullptr, "Startup DDL requires task manager for startup DDL execution.");

  // Parse the startup DDL.
  std::vector<std::string> startup_ddls;
  {
    auto startup_ddl_location = settings_manager_->GetString(settings::Param::startup_ddl_path);
    std::ifstream ddl_file(startup_ddl_location);
    if (!(ddl_file.is_open() && ddl_file.good())) {
      throw EXECUTION_EXCEPTION(fmt::format("Error locating startup DDL at \"{}\".", startup_ddl_location),
                                common::ErrorCode::ERRCODE_INTERNAL_ERROR);
    }
    std::string input_line;
    while (std::getline(ddl_file, input_line)) {
      bool is_empty = input_line.empty();                                                       // Empty lines.
      bool is_comment = input_line.size() > 2 && input_line[0] == '-' && input_line[1] == '-';  // -- comments.
      // Skip certain types of lines lines.
      if (is_empty || is_comment) {
        continue;
      }
      // Otherwise, execute the SQL command, which is assumed to fit on one line.
      startup_ddls.emplace_back(std::move(input_line));
    }
  }

  catalog::db_oid_t default_db_oid = catalog_layer_->GetCatalog()->GetDefaultDatabaseOid();

  auto policy = txn_layer_->GetTransactionManager()->GetDefaultTransactionPolicy();
  policy.replication_ = transaction::ReplicationPolicy::DISABLE;

  // Run the startup DDL commands, waiting for successful completion one-by-one.
  for (auto &ddl : startup_ddls) {
    COMMON_LOG_INFO("Executing startup DDL: {}", ddl);
    common::FutureDummy sync;
    task_manager_->AddTask(task::TaskDDL::Builder()
                               .SetDatabaseOid(default_db_oid)
                               .SetFuture(common::ManagedPointer(&sync))
                               .SetQueryText(std::move(ddl))
                               .SetTransactionPolicy(policy)
                               .Build());
    auto future_result = sync.DangerousWait();
    if (!future_result.second) {
      throw EXECUTION_EXCEPTION("Error executing startup DDL.", common::ErrorCode::ERRCODE_INTERNAL_ERROR);
    }
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
  pilot_thread_.reset();
  pilot_.reset();
  metrics_thread_.reset();
  task_manager_.reset();

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
