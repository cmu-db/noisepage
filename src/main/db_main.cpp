#include "main/db_main.h"

#define __SETTING_GFLAGS_DEFINE__    // NOLINT
#include "settings/settings_defs.h"  // NOLINT
#undef __SETTING_GFLAGS_DEFINE__     // NOLINT

#include "execution/execution_util.h"
#include "loggers/common_logger.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "storage/recovery/replication_log_provider.h"

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

  if (!startup_ddls.empty() && query_exec_util_ != nullptr) {
    query_exec_util_->BeginTransaction();
    query_exec_util_->SetCostModelFunction([]() { return std::make_unique<optimizer::TrivialCostModel>(); });
    for (auto &ddl : startup_ddls) {
      query_exec_util_->ExecuteDDL(ddl);
    }
    query_exec_util_->EndTransaction(true);
  } else if (query_exec_util_ == nullptr) {
    COMMON_LOG_WARN("TryLoadStartupDDL() invoked without QueryExecUtil");
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

  {
    std::unique_lock<std::mutex> lock(server->RunningMutex());
    server->RunningCV().wait(lock, [=] { return !(server->Running()); });
  }
}

void DBMain::ForceShutdown() {
<<<<<<< HEAD
  {
    // Release all claims to QueryInternalThread
    if (metrics_manager_ != nullptr) {
      metrics_manager_->SetQueryInternalThread(nullptr);
    }

    if (pilot_ != nullptr) {
      pilot_->SetQueryInternalThread(nullptr);
    }
    (void)pilot_thread_.release();
    (void)metrics_thread_.release();

    // Need to let internal thread flush through requests
    (void)query_internal_thread_.release();
  }

=======
  if (replication_manager_ != DISABLED) {
    if (!replication_manager_->IsPrimary()) {
      replication_manager_->GetAsReplica()->GetReplicationLogProvider()->EndReplication();
    }
  }
  if (recovery_manager_ != DISABLED && recovery_manager_->IsRecoveryTaskRunning()) {
    recovery_manager_->WaitForRecoveryToFinish();
  }
>>>>>>> cmudb/master
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
