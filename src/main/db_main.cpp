#include "main/db_main.h"
#include <memory>
#include <unordered_map>
#include <utility>
#include "common/managed_pointer.h"
#include "loggers/loggers_util.h"
#include "settings/settings_manager.h"
#include "settings/settings_param.h"
#include "storage/garbage_collector_thread.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"

namespace terrier {

DBMain::DBMain(std::unordered_map<settings::Param, settings::ParamInfo> &&param_map)
    : param_map_(std::move(param_map)) {
  LoggersUtil::Initialize(false);

  // initialize stat registry
  main_stat_reg_ = std::make_shared<common::StatisticsRegistry>();

  // create the global transaction mgr
  buffer_segment_pool_ = new storage::RecordBufferSegmentPool(
      type::TransientValuePeeker::PeekInteger(
          param_map_.find(settings::Param::record_buffer_segment_size)->second.value_),
      type::TransientValuePeeker::PeekInteger(
          param_map_.find(settings::Param::record_buffer_segment_reuse)->second.value_));
  settings_manager_ = new settings::SettingsManager(this);
  thread_registry_ = new common::DedicatedThreadRegistry;

  // Create LogManager
  log_manager_ = new storage::LogManager(
      settings_manager_->GetString(settings::Param::log_file_path),
      settings_manager_->GetInt(settings::Param::num_log_manager_buffers),
      std::chrono::milliseconds{settings_manager_->GetInt(settings::Param::log_serialization_interval)},
      std::chrono::milliseconds{settings_manager_->GetInt(settings::Param::log_persist_interval)},
      settings_manager_->GetInt(settings::Param::log_persist_threshold), buffer_segment_pool_,
      common::ManagedPointer(thread_registry_));
  log_manager_->Start();

  txn_manager_ = new transaction::TransactionManager(buffer_segment_pool_, true, log_manager_);
  gc_thread_ = new storage::GarbageCollectorThread(txn_manager_,
                                                   std::chrono::milliseconds{type::TransientValuePeeker::PeekInteger(
                                                       param_map_.find(settings::Param::gc_interval)->second.value_)});

  thread_pool_ = new common::WorkerPool(
      type::TransientValuePeeker::PeekInteger(param_map_.find(settings::Param::num_worker_threads)->second.value_), {});
  thread_pool_->Startup();

  t_cop_ = new trafficcop::TrafficCop;
  command_factory_ = new network::PostgresCommandFactory;

  connection_handle_factory_ = new network::ConnectionHandleFactory(common::ManagedPointer(t_cop_));
  provider_ = new network::PostgresProtocolInterpreter::Provider(common::ManagedPointer(command_factory_));
  server_ =
      new network::TerrierServer(common::ManagedPointer(provider_), common::ManagedPointer(connection_handle_factory_),
                                 common::ManagedPointer(thread_registry_));

  LOG_INFO("Initialization complete");
}

void DBMain::Run() {
  running = true;
  server_->SetPort(static_cast<int16_t>(
      type::TransientValuePeeker::PeekInteger(param_map_.find(settings::Param::port)->second.value_)));
  server_->RunServer();

  {
    std::unique_lock<std::mutex> lock(server_->RunningMutex());
    server_->RunningCV().wait(lock, [=] { return !(server_->Running()); });
  }

  // server loop exited, begin cleaning up
  CleanUp();
}

void DBMain::ForceShutdown() {
  if (running) {
    server_->StopServer();
  }
  CleanUp();
}

void DBMain::CleanUp() {
  main_stat_reg_->Shutdown(false);
  log_manager_->PersistAndStop();
  thread_pool_->Shutdown();
  LOG_INFO("Terrier has shut down.");
  LoggersUtil::ShutDown();
}

}  // namespace terrier
