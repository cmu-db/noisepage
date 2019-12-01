#include "main/db_main.h"
#include <memory>
#include <unordered_map>
#include <utility>
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
  main_stat_reg_ = std::make_unique<common::StatisticsRegistry>();

  // create the global transaction mgr
  buffer_segment_pool_ = new storage::RecordBufferSegmentPool(
      type::TransientValuePeeker::PeekInteger(
          param_map_.find(settings::Param::record_buffer_segment_size)->second.value_),
      type::TransientValuePeeker::PeekInteger(
          param_map_.find(settings::Param::record_buffer_segment_reuse)->second.value_));
  settings_manager_ = new settings::SettingsManager(this);
  metrics_manager_ = new metrics::MetricsManager;
  thread_registry_ = new common::DedicatedThreadRegistry(common::ManagedPointer(metrics_manager_));

  // Create LogManager
  log_manager_ = new storage::LogManager(
      settings_manager_->GetString(settings::Param::log_file_path),
      static_cast<uint64_t>(settings_manager_->GetInt64(settings::Param::num_log_manager_buffers)),
      std::chrono::milliseconds{settings_manager_->GetInt(settings::Param::log_serialization_interval)},
      std::chrono::milliseconds{settings_manager_->GetInt(settings::Param::log_persist_interval)},
      static_cast<uint64_t>(settings_manager_->GetInt64(settings::Param::log_persist_threshold)), buffer_segment_pool_,
      common::ManagedPointer(thread_registry_));
  log_manager_->Start();

  timestamp_manager_ = new transaction::TimestampManager;
  deferred_action_manager_ = new transaction::DeferredActionManager(timestamp_manager_);
  txn_manager_ = new transaction::TransactionManager(timestamp_manager_, deferred_action_manager_, buffer_segment_pool_,
                                                     true, log_manager_);
  garbage_collector_ =
      new storage::GarbageCollector(timestamp_manager_, deferred_action_manager_, txn_manager_, DISABLED);
  gc_thread_ = new storage::GarbageCollectorThread(garbage_collector_,
                                                   std::chrono::milliseconds{type::TransientValuePeeker::PeekInteger(
                                                       param_map_.find(settings::Param::gc_interval)->second.value_)});

  thread_pool_ = new common::WorkerPool(static_cast<uint32_t>(type::TransientValuePeeker::PeekInteger(
                                            param_map_.find(settings::Param::num_worker_threads)->second.value_)),
                                        {});
  thread_pool_->Startup();

  block_store_ = new storage::BlockStore(static_cast<uint64_t>(type::TransientValuePeeker::PeekBigInt(
                                             param_map_.find(settings::Param::block_store_size)->second.value_)),
                                         static_cast<uint64_t>(type::TransientValuePeeker::PeekBigInt(
                                             param_map_.find(settings::Param::block_store_size)->second.value_)));

  // Initialize the catalog, depends on transaction manager, block store.
  catalog_ = std::make_unique<catalog::Catalog>(txn_manager_, block_store_);

  // Bootstrap the default database in the catalog.
  auto *bootstrap_txn = txn_manager_->BeginTransaction();
  catalog_->CreateDatabase(bootstrap_txn, catalog::DEFAULT_DATABASE, true);
  txn_manager_->Commit(bootstrap_txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Run the GC to flush it down to a clean system
  garbage_collector_->PerformGarbageCollection();
  garbage_collector_->PerformGarbageCollection();

  t_cop_ = new trafficcop::TrafficCop(common::ManagedPointer(txn_manager_), common::ManagedPointer(catalog_));
  connection_handle_factory_ = new network::ConnectionHandleFactory(common::ManagedPointer(t_cop_));

  command_factory_ = new network::PostgresCommandFactory;
  provider_ = new network::PostgresProtocolInterpreter::Provider(common::ManagedPointer(command_factory_));
  server_ =
      new network::TerrierServer(common::ManagedPointer(provider_), common::ManagedPointer(connection_handle_factory_),
                                 common::ManagedPointer(thread_registry_));

  LOG_INFO("Initialization complete");
}

void DBMain::Run() {
  running_ = true;
  server_->SetPort(static_cast<uint16_t>(
      type::TransientValuePeeker::PeekInteger(param_map_.find(settings::Param::port)->second.value_)));
  server_->RunServer();

  {
    std::unique_lock<std::mutex> lock(server_->RunningMutex());
    server_->RunningCV().wait(lock, [=] { return !(server_->Running()); });
  }

  // Server loop exited, begin cleaning up
  CleanUp();
}

void DBMain::ForceShutdown() {
  if (running_) {
    server_->StopServer();
  }
  CleanUp();
}

void DBMain::CleanUp() {
  catalog_->TearDown();
  delete gc_thread_;
  deferred_action_manager_->FullyPerformGC(garbage_collector_, log_manager_);
  main_stat_reg_->Shutdown(false);
  log_manager_->PersistAndStop();
  thread_pool_->Shutdown();
  LOG_INFO("Terrier has shut down.");
  LoggersUtil::ShutDown();
}

}  // namespace terrier
