#include "main/db_main.h"

#include <memory>
#include <unordered_map>
#include <utility>

#include "catalog/catalog.h"
#include "loggers/loggers_util.h"
#include "settings/settings_manager.h"
#include "settings/settings_param.h"
#include "storage/garbage_collector_thread.h"
#include "traffic_cop/terrier_engine.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"

namespace terrier {

DBMain::DBMain(std::unordered_map<settings::Param, settings::ParamInfo> &&param_map)
    : param_map_(std::move(param_map)) {
  LoggersUtil::Initialize(false);

  // Create statistics registry.
  main_stat_reg_ = std::make_unique<common::StatisticsRegistry>();

  // Create buffer segment pool.
  buffer_segment_pool_ = new storage::RecordBufferSegmentPool(
      type::TransientValuePeeker::PeekInteger(
          param_map_.find(settings::Param::record_buffer_segment_size)->second.value_),
      type::TransientValuePeeker::PeekInteger(
          param_map_.find(settings::Param::record_buffer_segment_reuse)->second.value_));

  // Create settings manager.
  settings_manager_ = new settings::SettingsManager(this);

  // Create metrics manager.
  metrics_manager_ = new metrics::MetricsManager;

  // Create thread registry, depends on metrics manager.
  thread_registry_ = new common::DedicatedThreadRegistry(common::ManagedPointer(metrics_manager_));

  // Create and start log manager, depends on buffer segment pool, thread registry.
  log_manager_ = new storage::LogManager(
      settings_manager_->GetString(settings::Param::log_file_path),
      settings_manager_->GetInt(settings::Param::num_log_manager_buffers),
      std::chrono::milliseconds{settings_manager_->GetInt(settings::Param::log_serialization_interval)},
      std::chrono::milliseconds{settings_manager_->GetInt(settings::Param::log_persist_interval)},
      settings_manager_->GetInt(settings::Param::log_persist_threshold), buffer_segment_pool_,
      common::ManagedPointer(thread_registry_));
  log_manager_->Start();

  // Create timestamp manager.
  timestamp_manager_ = new transaction::TimestampManager;

  // Create deferred action manager.
  deferred_action_manager_ = std::make_unique<transaction::DeferredActionManager>(timestamp_manager_);

  // Create transaction manager, depends on timestamp manager, buffer segment pool, log manager.
  // It _should_ eventually depend on deferred events manager, but we're not there yet.
  txn_manager_ = new transaction::TransactionManager(timestamp_manager_, deferred_action_manager_.get(),
                                                     buffer_segment_pool_, true, log_manager_);

  // Create garbage collector, depends on timestamp manager, transaction manager.
  // It _should_ eventually depend on deferred events manager, but we're not there yet.
  garbage_collector_ =
      new storage::GarbageCollector(timestamp_manager_, deferred_action_manager_.get(), txn_manager_, DISABLED);
  // Creates a dedicated garbage collector thread.
  gc_thread_ = new storage::GarbageCollectorThread(garbage_collector_,
                                                   std::chrono::milliseconds{type::TransientValuePeeker::PeekInteger(
                                                       param_map_.find(settings::Param::gc_interval)->second.value_)});

  // Create thread pool.
  thread_pool_ = new common::WorkerPool(
      type::TransientValuePeeker::PeekInteger(param_map_.find(settings::Param::num_worker_threads)->second.value_), {});
  thread_pool_->Startup();

  // Initialize the catalog, depends on transaction manager, block store.
  catalog_ = std::make_unique<catalog::Catalog>(txn_manager_, &block_store_);

  // Bootstrap the default database in the catalog.
  auto *bootstrap_txn = txn_manager_->BeginTransaction();
  default_database_oid_ = catalog_->CreateDatabase(bootstrap_txn, catalog::DEFAULT_DATABASE, true);
  txn_manager_->Commit(bootstrap_txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Create terrier engine, depends on parser, transaction manager, catalog.
  terrier_engine_ = std::make_unique<trafficcop::TerrierEngine>(
      common::ManagedPointer(parser_), common::ManagedPointer(txn_manager_), common::ManagedPointer(catalog_));

  // Create traffic cop, depends on terrier engine.
  t_cop_ = new trafficcop::TrafficCop(default_database_oid_, common::ManagedPointer(terrier_engine_));

  // Create connection handle factory, depends on traffic cop.
  connection_handle_factory_ = new network::ConnectionHandleFactory(common::ManagedPointer(t_cop_));

  // Create server.
  command_factory_ = new network::PostgresCommandFactory;
  provider_ = new network::PostgresProtocolInterpreter::Provider(common::ManagedPointer(command_factory_));
  server_ =
      new network::TerrierServer(common::ManagedPointer(provider_), common::ManagedPointer(connection_handle_factory_),
                                 common::ManagedPointer(thread_registry_));

  LOG_INFO("Initialization complete");
}

DBMain::~DBMain() {
  ForceShutdown();
  // TODO(Matt): might as well make these std::unique_ptr, but then will need to refactor other classes to take
  // ManagedPointers unless we want a bunch of .get()s, which sounds like a future PR
  delete gc_thread_;
  delete metrics_manager_;
  delete garbage_collector_;
  delete settings_manager_;
  delete txn_manager_;
  delete timestamp_manager_;
  delete buffer_segment_pool_;
  delete thread_pool_;
  delete log_manager_;
  delete connection_handle_factory_;
  delete server_;
  delete command_factory_;
  delete provider_;
  delete t_cop_;
  delete thread_registry_;
}

void DBMain::Run() {
  running_ = true;
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
  if (running_) {
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
