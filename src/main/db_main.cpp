#include "main/db_main.h"
#include <settings/settings_param.h>
#include <memory>
#include "loggers/loggers_util.h"
#include "settings/settings_manager.h"
#include "storage/garbage_collector_thread.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"

namespace terrier {

void DBMain::Init() {
  LoggersUtil::Initialize(false);

  // initialize stat registry
  main_stat_reg_ = std::make_shared<common::StatisticsRegistry>();

  // create the global transaction mgr
  buffer_segment_pool_ = new storage::RecordBufferSegmentPool(
      type::TransientValuePeeker::PeekInteger(
          param_map_.find(settings::Param::record_buffer_segment_size)->second.value_),
      type::TransientValuePeeker::PeekInteger(
          param_map_.find(settings::Param::record_buffer_segment_reuse)->second.value_));
  txn_manager_ = new transaction::TransactionManager(buffer_segment_pool_, true, nullptr);
  gc_thread_ = new storage::GarbageCollectorThread(txn_manager_,
                                                   std::chrono::milliseconds{type::TransientValuePeeker::PeekInteger(
                                                       param_map_.find(settings::Param::gc_interval)->second.value_)});
  transaction::TransactionContext *txn = txn_manager_->BeginTransaction();
  settings_manager_ = new settings::SettingsManager(this);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Create LogManager
  log_manager_ = new storage::LogManager(
      settings_manager_->GetString(settings::Param::log_file_path).c_str(),
      settings_manager_->GetInt(settings::Param::num_log_manager_buffers),
      std::chrono::milliseconds{settings_manager_->GetInt(settings::Param::log_serialization_interval)},
      std::chrono::milliseconds{settings_manager_->GetInt(settings::Param::log_flushing_interval)},
      buffer_segment_pool_);
  log_manager_->Start();

  thread_pool_ = new common::WorkerPool(
      type::TransientValuePeeker::PeekInteger(param_map_.find(settings::Param::num_worker_threads)->second.value_), {});
  thread_pool_->Startup();

  t_cop_ = new terrier::traffic_cop::TrafficCop;
  command_factory_ = new terrier::network::CommandFactory;
  connection_handle_factory_ = new terrier::network::ConnectionHandleFactory(t_cop_, command_factory_);
  server_ = new terrier::network::TerrierServer(connection_handle_factory_);

  LOG_INFO("Initialization complete");

  initialized = true;
}

void DBMain::Run() {
  running = true;
  server_->SetPort(static_cast<int16_t>(
      type::TransientValuePeeker::PeekInteger(param_map_.find(settings::Param::port)->second.value_)));
  server_->SetupServer().ServerLoop();

  // server loop exited, begin cleaning up
  CleanUp();
}

void DBMain::ForceShutdown() {
  if (running) {
    server_->Close();
  }
  CleanUp();
}

void DBMain::CleanUp() {
  main_stat_reg_->Shutdown(false);
  LoggersUtil::ShutDown();
  log_manager_->PersistAndStop();
  thread_pool_->Shutdown();
  LOG_INFO("Terrier has shut down.");
}

}  // namespace terrier
