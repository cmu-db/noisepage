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

  settings_manager_ = std::make_unique<settings::SettingsManager>(this);

  metrics_manager_ = std::make_unique<metrics::MetricsManager>();

  thread_registry_ = std::make_unique<common::DedicatedThreadRegistry>(common::ManagedPointer(metrics_manager_));

  buffer_segment_pool_ = std::make_unique<storage::RecordBufferSegmentPool>(
      type::TransientValuePeeker::PeekInteger(
          param_map_.find(settings::Param::record_buffer_segment_size)->second.value_),
      type::TransientValuePeeker::PeekInteger(
          param_map_.find(settings::Param::record_buffer_segment_reuse)->second.value_));

  log_manager_ = std::make_unique<storage::LogManager>(
      settings_manager_->GetString(settings::Param::log_file_path),
      static_cast<uint64_t>(settings_manager_->GetInt64(settings::Param::num_log_manager_buffers)),
      std::chrono::milliseconds{settings_manager_->GetInt(settings::Param::log_serialization_interval)},
      std::chrono::milliseconds{settings_manager_->GetInt(settings::Param::log_persist_interval)},
      static_cast<uint64_t>(settings_manager_->GetInt64(settings::Param::log_persist_threshold)),
      common::ManagedPointer(buffer_segment_pool_), common::ManagedPointer(thread_registry_));
  log_manager_->Start();

  txn_layer_ = std::make_unique<TransactionLayer>(common::ManagedPointer(buffer_segment_pool_), true,
                                                  common::ManagedPointer(log_manager_));

  garbage_collector_ = std::make_unique<storage::GarbageCollector>(txn_layer_->GetTimestampManager(),
                                                                   txn_layer_->GetDeferredActionManager(),
                                                                   txn_layer_->GetTransactionManager(), DISABLED);

  block_store_ = std::make_unique<storage::BlockStore>(
      static_cast<uint64_t>(
          type::TransientValuePeeker::PeekBigInt(param_map_.find(settings::Param::block_store_size)->second.value_)),
      static_cast<uint64_t>(
          type::TransientValuePeeker::PeekBigInt(param_map_.find(settings::Param::block_store_size)->second.value_)));

  catalog_layer_ =
      std::make_unique<CatalogLayer>(common::ManagedPointer(txn_layer_), common::ManagedPointer(block_store_),
                                     common::ManagedPointer(garbage_collector_), common::ManagedPointer(log_manager_));

  gc_thread_ = std::make_unique<storage::GarbageCollectorThread>(
      common::ManagedPointer(garbage_collector_), std::chrono::milliseconds{type::TransientValuePeeker::PeekInteger(
                                                      param_map_.find(settings::Param::gc_interval)->second.value_)});

  traffic_cop_layer_ =
      std::make_unique<TrafficCopLayer>(common::ManagedPointer(txn_layer_), common::ManagedPointer(catalog_layer_));

  network_layer_ =
      std::make_unique<NetworkLayer>(common::ManagedPointer(thread_registry_), traffic_cop_layer_->GetTrafficCop());

  LOG_INFO("Initialization complete");
}

void DBMain::Run() {
  running_ = true;
  network_layer_->GetServer()->SetPort(static_cast<uint16_t>(
      type::TransientValuePeeker::PeekInteger(param_map_.find(settings::Param::port)->second.value_)));
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

DBMain::~DBMain() {
  ForceShutdown();
  LoggersUtil::ShutDown();
}

}  // namespace terrier
