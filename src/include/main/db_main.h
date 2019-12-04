#pragma once

#include <memory>
#include <unordered_map>
#include <utility>

#include "catalog/catalog.h"
#include "common/action_context.h"
#include "common/managed_pointer.h"
#include "common/stat_registry.h"
#include "common/worker_pool.h"
#include "metrics/metrics_manager.h"
#include "network/terrier_server.h"
#include "settings/settings_manager.h"
#include "settings/settings_param.h"
#include "storage/garbage_collector_thread.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_manager.h"

namespace terrier {

namespace settings {
class SettingsManager;
class SettingsTests;
class Callbacks;
}  // namespace settings

namespace metrics {
class MetricsTests;
}

namespace trafficcop {
class TrafficCopTests;
}

namespace storage {
class WriteAheadLoggingTests;
}

namespace common {
class DedicatedThreadRegistry;
}

/**
 * The DBMain Class holds all the singleton pointers. It has the full knowledge
 * of the whole database systems and serves as a global context of the system.
 * *Only the settings manager should be able to access the DBMain object.*
 */
class DBMain {
 public:
  explicit DBMain(std::unordered_map<settings::Param, settings::ParamInfo> &&param_map);
  ~DBMain();

  /**
   * Boots the traffic cop and networking layer, starts the server loop.
   * It will block until server shuts down.
   */
  void Run();

  /**
   * Shuts down the server.
   * It is worth noting that in normal cases, terrier will shut down and return from Run().
   * So, use this function only when you want to shutdown the server from code.
   * For example, in the end of unit tests when you want to shut down your test server.
   */
  void ForceShutdown();

 private:
  friend class settings::SettingsManager;
  friend class settings::SettingsTests;
  friend class settings::Callbacks;
  friend class metrics::MetricsTests;
  friend class trafficcop::TrafficCopTests;

  class TransactionLayer {
   public:
    TransactionLayer(const common::ManagedPointer<storage::RecordBufferSegmentPool> buffer_segment_pool,
                     const bool gc_enabled, const common::ManagedPointer<storage::LogManager> log_manager) {
      TERRIER_ASSERT(buffer_segment_pool != nullptr, "Need a buffer segment pool for Transaction layer.");
      timestamp_manager_ = std::make_unique<transaction::TimestampManager>();
      deferred_action_manager_ =
          std::make_unique<transaction::DeferredActionManager>(common::ManagedPointer(timestamp_manager_));
      txn_manager_ = std::make_unique<transaction::TransactionManager>(common::ManagedPointer(timestamp_manager_),
                                                                       common::ManagedPointer(deferred_action_manager_),
                                                                       buffer_segment_pool, gc_enabled, log_manager);
    }

    common::ManagedPointer<transaction::TimestampManager> GetTimestampManager() const {
      return common::ManagedPointer(timestamp_manager_);
    }

    common::ManagedPointer<transaction::DeferredActionManager> GetDeferredActionManager() const {
      return common::ManagedPointer(deferred_action_manager_);
    }

    common::ManagedPointer<transaction::TransactionManager> GetTransactionManager() const {
      return common::ManagedPointer(txn_manager_);
    }

   private:
    std::unique_ptr<transaction::TimestampManager> timestamp_manager_;
    std::unique_ptr<transaction::DeferredActionManager> deferred_action_manager_;
    std::unique_ptr<transaction::TransactionManager> txn_manager_;
  };

  class StorageLayer {
   public:
    StorageLayer(const common::ManagedPointer<TransactionLayer> txn_layer, const uint64_t block_store_size_limit,
                 const uint64_t block_store_reuse_limit, const bool use_gc) {
      garbage_collector_ = use_gc ? std::make_unique<storage::GarbageCollector>(
                                        txn_layer->GetTimestampManager(), txn_layer->GetDeferredActionManager(),
                                        txn_layer->GetTransactionManager(), DISABLED)
                                  : DISABLED;

      block_store_ = std::make_unique<storage::BlockStore>(block_store_size_limit, block_store_reuse_limit);
    }

    common::ManagedPointer<storage::GarbageCollector> GetGarbageCollector() const {
      return common::ManagedPointer(garbage_collector_);
    }

    common::ManagedPointer<storage::BlockStore> GetBlockStore() const { return common::ManagedPointer(block_store_); }

   private:
    std::unique_ptr<storage::GarbageCollector> garbage_collector_;
    std::unique_ptr<storage::BlockStore> block_store_;
  };

  class CatalogLayer {
   public:
    CatalogLayer(const common::ManagedPointer<TransactionLayer> txn_layer,
                 const common::ManagedPointer<StorageLayer> storage_layer,
                 const common::ManagedPointer<storage::LogManager> log_manager)
        : deferred_action_manager_(txn_layer->GetDeferredActionManager()),
          garbage_collector_(storage_layer->GetGarbageCollector()),
          log_manager_(log_manager) {
      TERRIER_ASSERT(deferred_action_manager_ != nullptr, "Catalog::TearDown() needs DeferredActionManager.");
      TERRIER_ASSERT(garbage_collector_ != nullptr, "Catalog::TearDown() needs GarbageCollector.");

      catalog_ = std::make_unique<catalog::Catalog>(txn_layer->GetTransactionManager(), storage_layer->GetBlockStore());

      // Bootstrap the default database in the catalog.
      auto *bootstrap_txn = txn_layer->GetTransactionManager()->BeginTransaction();
      catalog_->CreateDatabase(bootstrap_txn, catalog::DEFAULT_DATABASE, true);
      txn_layer->GetTransactionManager()->Commit(bootstrap_txn, transaction::TransactionUtil::EmptyCallback, nullptr);

      // Run the GC to get a clean system. This needs to be done before instantiating the GC thread
      // because the GC is not thread-safe
      deferred_action_manager_->FullyPerformGC(garbage_collector_, log_manager_);
    }

    ~CatalogLayer() {
      catalog_->TearDown();
      deferred_action_manager_->FullyPerformGC(garbage_collector_, log_manager_);
    }

    common::ManagedPointer<catalog::Catalog> GetCatalog() const { return common::ManagedPointer(catalog_); }

   private:
    const common::ManagedPointer<transaction::DeferredActionManager> deferred_action_manager_;
    const common::ManagedPointer<storage::GarbageCollector> garbage_collector_;
    const common::ManagedPointer<storage::LogManager> log_manager_;

    std::unique_ptr<catalog::Catalog> catalog_;
  };

  class TrafficCopLayer {
   public:
    TrafficCopLayer(const common::ManagedPointer<TransactionLayer> txn_layer,
                    const common::ManagedPointer<CatalogLayer> catalog_layer) {
      traffic_cop_ =
          std::make_unique<trafficcop::TrafficCop>(txn_layer->GetTransactionManager(), catalog_layer->GetCatalog());
    }

    common::ManagedPointer<trafficcop::TrafficCop> GetTrafficCop() const {
      return common::ManagedPointer(traffic_cop_);
    }

   private:
    std::unique_ptr<trafficcop::TrafficCop> traffic_cop_;
  };

  class NetworkLayer {
   public:
    NetworkLayer(const common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry,
                 const common::ManagedPointer<trafficcop::TrafficCop> traffic_cop) {
      connection_handle_factory_ = std::make_unique<network::ConnectionHandleFactory>(traffic_cop);
      command_factory_ = std::make_unique<network::PostgresCommandFactory>();
      provider_ =
          std::make_unique<network::PostgresProtocolInterpreter::Provider>(common::ManagedPointer(command_factory_));
      server_ = std::make_unique<network::TerrierServer>(
          common::ManagedPointer(provider_), common::ManagedPointer(connection_handle_factory_), thread_registry);
    }

    common::ManagedPointer<network::TerrierServer> GetServer() const { return common::ManagedPointer(server_); }

   private:
    std::unique_ptr<network::ConnectionHandleFactory> connection_handle_factory_;
    std::unique_ptr<network::PostgresCommandFactory> command_factory_;
    std::unique_ptr<network::ProtocolInterpreter::Provider> provider_;
    std::unique_ptr<network::TerrierServer> server_;
  };

 public:
  class Builder {
   public:
    std::unique_ptr<DBMain> Build() {
      auto db_main = std::make_unique<DBMain>(std::move(param_map_));

      auto settings_manager = std::make_unique<settings::SettingsManager>(common::ManagedPointer(db_main));

      std::unique_ptr<metrics::MetricsManager> metrics_manager = DISABLED;
      if (use_metrics_manager_) metrics_manager = std::make_unique<metrics::MetricsManager>();

      std::unique_ptr<common::DedicatedThreadRegistry> thread_registry = DISABLED;
      if (use_logging_ || use_network_)
        thread_registry = std::make_unique<common::DedicatedThreadRegistry>(common::ManagedPointer(metrics_manager));

      auto buffer_segment_pool = std::make_unique<storage::RecordBufferSegmentPool>(
          static_cast<uint64_t>(settings_manager->GetInt(settings::Param::record_buffer_segment_size)),
          static_cast<uint64_t>(settings_manager->GetInt(settings::Param::record_buffer_segment_reuse)));

      std::unique_ptr<storage::LogManager> log_manager = DISABLED;
      if (use_logging_) {
        log_manager = std::make_unique<storage::LogManager>(
            settings_manager->GetString(settings::Param::log_file_path),
            static_cast<uint64_t>(settings_manager->GetInt64(settings::Param::num_log_manager_buffers)),
            std::chrono::milliseconds{settings_manager->GetInt(settings::Param::log_serialization_interval)},
            std::chrono::milliseconds{settings_manager->GetInt(settings::Param::log_persist_interval)},
            static_cast<uint64_t>(settings_manager->GetInt64(settings::Param::log_persist_threshold)),
            common::ManagedPointer(buffer_segment_pool), common::ManagedPointer(thread_registry));
        log_manager->Start();
      }

      auto txn_layer = std::make_unique<TransactionLayer>(common::ManagedPointer(buffer_segment_pool), use_gc_,
                                                          common::ManagedPointer(log_manager));

      auto storage_layer = std::make_unique<StorageLayer>(
          common::ManagedPointer(txn_layer),
          static_cast<uint64_t>(settings_manager->GetInt64(settings::Param::block_store_size)),
          static_cast<uint64_t>(settings_manager->GetInt64(settings::Param::block_store_reuse)), use_gc_);

      std::unique_ptr<CatalogLayer> catalog_layer = DISABLED;
      if (use_catalog_) {
        TERRIER_ASSERT(use_gc_, "Catalog needs GarbageCollector.");
        catalog_layer =
            std::make_unique<CatalogLayer>(common::ManagedPointer(txn_layer), common::ManagedPointer(storage_layer),
                                           common::ManagedPointer(log_manager));
      }

      std::unique_ptr<storage::GarbageCollectorThread> gc_thread = DISABLED;
      if (use_gc_thread_) {
        TERRIER_ASSERT(use_gc_, "GarbageCollectorThread needs GarbageCollector.");
        gc_thread = std::make_unique<storage::GarbageCollectorThread>(
            storage_layer->GetGarbageCollector(),
            std::chrono::milliseconds{settings_manager->GetInt(settings::Param::gc_interval)});
      }

      std::unique_ptr<TrafficCopLayer> traffic_cop_layer = DISABLED;
      if (use_traffic_cop_) {
        TERRIER_ASSERT(use_catalog_, "TrafficCopLayer needs the CatalogLayer.");
        traffic_cop_layer =
            std::make_unique<TrafficCopLayer>(common::ManagedPointer(txn_layer), common::ManagedPointer(catalog_layer));
      }

      std::unique_ptr<NetworkLayer> network_layer = DISABLED;
      if (use_network_) {
        TERRIER_ASSERT(use_traffic_cop_, "NetworkLayer needs TrafficCopLayer.");
        network_layer =
            std::make_unique<NetworkLayer>(common::ManagedPointer(thread_registry), traffic_cop_layer->GetTrafficCop());
      }

      db_main->settings_manager_ = std::move(settings_manager);
      db_main->metrics_manager_ = std::move(metrics_manager);
      db_main->thread_registry_ = std::move(thread_registry);
      db_main->buffer_segment_pool_ = std::move(buffer_segment_pool);
      db_main->log_manager_ = std::move(log_manager);
      db_main->txn_layer_ = std::move(txn_layer);
      db_main->storage_layer_ = std::move(storage_layer);
      db_main->catalog_layer_ = std::move(catalog_layer);
      db_main->gc_thread_ = std::move(gc_thread);
      db_main->traffic_cop_layer_ = std::move(traffic_cop_layer);
      db_main->network_layer_ = std::move(network_layer);

      return db_main;
    }

    Builder &SetSettingsParameterMap(std::unordered_map<settings::Param, settings::ParamInfo> &&param_map) {
      param_map_ = std::move(param_map);
      return *this;
    }

    Builder &SetUseSettingsManager(const bool value) {
      use_settings_manager_ = value;
      return *this;
    }

    Builder &SetUseMetricsManager(const bool value) {
      use_metrics_manager_ = value;
      return *this;
    }

    Builder &SetUseLogging(const bool value) {
      use_logging_ = value;
      return *this;
    }

    Builder &SetUseGC(const bool value) {
      use_gc_ = value;
      return *this;
    }

    Builder &SetUseCatalog(const bool value) {
      use_catalog_ = value;
      return *this;
    }

    Builder &SetUseGCThread(const bool value) {
      use_gc_thread_ = value;
      return *this;
    }

    Builder &SetUseTrafficCop(const bool value) {
      use_traffic_cop_ = value;
      return *this;
    }

    Builder &SetUseNetwork(const bool value) {
      use_network_ = value;
      return *this;
    }

   private:
    std::unordered_map<settings::Param, settings::ParamInfo> param_map_;
    bool use_settings_manager_ = false;
    bool use_metrics_manager_ = false;
    bool use_logging_ = false;
    bool use_gc_ = false;
    bool use_catalog_ = false;
    bool use_gc_thread_ = false;
    bool use_traffic_cop_ = false;
    bool use_network_ = false;
  };

 private:
  bool running_ = false;
  std::unordered_map<settings::Param, settings::ParamInfo> param_map_;
  std::unique_ptr<settings::SettingsManager> settings_manager_;
  std::unique_ptr<metrics::MetricsManager> metrics_manager_;
  std::unique_ptr<common::DedicatedThreadRegistry> thread_registry_;
  std::unique_ptr<storage::RecordBufferSegmentPool> buffer_segment_pool_;
  std::unique_ptr<storage::LogManager> log_manager_;
  std::unique_ptr<TransactionLayer> txn_layer_;
  std::unique_ptr<StorageLayer> storage_layer_;
  std::unique_ptr<CatalogLayer> catalog_layer_;
  std::unique_ptr<storage::GarbageCollectorThread> gc_thread_;
  std::unique_ptr<TrafficCopLayer> traffic_cop_layer_;
  std::unique_ptr<NetworkLayer> network_layer_;
};

}  // namespace terrier
