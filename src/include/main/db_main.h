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
  /**
   * The constructor of DBMain
   * This function boots the backend components.
   * It initializes the following components in the following order:
   *    Debug loggers
   *    Stats registry (counters)
   *    Buffer segment pools
   *    Transaction manager
   *    Garbage collector thread
   *    Catalog
   *    Settings manager
   *    Log manager
   *    Worker pool
   * @param param_map a map stores setting values
   */
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
  std::unordered_map<settings::Param, settings::ParamInfo> param_map_;

  bool running_ = false;

  //////////

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

  class CatalogLayer {
   public:
    CatalogLayer(const common::ManagedPointer<TransactionLayer> txn_layer,
                 const common::ManagedPointer<storage::BlockStore> block_store,
                 const common::ManagedPointer<storage::GarbageCollector> garbage_collector,
                 const common::ManagedPointer<storage::LogManager> log_manager)
        : deferred_action_manager_(txn_layer->GetDeferredActionManager()),
          garbage_collector_(garbage_collector),
          log_manager_(log_manager) {
      TERRIER_ASSERT(deferred_action_manager_ != nullptr, "Catalog::TearDown() needs DeferredActionManager.");
      TERRIER_ASSERT(garbage_collector_ != nullptr, "Catalog::TearDown() needs GarbageCollector.");

      catalog_ = std::make_unique<catalog::Catalog>(txn_layer->GetTransactionManager(), block_store);

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

  std::unique_ptr<settings::SettingsManager> settings_manager_;
  std::unique_ptr<metrics::MetricsManager> metrics_manager_;
  std::unique_ptr<common::DedicatedThreadRegistry> thread_registry_;
  std::unique_ptr<storage::RecordBufferSegmentPool> buffer_segment_pool_;
  std::unique_ptr<storage::LogManager> log_manager_;
  std::unique_ptr<TransactionLayer> txn_layer_;
  std::unique_ptr<storage::GarbageCollector> garbage_collector_;
  std::unique_ptr<storage::BlockStore> block_store_;
  std::unique_ptr<CatalogLayer> catalog_layer_;
  std::unique_ptr<storage::GarbageCollectorThread> gc_thread_;
  std::unique_ptr<TrafficCopLayer> traffic_cop_layer_;
  std::unique_ptr<NetworkLayer> network_layer_;
};

}  // namespace terrier
