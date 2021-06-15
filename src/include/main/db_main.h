#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "catalog/catalog.h"
#include "common/action_context.h"
#include "common/dedicated_thread_registry.h"
#include "common/managed_pointer.h"
#include "messenger/messenger.h"
#include "metrics/metrics_defs.h"
#include "metrics/metrics_thread.h"
#include "network/connection_handle_factory.h"
#include "network/noisepage_server.h"
#include "network/postgres/postgres_command_factory.h"
#include "network/postgres/postgres_protocol_interpreter.h"
#include "optimizer/statistics/stats_storage.h"
#include "replication/primary_replication_manager.h"
#include "replication/replica_replication_manager.h"
#include "self_driving/model_server/model_server_manager.h"
#include "self_driving/planning/pilot.h"
#include "self_driving/planning/pilot_thread.h"
#include "settings/settings_manager.h"
#include "settings/settings_param.h"
#include "storage/garbage_collector_thread.h"
#include "storage/recovery/recovery_manager.h"
#include "task/task_manager.h"
#include "traffic_cop/traffic_cop.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_manager.h"
#include "util/query_exec_util.h"

namespace noisepage {

namespace settings {
class SettingsManager;
class Callbacks;
}  // namespace settings

namespace trafficcop {
class TrafficCopTests;
}

/**
 * The DBMain Class holds all the singleton pointers. It is mostly useful for coordinating dependencies between
 * components, particularly at destruction since there are implicit dependencies in the interaction of various
 * components.
 * Use the Builder to request the components that you want, and it should detect if you missed a dependency at
 * construction on Build().
 * DBMain should never be passed as a dependency to any components. Component dependencies should be explicit
 * constructor arguments. This also means that the Getters for components on DBMain are really only meant for testing
 * purposes, since the main() function's scope (either in a benchmark, test, or the DBMS executable) is the only scope
 * with access to the DBMain object.
 */
class DBMain {
 public:
  ~DBMain();

  /**
   * Boots the traffic cop and networking layer, starts the server loop.
   * It will block until server shuts down.
   */
  void Run();

  /**
   * Function loads a startup.sql file specified by the startup_ddl_path
   * settings parameter. The file is treated as a file of DDL commands
   * that are then executed.
   *
   * Function assumes the following:
   * - Each SQL command is a DDL statement
   * - Each line is either blank, a comment, or a SQL command to run
   * - A comment is any line that starts with '--'
   * - A SQL command must fit on a single line
   */
  void TryLoadStartupDDL();

  /**
   * Shuts down the server.
   * It is worth noting that in normal cases, noisepage will shut down and return from Run().
   * So, use this function only when you want to shutdown the server from code.
   * For example, in the end of unit tests when you want to shut down your test server.
   */
  void ForceShutdown();

  /**
   * TimestampManager, DeferredActionManager, and TransactionManager
   */
  class TransactionLayer {
   public:
    /**
     * @param buffer_segment_pool non-null required component
     * @param gc_enabled argument to the TransactionManager
     * @param wal_async_commit_enable true if commit callbacks should be invoked by TransactionManager at commit time
     * rather than waiting until durable on disk and being invoked by the WAL worker. Doesn't make sense to set to true
     * if WAL is not enabled.
     * @param log_manager argument to the TransactionManager
     */
    TransactionLayer(const common::ManagedPointer<storage::RecordBufferSegmentPool> buffer_segment_pool,
                     const bool gc_enabled, const bool wal_async_commit_enable,
                     const common::ManagedPointer<storage::LogManager> log_manager) {
      NOISEPAGE_ASSERT(buffer_segment_pool != nullptr, "Need a buffer segment pool for Transaction layer.");
      NOISEPAGE_ASSERT(!wal_async_commit_enable || (wal_async_commit_enable && log_manager != DISABLED),
                       "Doesn't make sense to enable async commit without enabling logging.");
      timestamp_manager_ = std::make_unique<transaction::TimestampManager>();
      deferred_action_manager_ =
          std::make_unique<transaction::DeferredActionManager>(common::ManagedPointer(timestamp_manager_));
      txn_manager_ = std::make_unique<transaction::TransactionManager>(
          common::ManagedPointer(timestamp_manager_), common::ManagedPointer(deferred_action_manager_),
          buffer_segment_pool, gc_enabled, wal_async_commit_enable, log_manager);
    }

    /**
     * @return ManagedPointer to the component
     */
    common::ManagedPointer<transaction::TimestampManager> GetTimestampManager() const {
      return common::ManagedPointer(timestamp_manager_);
    }

    /**
     * @return ManagedPointer to the component
     */
    common::ManagedPointer<transaction::DeferredActionManager> GetDeferredActionManager() const {
      return common::ManagedPointer(deferred_action_manager_);
    }

    /**
     * @return ManagedPointer to the component
     */
    common::ManagedPointer<transaction::TransactionManager> GetTransactionManager() const {
      return common::ManagedPointer(txn_manager_);
    }

   private:
    // Order matters here for destruction order
    std::unique_ptr<transaction::TimestampManager> timestamp_manager_;
    std::unique_ptr<transaction::DeferredActionManager> deferred_action_manager_;
    std::unique_ptr<transaction::TransactionManager> txn_manager_;
  };

  /**
   * BlockStore and GarbageCollector
   * Additionally, a shared empty buffer queue that people pull from and push to.
   */
  class StorageLayer {
   public:
    /**
     * @param txn_layer arguments to the GarbageCollector
     * @param block_store_size_limit argument to the BlockStore
     * @param block_store_reuse_limit argument to the BlockStore
     * @param use_gc enable GarbageCollector
     * @param log_manager needed for safe destruction of StorageLayer
     * @param empty_buffer_queue The common buffer queue that all empty buffers are pulled from and returned to.
     */
    StorageLayer(const common::ManagedPointer<TransactionLayer> txn_layer, const uint64_t block_store_size_limit,
                 const uint64_t block_store_reuse_limit, const bool use_gc,
                 const common::ManagedPointer<storage::LogManager> log_manager,
                 std::unique_ptr<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue)
        : empty_buffer_queue_(std::move(empty_buffer_queue)),
          deferred_action_manager_(txn_layer->GetDeferredActionManager()),
          log_manager_(log_manager) {
      if (use_gc)
        garbage_collector_ = std::make_unique<storage::GarbageCollector>(txn_layer->GetTimestampManager(),
                                                                         txn_layer->GetDeferredActionManager(),
                                                                         txn_layer->GetTransactionManager(), DISABLED);

      block_store_ = std::make_unique<storage::BlockStore>(block_store_size_limit, block_store_reuse_limit);
    }

    ~StorageLayer() {
      if (garbage_collector_ != DISABLED) {
        // flush the system
        deferred_action_manager_->FullyPerformGC(common::ManagedPointer(garbage_collector_), log_manager_);
      }
      if (log_manager_ != DISABLED) {
        // stop the LogManager and make sure all buffers are released
        log_manager_->PersistAndStop();
      }
    }

    /**
     * @return ManagedPointer to the component, can be nullptr if disabled
     */
    common::ManagedPointer<storage::GarbageCollector> GetGarbageCollector() const {
      return common::ManagedPointer(garbage_collector_);
    }

    /**
     * @return ManagedPointer to the component
     */
    common::ManagedPointer<storage::BlockStore> GetBlockStore() const { return common::ManagedPointer(block_store_); }

    /**
     * @return A pointer to the empty buffer queue that is shared by separate components of the system.
     *         Currently, the buffers are shared by LogSerializerTask and ReplicationManager.
     */
    common::ManagedPointer<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> GetEmptyBufferQueue() {
      return common::ManagedPointer(empty_buffer_queue_);
    }

   private:
    // Order currently does not matter in this layer
    std::unique_ptr<storage::BlockStore> block_store_;
    std::unique_ptr<storage::GarbageCollector> garbage_collector_;
    std::unique_ptr<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>> empty_buffer_queue_;

    // External dependencies for this layer
    const common::ManagedPointer<transaction::DeferredActionManager> deferred_action_manager_;
    const common::ManagedPointer<storage::LogManager> log_manager_;
  };

  /**
   * Catalog, but needs a layer due to some complex teardown logic
   */
  class CatalogLayer {
   public:
    /**
     * @param txn_layer arguments to the Catalog
     * @param storage_layer arguments to the Catalog
     * @param log_manager needed for safe destruction of CatalogLayer if logging is enabled
     * @param create_default_database bootstrap the default database, false is used when recovering the Catalog
     */
    CatalogLayer(const common::ManagedPointer<TransactionLayer> txn_layer,
                 const common::ManagedPointer<StorageLayer> storage_layer,
                 const common::ManagedPointer<storage::LogManager> log_manager, const bool create_default_database)
        : deferred_action_manager_(txn_layer->GetDeferredActionManager()),
          garbage_collector_(storage_layer->GetGarbageCollector()),
          log_manager_(log_manager) {
      NOISEPAGE_ASSERT(garbage_collector_ != DISABLED, "Required component missing.");

      catalog_ = std::make_unique<catalog::Catalog>(txn_layer->GetTransactionManager(), storage_layer->GetBlockStore(),
                                                    garbage_collector_);

      // Bootstrap the default database in the catalog.
      if (create_default_database) {
        auto *bootstrap_txn = txn_layer->GetTransactionManager()->BeginTransaction();
        bootstrap_txn->SetReplicationPolicy(transaction::ReplicationPolicy::DISABLE);
        catalog_->CreateDatabase(common::ManagedPointer(bootstrap_txn), catalog::DEFAULT_DATABASE, true);
        txn_layer->GetTransactionManager()->Commit(bootstrap_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      }

      // Run the GC to get a clean system. This needs to be done before instantiating the GC thread
      // because the GC is not thread-safe
      deferred_action_manager_->FullyPerformGC(garbage_collector_, log_manager_);
    }

    ~CatalogLayer() {
      // Ensure all user-driven transactions are fully flushed and GC'd
      deferred_action_manager_->FullyPerformGC(common::ManagedPointer(garbage_collector_), log_manager_);
      // Identify and schedule deletion of remaining tables and objects (generates txns and deferred actions).
      // Specifically, this will generate the following (Note: these transactions are read-only and there are no logical
      // deletes of the data as that would cause replicas to replay :
      //
      // CatalogTearDown:
      //   BEGIN
      //     For each database:
      //       DEFER {
      //         database.TearDown()
      //         delete database
      //       }
      //     DEFER deletion of pg_database objects
      //   COMMIT
      //
      // DatabaseTearDown:
      //   BEGIN
      //     For each stored object:
      //       DEFER deletion of object
      //   COMMIT
      //
      // TODO(John): We could eagerly execute the database.TearDown() and delete calls, but this would force us into
      // sequential execution of TearDown when we could potentially multithread the table scans of each database catalog
      // using DAF.
      catalog_->TearDown();
      // Ensure these resources are properly released.
      deferred_action_manager_->FullyPerformGC(common::ManagedPointer(garbage_collector_), log_manager_);
    }

    /**
     * @return ManagedPointer to the component
     */
    common::ManagedPointer<catalog::Catalog> GetCatalog() const { return common::ManagedPointer(catalog_); }

   private:
    // Order currently does not matter in this layer
    std::unique_ptr<catalog::Catalog> catalog_;

    // External dependencies for this layer
    const common::ManagedPointer<transaction::DeferredActionManager> deferred_action_manager_;
    const common::ManagedPointer<storage::GarbageCollector> garbage_collector_;
    const common::ManagedPointer<storage::LogManager> log_manager_;
  };

  /**
   * ConnectionHandleFactory, CommandFactory, ProtocolInterpreterProvider, Server
   */
  class NetworkLayer {
   public:
    /**
     * @param thread_registry argument to the TerrierServer
     * @param traffic_cop argument to the ConnectionHandleFactor
     * @param port argument to TerrierServer
     * @param connection_thread_count argument to TerrierServer
     * @param socket_directory argument to TerrierServer
     */
    NetworkLayer(const common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry,
                 const common::ManagedPointer<trafficcop::TrafficCop> traffic_cop, const uint16_t port,
                 const uint16_t connection_thread_count, const std::string &socket_directory) {
      connection_handle_factory_ = std::make_unique<network::ConnectionHandleFactory>(traffic_cop);
      command_factory_ = std::make_unique<network::PostgresCommandFactory>();
      provider_ =
          std::make_unique<network::PostgresProtocolInterpreter::Provider>(common::ManagedPointer(command_factory_));
      server_ = std::make_unique<network::TerrierServer>(
          common::ManagedPointer(provider_), common::ManagedPointer(connection_handle_factory_), thread_registry, port,
          connection_thread_count, socket_directory);
    }

    /**
     * @return ManagedPointer to the component
     */
    common::ManagedPointer<network::TerrierServer> GetServer() const { return common::ManagedPointer(server_); }

   private:
    // Order matters here for destruction order
    std::unique_ptr<network::ConnectionHandleFactory> connection_handle_factory_;
    std::unique_ptr<network::PostgresCommandFactory> command_factory_;
    std::unique_ptr<network::ProtocolInterpreterProvider> provider_;
    std::unique_ptr<network::TerrierServer> server_;
  };

  /**
   * Currently doesn't hold any objects. The constructor and destructor are just used to orchestrate the setup and
   * teardown for TPL.
   */
  class ExecutionLayer {
   public:
    /**
     * @param bytecode_handlers_path path to the bytecode handlers bitcode file
     */
    explicit ExecutionLayer(const std::string &bytecode_handlers_path);
    ~ExecutionLayer();
  };

  /** Create a Messenger. */
  class MessengerLayer {
   public:
    /**
     * Instantiate and register a Messenger.
     * @param thread_registry       The DedicatedThreadRegistry that the Messenger will be registered to.
     * @param messenger_port        The port on which the Messenger will listen by default.
     * @param messenger_identity    The name by which this Messenger instance will be known.
     */
    explicit MessengerLayer(const common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry,
                            const uint16_t messenger_port, const std::string &messenger_identity)
        : messenger_manager_(
              std::make_unique<messenger::MessengerManager>(thread_registry, messenger_port, messenger_identity)) {}

    /** Destructor. */
    ~MessengerLayer() = default;

    /** @return The Messenger. */
    common::ManagedPointer<messenger::Messenger> GetMessenger() const { return messenger_manager_->GetMessenger(); }

   private:
    std::unique_ptr<messenger::MessengerManager> messenger_manager_;
  };

  /**
   * Creates DBMain objects
   */
  class Builder {
   public:
    /**
     * Validate configuration and construct requested DBMain.
     * @return DBMain that you get to own. YES, YOU! We're just giving away DBMains over here!
     */
    std::unique_ptr<DBMain> Build() {
      // Order matters through the Build() function and reflects the dependency ordering. It should match the member
      // ordering so things get destructed in the right order.
      auto db_main = std::make_unique<DBMain>();

      std::unique_ptr<settings::SettingsManager> settings_manager =
          use_settings_manager_ ? BootstrapSettingsManager(common::ManagedPointer(db_main)) : DISABLED;

      std::unique_ptr<metrics::MetricsManager> metrics_manager = DISABLED;
      if (use_metrics_) metrics_manager = BootstrapMetricsManager();

      std::unique_ptr<optimizer::StatsStorage> stats_storage = DISABLED;
      if (use_stats_storage_) {
        stats_storage = std::make_unique<optimizer::StatsStorage>();
      }

      std::unique_ptr<common::DedicatedThreadRegistry> thread_registry = DISABLED;
      if (use_thread_registry_ || use_logging_ || use_network_)
        thread_registry = std::make_unique<common::DedicatedThreadRegistry>(common::ManagedPointer(metrics_manager));

      auto buffer_segment_pool =
          std::make_unique<storage::RecordBufferSegmentPool>(record_buffer_segment_size_, record_buffer_segment_reuse_);

      std::unique_ptr<MessengerLayer> messenger_layer = DISABLED;
      std::unique_ptr<replication::ReplicationManager> replication_manager = DISABLED;
      if (use_messenger_) {
        messenger_layer = std::make_unique<MessengerLayer>(common::ManagedPointer(thread_registry), messenger_port_,
                                                           network_identity_);
      }

      auto empty_buffer_queue = std::make_unique<common::ConcurrentBlockingQueue<storage::BufferedLogWriter *>>();

      if (use_replication_) {
        NOISEPAGE_ASSERT(use_messenger_, "Replication uses the messenger subsystem.");
        NOISEPAGE_ASSERT(use_logging_, "Replication uses logging.");
        if (network_identity_ == "primary") {
          replication_manager = std::make_unique<replication::PrimaryReplicationManager>(
              messenger_layer->GetMessenger(), network_identity_, replication_port_, replication_hosts_path_,
              common::ManagedPointer(empty_buffer_queue));
        } else {
          replication_manager = std::make_unique<replication::ReplicaReplicationManager>(
              messenger_layer->GetMessenger(), network_identity_, replication_port_, replication_hosts_path_,
              common::ManagedPointer(empty_buffer_queue));
        }
      }

      std::unique_ptr<storage::LogManager> log_manager = DISABLED;
      if (use_logging_) {
        auto rep_manager_ptr = network_identity_ == "primary"
                                   ? common::ManagedPointer(replication_manager)
                                         .CastManagedPointerTo<replication::PrimaryReplicationManager>()
                                   : nullptr;
        log_manager = std::make_unique<storage::LogManager>(
            wal_file_path_, wal_num_buffers_, std::chrono::microseconds{wal_serialization_interval_},
            std::chrono::microseconds{wal_persist_interval_}, wal_persist_threshold_,
            common::ManagedPointer(buffer_segment_pool), common::ManagedPointer(empty_buffer_queue), rep_manager_ptr,
            common::ManagedPointer(thread_registry));
        log_manager->Start();
      }

      auto txn_layer =
          std::make_unique<TransactionLayer>(common::ManagedPointer(buffer_segment_pool), use_gc_,
                                             wal_async_commit_enable_, common::ManagedPointer(log_manager));

      auto storage_layer =
          std::make_unique<StorageLayer>(common::ManagedPointer(txn_layer), block_store_size_, block_store_reuse_,
                                         use_gc_, common::ManagedPointer(log_manager), std::move(empty_buffer_queue));

      std::unique_ptr<CatalogLayer> catalog_layer = DISABLED;
      if (use_catalog_) {
        NOISEPAGE_ASSERT(use_gc_ && storage_layer->GetGarbageCollector() != DISABLED,
                         "Catalog needs GarbageCollector.");
        catalog_layer =
            std::make_unique<CatalogLayer>(common::ManagedPointer(txn_layer), common::ManagedPointer(storage_layer),
                                           common::ManagedPointer(log_manager), create_default_database_);
      }

      // Instantiate the task manager
      std::unique_ptr<util::QueryExecUtil> query_exec_util = DISABLED;
      std::unique_ptr<task::TaskManager> task_manager = DISABLED;
      if (thread_registry && txn_layer && use_catalog_ && use_settings_manager_) {
        // Create the base QueryExecUtil
        query_exec_util = std::make_unique<util::QueryExecUtil>(
            txn_layer->GetTransactionManager(), catalog_layer->GetCatalog(), common::ManagedPointer(settings_manager),
            common::ManagedPointer(stats_storage), optimizer_timeout_);

        task_manager = std::make_unique<task::TaskManager>(
            common::ManagedPointer(thread_registry),
            util::QueryExecUtil::ConstructThreadLocal(common::ManagedPointer(query_exec_util)), task_pool_size_);
      }

      std::unique_ptr<metrics::MetricsThread> metrics_thread = DISABLED;
      if (use_metrics_thread_) {
        NOISEPAGE_ASSERT(use_metrics_ && metrics_manager != DISABLED,
                         "Can't have a MetricsThread without a MetricsManager.");

        metrics_thread = std::make_unique<metrics::MetricsThread>(common::ManagedPointer(metrics_manager),
                                                                  common::ManagedPointer(task_manager),
                                                                  std::chrono::microseconds{metrics_interval_});
      }

      std::unique_ptr<storage::RecoveryManager> recovery_manager = DISABLED;
      if (use_replication_) {
        auto log_provider = replication_manager->IsPrimary()
                                ? nullptr
                                : replication_manager->GetAsReplica()
                                      ->GetReplicationLogProvider()
                                      .CastManagedPointerTo<storage::AbstractLogProvider>();
        recovery_manager = std::make_unique<storage::RecoveryManager>(
            log_provider, catalog_layer->GetCatalog(), txn_layer->GetTransactionManager(),
            txn_layer->GetDeferredActionManager(), common::ManagedPointer(replication_manager),
            common::ManagedPointer(thread_registry), common::ManagedPointer(storage_layer->GetBlockStore()));
        recovery_manager->StartRecovery();
      }

      std::unique_ptr<storage::GarbageCollectorThread> gc_thread = DISABLED;
      if (use_gc_thread_) {
        NOISEPAGE_ASSERT(use_gc_ && storage_layer->GetGarbageCollector() != DISABLED,
                         "GarbageCollectorThread needs GarbageCollector.");
        gc_thread = std::make_unique<storage::GarbageCollectorThread>(storage_layer->GetGarbageCollector(),
                                                                      std::chrono::microseconds{gc_interval_},
                                                                      common::ManagedPointer(metrics_manager));
      }

      std::unique_ptr<ExecutionLayer> execution_layer = DISABLED;
      if (use_execution_) {
        execution_layer = std::make_unique<ExecutionLayer>(bytecode_handlers_path_);
      }

      std::unique_ptr<trafficcop::TrafficCop> traffic_cop = DISABLED;
      if (use_traffic_cop_) {
        NOISEPAGE_ASSERT(use_catalog_ && catalog_layer->GetCatalog() != DISABLED,
                         "TrafficCopLayer needs the CatalogLayer.");
        NOISEPAGE_ASSERT(use_stats_storage_ && stats_storage != DISABLED, "TrafficCopLayer needs StatsStorage.");
        NOISEPAGE_ASSERT(use_execution_ && execution_layer != DISABLED, "TrafficCopLayer needs ExecutionLayer.");
        traffic_cop = std::make_unique<trafficcop::TrafficCop>(
            txn_layer->GetTransactionManager(), catalog_layer->GetCatalog(),
            common::ManagedPointer(replication_manager), common::ManagedPointer(recovery_manager),
            common::ManagedPointer(settings_manager), common::ManagedPointer(stats_storage), optimizer_timeout_,
            use_query_cache_, execution_mode_);
      }

      std::unique_ptr<NetworkLayer> network_layer = DISABLED;
      if (use_network_) {
        NOISEPAGE_ASSERT(use_traffic_cop_ && traffic_cop != DISABLED, "NetworkLayer needs TrafficCopLayer.");
        network_layer =
            std::make_unique<NetworkLayer>(common::ManagedPointer(thread_registry), common::ManagedPointer(traffic_cop),
                                           network_port_, connection_thread_count_, uds_file_directory_);
      }

      std::unique_ptr<modelserver::ModelServerManager> model_server_manager = DISABLED;
      if (use_model_server_) {
        NOISEPAGE_ASSERT(use_messenger_, "Pilot requires messenger layer.");
        model_server_manager = std::make_unique<modelserver::ModelServerManager>(
            model_server_path_, messenger_layer->GetMessenger(), model_server_enable_python_coverage_);
      }

      std::unique_ptr<selfdriving::pilot::PilotThread> pilot_thread = DISABLED;
      std::unique_ptr<selfdriving::pilot::Pilot> pilot = DISABLED;
      if (use_pilot_thread_) {
        NOISEPAGE_ASSERT(use_model_server_, "Pilot requires model server manager.");
        std::unique_ptr<util::QueryExecUtil> util =
            query_exec_util ? util::QueryExecUtil::ConstructThreadLocal(common::ManagedPointer(query_exec_util))
                            : nullptr;
        pilot = std::make_unique<selfdriving::pilot::Pilot>(
            ou_model_save_path_, interference_model_save_path_, forecast_model_save_path_,
            common::ManagedPointer(catalog_layer->GetCatalog()), common::ManagedPointer(metrics_thread),
            common::ManagedPointer(model_server_manager), common::ManagedPointer(settings_manager),
            common::ManagedPointer(stats_storage), common::ManagedPointer(txn_layer->GetTransactionManager()),
            std::move(util), common::ManagedPointer(task_manager), workload_forecast_interval_, sequence_length_,
            horizon_length_);
        pilot_thread = std::make_unique<selfdriving::pilot::PilotThread>(
            common::ManagedPointer(pilot), std::chrono::microseconds{pilot_interval_},
            std::chrono::microseconds{forecast_train_interval_}, pilot_planning_);
      }

      NOISEPAGE_ASSERT(!(async_replication_enable_ && !use_replication_),
                       "async_replication_enable only controls whether replication is sync or async, you also need "
                       "use_replication.");

      // If replication is enabled, configure the replication policy for all transactions.
      if (use_replication_) {
        if (replication_manager->IsPrimary()) {
          // On the primary, replicate buffers synchronously by default, but can be made async via command line.
          transaction::ReplicationPolicy policy =
              async_replication_enable_ ? transaction::ReplicationPolicy::ASYNC : transaction::ReplicationPolicy::SYNC;
          txn_layer->GetTransactionManager()->SetDefaultTransactionReplicationPolicy(policy);
        } else {
          // On a replica, do not replicate any buffers by default.
          txn_layer->GetTransactionManager()->SetDefaultTransactionReplicationPolicy(
              transaction::ReplicationPolicy::DISABLE);
        }
      }
      {
        UNUSED_ATTRIBUTE auto &default_txn_policy = txn_layer->GetTransactionManager()->GetDefaultTransactionPolicy();
        STORAGE_LOG_DEBUG(fmt::format("Default transaction policy: DURABILITY {} REPLICATION {}",
                                      transaction::DurabilityPolicyToString(default_txn_policy.durability_),
                                      transaction::ReplicationPolicyToString(default_txn_policy.replication_)));
      }

      db_main->settings_manager_ = std::move(settings_manager);
      db_main->metrics_manager_ = std::move(metrics_manager);
      db_main->metrics_thread_ = std::move(metrics_thread);
      db_main->thread_registry_ = std::move(thread_registry);
      db_main->buffer_segment_pool_ = std::move(buffer_segment_pool);
      db_main->log_manager_ = std::move(log_manager);
      db_main->txn_layer_ = std::move(txn_layer);
      db_main->storage_layer_ = std::move(storage_layer);
      db_main->catalog_layer_ = std::move(catalog_layer);
      db_main->recovery_manager_ = std::move(recovery_manager);
      db_main->gc_thread_ = std::move(gc_thread);
      db_main->stats_storage_ = std::move(stats_storage);
      db_main->execution_layer_ = std::move(execution_layer);
      db_main->traffic_cop_ = std::move(traffic_cop);
      db_main->network_layer_ = std::move(network_layer);
      db_main->pilot_thread_ = std::move(pilot_thread);
      db_main->pilot_ = std::move(pilot);
      db_main->model_server_manager_ = std::move(model_server_manager);
      db_main->messenger_layer_ = std::move(messenger_layer);
      db_main->replication_manager_ = std::move(replication_manager);
      db_main->query_exec_util_ = std::move(query_exec_util);
      db_main->task_manager_ = std::move(task_manager);
      return db_main;
    }

    /**
     * @param value LogManager argument
     * @return self reference for chaining
     */
    Builder &SetWalFilePath(const std::string &value) {
      wal_file_path_ = value;
      return *this;
    }

    /**
     * @param param_map SettingsManager argument
     * @return self reference for chaining
     */
    Builder &SetSettingsParameterMap(std::unordered_map<settings::Param, settings::ParamInfo> &&param_map) {
      param_map_ = std::move(param_map);
      return *this;
    }

    /**
     * @param value use component
     * @return self reference for chaining
     */
    Builder &SetUseThreadRegistry(const bool value) {
      use_thread_registry_ = value;
      return *this;
    }

    /**
     * @param value use component
     * @return self reference for chaining
     */
    Builder &SetUseSettingsManager(const bool value) {
      use_settings_manager_ = value;
      return *this;
    }

    /**
     * @param value use component
     * @return self reference for chaining
     */
    Builder &SetUseMetrics(const bool value) {
      use_metrics_ = value;
      return *this;
    }

    /**
     * @param value use component
     * @return self reference for chaining
     */
    Builder &SetUseMetricsThread(const bool value) {
      use_metrics_thread_ = value;
      return *this;
    }

    /**
     * @param value use component
     * @return self reference for chaining
     */
    Builder &SetUseLogging(const bool value) {
      use_logging_ = value;
      return *this;
    }

    /**
     * @param value TransactionManager argument
     * @return self reference for chaining
     */
    Builder &SetWalAsyncCommit(const bool value) {
      wal_async_commit_enable_ = value;
      return *this;
    }

    /**
     * @param value LogManager argument
     * @return self reference for chaining
     */
    Builder &SetWalNumBuffers(const uint64_t value) {
      wal_num_buffers_ = value;
      return *this;
    }

    /**
     * @param value LogManager argument
     * @return self reference for chaining
     */
    Builder &SetWalSerializationInterval(const uint64_t value) {
      wal_serialization_interval_ = value;
      return *this;
    }

    /**
     * @param value LogManager argument
     * @return self reference for chaining
     */
    Builder &SetWalPersistInterval(const uint64_t value) {
      wal_persist_interval_ = value;
      return *this;
    }

    /**
     * @param value LogManager argument
     * @return self reference for chaining
     */
    Builder &SetWalPersistThreshold(const uint64_t value) {
      wal_persist_threshold_ = value;
      return *this;
    }

    /**
     * @param value use component
     * @return self reference for chaining
     */
    Builder &SetUseGC(const bool value) {
      use_gc_ = value;
      return *this;
    }

    /**
     * @param value use component
     * @return self reference for chaining
     */
    Builder &SetUseCatalog(const bool value) {
      use_catalog_ = value;
      return *this;
    }

    /**
     * @param value whether CatalogLayer should create default database (set to false if recovering)
     * @return self reference for chaining
     */
    Builder &SetCreateDefaultDatabase(const bool value) {
      create_default_database_ = value;
      return *this;
    }

    /**
     * @param value use component
     * @return self reference for chaining
     */
    Builder &SetUseGCThread(const bool value) {
      use_gc_thread_ = value;
      return *this;
    }

    /**
     * @param value use component
     * @return self reference for chaining
     */
    Builder &SetUseTrafficCop(const bool value) {
      use_traffic_cop_ = value;
      return *this;
    }

    /**
     * @param value use component
     * @return self reference for chaining
     */
    Builder &SetUseStatsStorage(const bool value) {
      use_stats_storage_ = value;
      return *this;
    }

    /**
     * @param value use component
     * @return self reference for chaining
     */
    Builder &SetUseNetwork(const bool value) {
      use_network_ = value;
      return *this;
    }

    /**
     * @param value use component
     * @return self reference for chaining
     */
    Builder &SetUseMessenger(const bool value) {
      use_messenger_ = value;
      return *this;
    }

    /**
     * @param identity Network identity. Must be unique across all replicas.
     * @return self reference for chaining
     */
    Builder &SetNetworkIdentity(const std::string &identity) {
      network_identity_ = identity;
      return *this;
    }

    /**
     * @param port Network port
     * @return self reference for chaining
     */
    Builder &SetNetworkPort(const uint16_t port) {
      network_port_ = port;
      return *this;
    }

    /**
     * @param port Messenger port
     * @return self reference for chaining
     */
    Builder &SetMessengerPort(const uint16_t port) {
      messenger_port_ = port;
      return *this;
    }

    /**
     * @param value RecordBufferSegmentPool argument
     * @return self reference for chaining
     */
    Builder &SetRecordBufferSegmentSize(const uint64_t value) {
      record_buffer_segment_size_ = value;
      return *this;
    }

    /**
     * @param value RecordBufferSegmentPool argument
     * @return self reference for chaining
     */
    Builder &SetRecordBufferSegmentReuse(const uint64_t value) {
      record_buffer_segment_reuse_ = value;
      return *this;
    }

    /**
     * @param value BlockStore argument
     * @return self reference for chaining
     */
    Builder &SetBlockStoreSize(const uint64_t value) {
      block_store_size_ = value;
      return *this;
    }

    /**
     * @param value BlockStore argument
     * @return self reference for chaining
     */
    Builder &SetBlockStoreReuse(const uint64_t value) {
      block_store_reuse_ = value;
      return *this;
    }

    /**
     * @param value TrafficCop argument
     * @return self reference for chaining
     */
    Builder &SetOptimizerTimeout(const uint64_t value) {
      optimizer_timeout_ = value;
      return *this;
    }

    /**
     * @param value use component
     * @return self reference for chaining
     */
    Builder &SetUseQueryCache(const bool value) {
      use_query_cache_ = value;
      return *this;
    }

    /**
     * @param value use component
     * @return self reference for chaining
     */
    Builder &SetUseExecution(const bool value) {
      use_execution_ = value;
      return *this;
    }

    /**
     * @param value use component
     * @return self reference for chaining
     */
    Builder &SetExecutionMode(const execution::vm::ExecutionMode value) {
      execution_mode_ = value;
      return *this;
    }

    /**
     * @param value with ModelServer enable
     * @return self reference for chaining
     */
    Builder &SetUseModelServer(const bool value) {
      use_model_server_ = value;
      return *this;
    }

    /**
     * @param value model_server_path
     * @return self reference for chaining
     */
    Builder &SetModelServerPath(const std::string &value) {
      model_server_path_ = value;
      return *this;
    }

    /**
     * @param value enable_python_coverage for ModelServer
     * @return self reference for chaining
     */
    Builder &SetModelServerEnablePythonCoverage(const bool value) {
      model_server_enable_python_coverage_ = value;
      return *this;
    }

    /**
     * @param value the new path to the bytecode handler bitcode file
     * @return self reference for chaining
     */
    Builder &SetBytecodeHandlersPath(const std::string &value) {
      bytecode_handlers_path_ = value;
      return *this;
    }

   private:
    std::unordered_map<settings::Param, settings::ParamInfo> param_map_;

    // These are meant to be reasonable defaults, mostly for tests. New settings should probably just mirror their
    // default values here. Larger scale tests and benchmarks may need to to use setters on the Builder to adjust these
    // before building DBMain. The real system should use the SettingsManager.

    // clang-tidy complains about excessive padding if you're not careful about ordering things.
    // This leads to somewhat unfortunate grouping but that shouldn't be too big an issue.

    uint64_t record_buffer_segment_size_ = 1e5;
    uint64_t record_buffer_segment_reuse_ = 1e4;
    uint64_t wal_num_buffers_ = 100;
    uint64_t wal_persist_threshold_ = static_cast<uint64_t>(1 << 20);
    uint64_t pilot_interval_ = 1e7;
    uint64_t forecast_train_interval_ = 120e7;
    uint64_t workload_forecast_interval_ = 1e6;
    uint64_t sequence_length_ = 10;
    uint64_t horizon_length_ = 30;
    uint64_t block_store_size_ = 1e5;
    uint64_t block_store_reuse_ = 1e3;
    uint64_t optimizer_timeout_ = 5000;
    uint64_t forecast_sample_limit_ = 5;

    std::string wal_file_path_ = "wal.log";
    std::string ou_model_save_path_;
    std::string interference_model_save_path_;
    std::string forecast_model_save_path_;
    std::string bytecode_handlers_path_ = "./bytecode_handlers_ir.bc";
    std::string network_identity_ = "primary";
    std::string uds_file_directory_ = "/tmp/";
    std::string replication_hosts_path_ = "./replication.config";
    /**
     * The ModelServer script is located at PROJECT_ROOT/script/model by default, and also assume
     * the build binary at PROJECT_ROOT/build/bin/noisepage. This should be override or set explicitly
     * in use cases where such assumptions are no longer true.
     */
    std::string model_server_path_ = "../../script/model/model_server.py";

    bool use_settings_manager_ = false;
    bool use_thread_registry_ = false;
    bool use_metrics_ = false;
    uint32_t metrics_interval_ = 10000;
    bool use_metrics_thread_ = false;
    bool query_trace_metrics_ = false;
    metrics::MetricsOutput query_trace_metrics_output_ = metrics::MetricsOutput::CSV;
    bool pipeline_metrics_ = false;
    uint8_t pipeline_metrics_sample_rate_ = 10;
    bool transaction_metrics_ = false;
    bool logging_metrics_ = false;
    uint8_t logging_metrics_sample_rate_ = 100;
    bool gc_metrics_ = false;
    bool bind_command_metrics_ = false;
    bool execute_command_metrics_ = false;
    int32_t wal_serialization_interval_ = 100;
    int32_t wal_persist_interval_ = 100;
    int32_t gc_interval_ = 1000;
    uint32_t task_pool_size_ = 1;

    uint16_t connection_thread_count_ = 4;
    uint16_t network_port_ = 15721;
    uint16_t messenger_port_ = 9022;
    uint16_t replication_port_ = 15445;

    execution::vm::ExecutionMode execution_mode_ = execution::vm::ExecutionMode::Interpret;

    bool use_logging_ = false;
    bool wal_async_commit_enable_ = false;
    bool use_gc_ = false;
    bool use_catalog_ = false;
    bool create_default_database_ = true;
    bool use_gc_thread_ = false;
    bool use_stats_storage_ = false;
    bool use_execution_ = false;
    bool use_traffic_cop_ = false;
    bool use_query_cache_ = true;
    bool use_network_ = false;
    bool use_messenger_ = false;
    bool use_replication_ = false;
    bool async_replication_enable_ = false;
    bool use_model_server_ = false;
    bool model_server_enable_python_coverage_ = false;
    bool use_pilot_thread_ = false;
    bool pilot_planning_ = false;

    /**
     * Instantiates the SettingsManager and reads all of the settings to override the Builder's settings.
     * @return
     */
    std::unique_ptr<settings::SettingsManager> BootstrapSettingsManager(const common::ManagedPointer<DBMain> db_main) {
      std::unique_ptr<settings::SettingsManager> settings_manager;
      NOISEPAGE_ASSERT(!param_map_.empty(), "Settings parameter map was never set.");
      settings_manager = std::make_unique<settings::SettingsManager>(db_main, std::move(param_map_));

      record_buffer_segment_size_ =
          static_cast<uint64_t>(settings_manager->GetInt(settings::Param::record_buffer_segment_size));
      record_buffer_segment_reuse_ =
          static_cast<uint64_t>(settings_manager->GetInt(settings::Param::record_buffer_segment_reuse));
      block_store_size_ = static_cast<uint64_t>(settings_manager->GetInt(settings::Param::block_store_size));
      block_store_reuse_ = static_cast<uint64_t>(settings_manager->GetInt(settings::Param::block_store_reuse));

      use_logging_ = settings_manager->GetBool(settings::Param::wal_enable);
      if (use_logging_) {
        wal_file_path_ = settings_manager->GetString(settings::Param::wal_file_path);
        wal_async_commit_enable_ = settings_manager->GetBool(settings::Param::wal_async_commit_enable);
        wal_num_buffers_ = static_cast<uint64_t>(settings_manager->GetInt64(settings::Param::wal_num_buffers));
        wal_serialization_interval_ = settings_manager->GetInt(settings::Param::wal_serialization_interval);
        wal_persist_interval_ = settings_manager->GetInt(settings::Param::wal_persist_interval);
        wal_persist_threshold_ =
            static_cast<uint64_t>(settings_manager->GetInt64(settings::Param::wal_persist_threshold));
      }

      use_metrics_ = settings_manager->GetBool(settings::Param::metrics);
      use_metrics_thread_ = settings_manager->GetBool(settings::Param::use_metrics_thread);
      use_pilot_thread_ = settings_manager->GetBool(settings::Param::use_pilot_thread);
      pilot_planning_ = settings_manager->GetBool(settings::Param::pilot_planning);

      gc_interval_ = settings_manager->GetInt(settings::Param::gc_interval);
      pilot_interval_ = settings_manager->GetInt64(settings::Param::pilot_interval);
      forecast_train_interval_ = settings_manager->GetInt64(settings::Param::forecast_train_interval);
      workload_forecast_interval_ = settings_manager->GetInt64(settings::Param::workload_forecast_interval);
      sequence_length_ = settings_manager->GetInt64(settings::Param::sequence_length);
      horizon_length_ = settings_manager->GetInt64(settings::Param::horizon_length);
      ou_model_save_path_ = settings_manager->GetString(settings::Param::ou_model_save_path);
      interference_model_save_path_ = settings_manager->GetString(settings::Param::interference_model_save_path);
      forecast_model_save_path_ = settings_manager->GetString(settings::Param::forecast_model_save_path);
      task_pool_size_ = settings_manager->GetInt(settings::Param::task_pool_size);

      uds_file_directory_ = settings_manager->GetString(settings::Param::uds_file_directory);
      // TODO(WAN): open an issue for handling settings.
      //  If you set it with the builder, it gets overwritten.
      //  If you set it with the setting manager, it isn't mutable.
      network_port_ = static_cast<uint16_t>(settings_manager->GetInt(settings::Param::port));
      network_identity_ = settings_manager->GetString(settings::Param::network_identity);
      connection_thread_count_ =
          static_cast<uint16_t>(settings_manager->GetInt(settings::Param::connection_thread_count));
      optimizer_timeout_ = static_cast<uint64_t>(settings_manager->GetInt(settings::Param::task_execution_timeout));
      use_query_cache_ = settings_manager->GetBool(settings::Param::use_query_cache);

      execution_mode_ = settings_manager->GetBool(settings::Param::compiled_query_execution)
                            ? execution::vm::ExecutionMode::Compiled
                            : execution::vm::ExecutionMode::Interpret;
      bytecode_handlers_path_ = settings_manager->GetString(settings::Param::bytecode_handlers_path);

      query_trace_metrics_ = settings_manager->GetBool(settings::Param::query_trace_metrics_enable);
      query_trace_metrics_output_ = *metrics::MetricsUtil::FromMetricsOutputString(
          settings_manager->GetString(settings::Param::query_trace_metrics_output));
      forecast_sample_limit_ = settings_manager->GetInt(settings::Param::forecast_sample_limit);
      pipeline_metrics_ = settings_manager->GetBool(settings::Param::pipeline_metrics_enable);
      pipeline_metrics_sample_rate_ = settings_manager->GetInt(settings::Param::pipeline_metrics_sample_rate);
      logging_metrics_sample_rate_ = settings_manager->GetInt(settings::Param::logging_metrics_sample_rate);
      transaction_metrics_ = settings_manager->GetBool(settings::Param::transaction_metrics_enable);
      logging_metrics_ = settings_manager->GetBool(settings::Param::logging_metrics_enable);
      gc_metrics_ = settings_manager->GetBool(settings::Param::gc_metrics_enable);
      bind_command_metrics_ = settings_manager->GetBool(settings::Param::bind_command_metrics_enable);
      execute_command_metrics_ = settings_manager->GetBool(settings::Param::execute_command_metrics_enable);

      use_messenger_ = settings_manager->GetBool(settings::Param::messenger_enable);
      messenger_port_ = settings_manager->GetInt(settings::Param::messenger_port);
      use_replication_ = settings_manager->GetBool(settings::Param::replication_enable);
      async_replication_enable_ = settings_manager->GetBool(settings::Param::async_replication_enable);
      replication_port_ = settings_manager->GetInt(settings::Param::replication_port);
      replication_hosts_path_ = settings_manager->GetString(settings::Param::replication_hosts_path);
      use_model_server_ = settings_manager->GetBool(settings::Param::model_server_enable);
      model_server_path_ = settings_manager->GetString(settings::Param::model_server_path);

      return settings_manager;
    }

    /**
     * Instantiate the MetricsManager and enable metrics for components arrocding to the Builder's settings.
     * @return
     */
    std::unique_ptr<metrics::MetricsManager> BootstrapMetricsManager() {
      std::unique_ptr<metrics::MetricsManager> metrics_manager = std::make_unique<metrics::MetricsManager>();
      metrics_manager->SetMetricSampleRate(metrics::MetricsComponent::EXECUTION_PIPELINE,
                                           pipeline_metrics_sample_rate_);
      metrics_manager->SetMetricSampleRate(metrics::MetricsComponent::LOGGING, logging_metrics_sample_rate_);

      if (query_trace_metrics_) metrics_manager->EnableMetric(metrics::MetricsComponent::QUERY_TRACE);
      metrics::QueryTraceMetricRawData::query_param_sample = forecast_sample_limit_;
      metrics::QueryTraceMetricRawData::query_segment_interval = workload_forecast_interval_;
      metrics_manager->SetMetricOutput(metrics::MetricsComponent::QUERY_TRACE, query_trace_metrics_output_);

      if (pipeline_metrics_) metrics_manager->EnableMetric(metrics::MetricsComponent::EXECUTION_PIPELINE);
      if (transaction_metrics_) metrics_manager->EnableMetric(metrics::MetricsComponent::TRANSACTION);
      if (logging_metrics_) metrics_manager->EnableMetric(metrics::MetricsComponent::LOGGING);
      if (gc_metrics_) metrics_manager->EnableMetric(metrics::MetricsComponent::GARBAGECOLLECTION);
      if (bind_command_metrics_) metrics_manager->EnableMetric(metrics::MetricsComponent::BIND_COMMAND);
      if (execute_command_metrics_) metrics_manager->EnableMetric(metrics::MetricsComponent::EXECUTE_COMMAND);

      return metrics_manager;
    }
  };

  /**
   * @return ManagedPointer to the component, can be nullptr if disabled
   */
  common::ManagedPointer<settings::SettingsManager> GetSettingsManager() const {
    return common::ManagedPointer(settings_manager_);
  }

  /**
   * @return ManagedPointer to the component, can be nullptr if disabled
   */
  common::ManagedPointer<metrics::MetricsManager> GetMetricsManager() const {
    return common::ManagedPointer(metrics_manager_);
  }

  /**
   * @return ManagedPointer to the component, can be nullptr if disabled
   */
  common::ManagedPointer<metrics::MetricsThread> GetMetricsThread() const {
    return common::ManagedPointer(metrics_thread_);
  }

  /**
   * @return ManagedPointer to the component, can be nullptr if disabled
   */
  common::ManagedPointer<common::DedicatedThreadRegistry> GetThreadRegistry() const {
    return common::ManagedPointer(thread_registry_);
  }

  /**
   * @return ManagedPointer to the component
   */
  common::ManagedPointer<storage::RecordBufferSegmentPool> GetBufferSegmentPool() const {
    return common::ManagedPointer(buffer_segment_pool_);
  }

  /**
   * @return ManagedPointer to the component, can be nullptr if disabled
   */
  common::ManagedPointer<storage::LogManager> GetLogManager() const { return common::ManagedPointer(log_manager_); }

  /**
   * @return ManagedPointer to the component
   */
  common::ManagedPointer<TransactionLayer> GetTransactionLayer() const { return common::ManagedPointer(txn_layer_); }

  /**
   * @return ManagedPointer to the component
   */
  common::ManagedPointer<StorageLayer> GetStorageLayer() const { return common::ManagedPointer(storage_layer_); }

  /**
   * @return ManagedPointer to the component, can be nullptr if disabled
   */
  common::ManagedPointer<CatalogLayer> GetCatalogLayer() const { return common::ManagedPointer(catalog_layer_); }

  /**
   * @return ManagedPointer to the component, can be nullptr if disabled
   */
  common::ManagedPointer<storage::GarbageCollectorThread> GetGarbageCollectorThread() const {
    return common::ManagedPointer(gc_thread_);
  }

  /**
   * @return ManagedPointer to the component, can be nullptr if disabled
   */
  common::ManagedPointer<replication::ReplicationManager> GetReplicationManager() const {
    return common::ManagedPointer(replication_manager_);
  }

  /**
   * @return ManagedPointer to the component, can be nullptr if disabled
   */
  common::ManagedPointer<selfdriving::pilot::Pilot> GetPilot() const { return common::ManagedPointer(pilot_); }

  /**
   * @return ManagedPointer to the component, can be nullptr if disabled
   */
  common::ManagedPointer<selfdriving::pilot::PilotThread> GetPilotThread() const {
    return common::ManagedPointer(pilot_thread_);
  }

  /**
   * @return ManagedPointer to the component, can be nullptr if disabled
   */
  common::ManagedPointer<trafficcop::TrafficCop> GetTrafficCop() const { return common::ManagedPointer(traffic_cop_); }

  /**
   * @return ManagedPointer to the component, can be nullptr if disabled
   */
  common::ManagedPointer<optimizer::StatsStorage> GetStatsStorage() const {
    return common::ManagedPointer(stats_storage_);
  }

  /**
   * @return ManagedPointer to the component, can be nullptr if disabled
   */
  common::ManagedPointer<NetworkLayer> GetNetworkLayer() const { return common::ManagedPointer(network_layer_); }

  /**
   * @return ManagedPointer to the component, can be nullptr if disabled
   */
  common::ManagedPointer<ExecutionLayer> GetExecutionLayer() const { return common::ManagedPointer(execution_layer_); }

  /** @return ManagedPointer to the MessengerLayer, can be nullptr if disabled. */
  common::ManagedPointer<MessengerLayer> GetMessengerLayer() const { return common::ManagedPointer(messenger_layer_); }

  /** @return ManagedPointer to the ModelServerManager, can be nullptr if disabled. */
  common::ManagedPointer<modelserver::ModelServerManager> GetModelServerManager() const {
    return common::ManagedPointer(model_server_manager_);
  }

  /** @return Standard QueryExecUtil from which other utils can be derived from */
  common::ManagedPointer<util::QueryExecUtil> GetQueryExecUtil() const {
    return common::ManagedPointer(query_exec_util_);
  }

  /** @return ManagedPointer to task manager */
  common::ManagedPointer<task::TaskManager> GetTaskManager() const { return common::ManagedPointer(task_manager_); }

 private:
  // Order matters here for destruction order
  std::unique_ptr<settings::SettingsManager> settings_manager_;
  std::unique_ptr<metrics::MetricsManager> metrics_manager_;
  std::unique_ptr<metrics::MetricsThread> metrics_thread_;
  std::unique_ptr<common::DedicatedThreadRegistry> thread_registry_;
  std::unique_ptr<storage::RecordBufferSegmentPool> buffer_segment_pool_;
  std::unique_ptr<storage::LogManager> log_manager_;
  std::unique_ptr<TransactionLayer> txn_layer_;
  std::unique_ptr<StorageLayer> storage_layer_;
  std::unique_ptr<CatalogLayer> catalog_layer_;
  std::unique_ptr<storage::GarbageCollectorThread>
      gc_thread_;  // thread needs to die before manual invocations of GC in CatalogLayer and others
  std::unique_ptr<optimizer::StatsStorage> stats_storage_;
  std::unique_ptr<ExecutionLayer> execution_layer_;
  std::unique_ptr<trafficcop::TrafficCop> traffic_cop_;
  std::unique_ptr<NetworkLayer> network_layer_;
  std::unique_ptr<MessengerLayer> messenger_layer_;
  std::unique_ptr<replication::ReplicationManager> replication_manager_;  // Depends on messenger.
  std::unique_ptr<storage::RecoveryManager> recovery_manager_;            // Depends on replication manager.
  std::unique_ptr<selfdriving::pilot::PilotThread> pilot_thread_;
  std::unique_ptr<selfdriving::pilot::Pilot> pilot_;
  std::unique_ptr<modelserver::ModelServerManager> model_server_manager_;
  std::unique_ptr<util::QueryExecUtil> query_exec_util_;
  std::unique_ptr<task::TaskManager> task_manager_;
};

}  // namespace noisepage
