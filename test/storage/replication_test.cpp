#include <test_util/test_harness.h>
#include <random>
#include <string>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/postgres/pg_namespace.h"
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "network/itp/itp_protocol_interpreter.h"
#include "storage/garbage_collector_thread.h"
#include "storage/index/index_builder.h"
#include "storage/recovery/recovery_manager.h"
#include "storage/recovery/replication_log_provider.h"
#include "storage/write_ahead_log/log_manager.h"
#include "test_util/sql_table_test_util.h"
#include "test_util/storage_test_util.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

// Make sure that if you create additional files, you call unlink on them after the test finishes. Otherwise, repeated
// executions will read old test's data, and the cause of the errors will be hard to identify. Trust me it will drive
// you nuts...
#define LOG_FILE_NAME "./test.log"

namespace terrier::storage {
class ReplicationTests : public TerrierTest {
 protected:
  // This is an estimate for how long we expect for all the logs to arrive at the replica and be replayed by the
  // recovery manager. You should be pessimistic in setting this number, be sure it is an overestimate. Each test should
  // set it to its own value depending on the amount of work it's doing
  std::chrono::seconds replication_delay_estimate_;

  // Settings for log manager
  const uint64_t num_log_buffers_ = 100;
  const std::chrono::microseconds log_serialization_interval_{10};
  const std::chrono::milliseconds log_persist_interval_{20};
  const uint64_t log_persist_threshold_ = (1 << 20);  // 1MB
  const std::string ip_address_ = "127.0.0.1";
  const uint16_t replication_port_ = 9022;

  // Settings for server
  uint32_t max_connections_ = 1;
  uint32_t conn_backlog_ = 1;

  // Settings for replication
  const std::chrono::seconds replication_timeout_{10};

  // General settings
  std::default_random_engine generator_;
  storage::RecordBufferSegmentPool buffer_pool_{2000, 100};
  storage::BlockStore block_store_{100, 100};

  // Settings for gc
  const std::chrono::milliseconds gc_period_{10};

  // Master node's components (prefixed with "master_") in order of initialization
  // We need:
  //  1. ThreadRegistry
  //  2. LogManager
  //  3. TxnManager
  //  4. Catalog
  //  4. GC
  common::DedicatedThreadRegistry *master_thread_registry_;
  LogManager *master_log_manager_;
  transaction::TimestampManager *master_timestamp_manager_;
  transaction::DeferredActionManager *master_deferred_action_manager_;
  transaction::TransactionManager *master_txn_manager_;
  catalog::Catalog *master_catalog_;
  storage::GarbageCollector *master_gc_;
  storage::GarbageCollectorThread *master_gc_thread_;

  // Replica node's components (prefixed with "replica_") in order of initialization
  //  1. Thread Registry
  //  2. TxnManager
  //  3. Catalog
  //  4. GC
  //  5. RecoveryManager
  //  6. TrafficCop
  //  7. TerrierServer
  common::DedicatedThreadRegistry *replica_thread_registry_;
  transaction::TimestampManager *replica_timestamp_manager_;
  transaction::DeferredActionManager *replica_deferred_action_manager_;
  transaction::TransactionManager *replica_txn_manager_;
  catalog::Catalog *replica_catalog_;
  storage::GarbageCollector *replica_gc_;
  storage::GarbageCollectorThread *replica_gc_thread_;
  storage::ReplicationLogProvider *replica_log_provider_;
  storage::RecoveryManager *replica_recovery_manager_;
  network::ITPCommandFactory *replica_itp_command_factory_;
  network::ITPProtocolInterpreter::Provider *replica_itp_protocol_provider_;
  network::ConnectionHandleFactory *replica_connection_handle_factory_;
  trafficcop::TrafficCop *replica_tcop_;
  network::TerrierServer *replica_server_;

  void SetUp() override {
    TerrierTest::SetUp();
    // Unlink log file in case one exists from previous test iteration
    unlink(LOG_FILE_NAME);

    // We first bring up the replica, then the master node
    replica_thread_registry_ = new common::DedicatedThreadRegistry(DISABLED);
    replica_timestamp_manager_ = new transaction::TimestampManager;
    replica_deferred_action_manager_ =
        new transaction::DeferredActionManager(common::ManagedPointer(replica_timestamp_manager_));
    replica_txn_manager_ = new transaction::TransactionManager(common::ManagedPointer(replica_timestamp_manager_),
                                                               common::ManagedPointer(replica_deferred_action_manager_),
                                                               common::ManagedPointer(&buffer_pool_), true, DISABLED);
    replica_gc_ = new storage::GarbageCollector(common::ManagedPointer(replica_timestamp_manager_),
                                                common::ManagedPointer(replica_deferred_action_manager_),
                                                common::ManagedPointer(replica_txn_manager_), DISABLED);
    replica_catalog_ = new catalog::Catalog(common::ManagedPointer(replica_txn_manager_),
                                            common::ManagedPointer(&block_store_), common::ManagedPointer(replica_gc_));
    replica_gc_thread_ = new storage::GarbageCollectorThread(common::ManagedPointer(replica_gc_), gc_period_,
                                                             DISABLED);  // Enable background GC

    // Bring up recovery manager
    replica_log_provider_ = new ReplicationLogProvider(replication_timeout_);
    replica_recovery_manager_ = new RecoveryManager(
        common::ManagedPointer<AbstractLogProvider>(replica_log_provider_), common::ManagedPointer(replica_catalog_),
        common::ManagedPointer(replica_txn_manager_), common::ManagedPointer(replica_deferred_action_manager_),
        common::ManagedPointer(replica_thread_registry_), common::ManagedPointer(&block_store_));
    replica_recovery_manager_->StartRecovery();

    // Bring up network layer
    replica_itp_command_factory_ = new network::ITPCommandFactory;
    replica_itp_protocol_provider_ =
        new network::ITPProtocolInterpreter::Provider(common::ManagedPointer(replica_itp_command_factory_));
    replica_tcop_ = new trafficcop::TrafficCop(common::ManagedPointer(replica_txn_manager_),
                                               common::ManagedPointer(replica_catalog_),
                                               common::ManagedPointer(replica_log_provider_), DISABLED, DISABLED, 0,
                                               false, execution::vm::ExecutionMode::Interpret);
    replica_connection_handle_factory_ = new network::ConnectionHandleFactory(common::ManagedPointer(replica_tcop_));
    try {
      replica_server_ = new network::TerrierServer(
          common::ManagedPointer<network::ProtocolInterpreter::Provider>(replica_itp_protocol_provider_),
          common::ManagedPointer(replica_connection_handle_factory_), common::ManagedPointer(replica_thread_registry_),
          replication_port_, max_connections_);
      replica_server_->RegisterProtocol(
          replication_port_,
          common::ManagedPointer<network::ProtocolInterpreter::Provider>(replica_itp_protocol_provider_),
          max_connections_, conn_backlog_);
      replica_server_->RunServer();
    } catch (NetworkProcessException &exception) {
      NETWORK_LOG_ERROR("[LaunchServer] exception when launching server");
      throw;
    }

    // Bring up components for master node
    master_thread_registry_ = new common::DedicatedThreadRegistry(DISABLED);
    master_log_manager_ = new LogManager(
        LOG_FILE_NAME, num_log_buffers_, log_serialization_interval_, log_persist_interval_, log_persist_threshold_,
        common::ManagedPointer(&buffer_pool_), common::ManagedPointer(master_thread_registry_));
    master_log_manager_->Start();
    master_timestamp_manager_ = new transaction::TimestampManager;
    master_deferred_action_manager_ =
        new transaction::DeferredActionManager(common::ManagedPointer(master_timestamp_manager_));
    master_txn_manager_ = new transaction::TransactionManager(
        common::ManagedPointer(master_timestamp_manager_), common::ManagedPointer(master_deferred_action_manager_),
        common::ManagedPointer(&buffer_pool_), true, common::ManagedPointer(master_log_manager_));
    master_gc_ = new storage::GarbageCollector(common::ManagedPointer(master_timestamp_manager_),
                                               common::ManagedPointer(master_deferred_action_manager_),
                                               common::ManagedPointer(master_txn_manager_), DISABLED);
    master_catalog_ = new catalog::Catalog(common::ManagedPointer(master_txn_manager_),
                                           common::ManagedPointer(&block_store_), common::ManagedPointer(master_gc_));
    master_gc_thread_ = new storage::GarbageCollectorThread(common::ManagedPointer(master_gc_), gc_period_,
                                                            DISABLED);  // Enable background GC
  }

  void TearDown() override {
    // Delete log file
    unlink(LOG_FILE_NAME);
    TerrierTest::TearDown();

    // Destroy original catalog. We need to manually call GC followed by a ForceFlush because catalog deletion can defer
    // events that create new transactions, which then need to be flushed before they can be GC'd.
    master_catalog_->TearDown();
    delete master_gc_thread_;
    // StorageTestUtil::FullyPerformGC(master_gc_, master_log_manager_);
    master_deferred_action_manager_->FullyPerformGC(common::ManagedPointer(master_gc_),
                                                    common::ManagedPointer(master_log_manager_));
    master_log_manager_->PersistAndStop();

    // Delete in reverse order of initialization
    delete master_gc_;
    delete master_catalog_;
    delete master_txn_manager_;
    delete master_deferred_action_manager_;
    delete master_timestamp_manager_;
    delete master_log_manager_;
    delete master_thread_registry_;

    // Replication should be finished by now, each test should ensure it waits for ample time for everything to
    // replicate
    replica_recovery_manager_->WaitForRecoveryToFinish();
    replica_catalog_->TearDown();
    delete replica_gc_thread_;
    // StorageTestUtil::FullyPerformGC(replica_gc_, DISABLED);
    replica_deferred_action_manager_->FullyPerformGC(common::ManagedPointer(replica_gc_), DISABLED);

    delete replica_server_;
    delete replica_connection_handle_factory_;
    delete replica_tcop_;
    delete replica_itp_protocol_provider_;
    delete replica_itp_command_factory_;
    delete replica_recovery_manager_;
    delete replica_log_provider_;
    delete replica_gc_;
    delete replica_catalog_;
    delete replica_txn_manager_;
    delete replica_deferred_action_manager_;
    delete replica_timestamp_manager_;
  }

  catalog::db_oid_t CreateDatabase(transaction::TransactionContext *txn, catalog::Catalog *catalog,
                                   const std::string &database_name) {
    auto db_oid = catalog->CreateDatabase(common::ManagedPointer(txn), database_name, true /* bootstrap */);
    EXPECT_TRUE(db_oid != catalog::INVALID_DATABASE_OID);
    return db_oid;
  }
};

TEST_F(ReplicationTests, CreateDatabaseTest) {
  std::string database_name = "testdb";
  // Create a database and commit, we should see this one after replication
  auto *txn = master_txn_manager_->BeginTransaction();
  auto db_oid = CreateDatabase(txn, master_catalog_, database_name);
  master_txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  replication_delay_estimate_ = std::chrono::seconds(2);
  std::this_thread::sleep_for(replication_delay_estimate_);

  txn = replica_txn_manager_->BeginTransaction();
  EXPECT_EQ(db_oid, replica_catalog_->GetDatabaseOid(common::ManagedPointer(txn), database_name));
  EXPECT_TRUE(replica_catalog_->GetDatabaseCatalog(common::ManagedPointer(txn), db_oid));
  replica_txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

}  // namespace terrier::storage
