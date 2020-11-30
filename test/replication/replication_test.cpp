#include <sys/mman.h>
#include <unistd.h>

#include <vector>

#include "gtest/gtest.h"
#include "loggers/replication_logger.h"
#include "main/db_main.h"
#include "messenger/connection_destination.h"
#include "replication/replication_manager.h"
#include "test_util/sql_table_test_util.h"
#include "test_util/test_harness.h"

namespace noisepage::replication {

class ReplicationTests : public TerrierTest {
 protected:
  /** A generic function that takes no arguments and returns no output. */
  using VoidFn = std::function<void(void)>;

  // Settings for OLTP style tests
  const uint64_t num_txns_ = 100;
  const uint64_t num_threads_ = 4;

  // General settings
  std::default_random_engine generator_;
  storage::RecordBufferSegmentPool buffer_pool_{2000, 100};
  storage::BlockStore block_store_{100, 100};

  /**
   * Run each function in @p funcs in a different child process, using fork().
   *
   * Note that debuggers will not show the separate child processes by default.
   * To trace forked processes, check out https://sourceware.org/gdb/onlinedocs/gdb/Forks.html
   *
   * @param funcs The functions to be run.
   * @return The list of child process PIDs that the functions were run in.
   */
  static std::vector<pid_t> ForkTests(const std::vector<VoidFn> &funcs) {
    std::vector<pid_t> pids;
    pids.reserve(funcs.size());

    // Fork for each separate function in funcs.
    for (const auto &func : funcs) {
      REPLICATION_LOG_TRACE(fmt::format("Parent {} forking.", ::getpid()));
      pid_t pid = fork();

      switch (pid) {
        case -1: {
          throw REPLICATION_EXCEPTION("Unable to fork.");
        }
        case 0: {
          // Child process. Execute the given function and break out.
          REPLICATION_LOG_TRACE(fmt::format("Child {} running.", ::getpid()));
          func();
          _exit(0);
        }
        default: {
          // Parent process. Continues to fork.
          NOISEPAGE_ASSERT(pid > 0, "Parent's kid has a bad pid.");
          pids.emplace_back(pid);
        }
      }
    }

    return pids;
  }

  /** @return Unique pointer to built DBMain that has the relevant parameters configured. */
  static std::unique_ptr<DBMain> BuildDBMain(uint16_t network_port, uint16_t messenger_port, uint16_t replication_port,
                                             const std::string &messenger_identity) {
    auto db_main = noisepage::DBMain::Builder()
                       .SetUseSettingsManager(false)
                       .SetUseGC(true)
                       .SetUseCatalog(true)
                       .SetUseGCThread(true)
                       .SetUseTrafficCop(true)
                       .SetUseStatsStorage(true)
                       .SetUseLogging(true)
                       .SetUseNetwork(true)
                       .SetNetworkPort(network_port)
                       .SetUseMessenger(true)
                       .SetMessengerPort(messenger_port)
                       .SetMessengerIdentity(messenger_identity)
                       .SetUseReplication(true)
                       .SetReplicationPort(replication_port)
                       .SetUseExecution(true)
                       .Build();

    return db_main;
  }

  LargeSqlTableTestObject* RunTest(std::unique_ptr<DBMain> &primary, const LargeSqlTableTestConfiguration &config) {
    auto primary_txn_manager = primary->GetTransactionLayer()->GetTransactionManager();
    auto primary_catalog = primary->GetCatalogLayer()->GetCatalog();
    // Run workload
    auto *tested =
        new LargeSqlTableTestObject(config, primary_txn_manager.Get(), primary_catalog.Get(), &block_store_, &generator_);
    tested->SimulateOltp(num_txns_, num_threads_);
    return tested;
  }

  void RunCheck(LargeSqlTableTestObject *tested, std::shared_ptr<DBMain> &primary, std::shared_ptr<DBMain> &replica) {
    auto primary_txn_manager = primary->GetTransactionLayer()->GetTransactionManager();
    auto primary_catalog = primary->GetCatalogLayer()->GetCatalog();
    REPLICATION_LOG_INFO("Getting replica stuff...");
    auto replica_txn_manager = replica->GetTransactionLayer()->GetTransactionManager();
    auto replica_catalog = replica->GetCatalogLayer()->GetCatalog();
    auto replica_recovery_manager = replica->GetReplicationManager()->GetRecoveryManager();
    REPLICATION_LOG_INFO("Failed to get replica stuff...");
    // Check we recovered all the original tables
    for (auto &database : tested->GetTables()) {
      auto database_oid = database.first;
      for (auto &table_oid : database.second) {
        // Get original sql table
        auto original_txn = primary_txn_manager->BeginTransaction();
        auto original_sql_table =
            primary_catalog->GetDatabaseCatalog(common::ManagedPointer(original_txn), database_oid)
                ->GetTable(common::ManagedPointer(original_txn), table_oid);

        // Get Recovered table
        auto *recovery_txn = replica_txn_manager->BeginTransaction();
        auto db_catalog = replica_catalog->GetDatabaseCatalog(common::ManagedPointer(recovery_txn), database_oid);
        EXPECT_TRUE(db_catalog != nullptr);
        auto recovered_sql_table = db_catalog->GetTable(common::ManagedPointer(recovery_txn), table_oid);
        EXPECT_TRUE(recovered_sql_table != nullptr);

        EXPECT_TRUE(StorageTestUtil::SqlTableEqualDeep(
            original_sql_table->table_.layout_, original_sql_table, recovered_sql_table,
            tested->GetTupleSlotsForTable(database_oid, table_oid), replica_recovery_manager->tuple_slot_map_,
            primary_txn_manager.Get(), replica_txn_manager.Get()));
        primary_txn_manager->Commit(original_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
        replica_txn_manager->Commit(recovery_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      }
    }
    delete tested;
  }

  catalog::db_oid_t CreateDatabase(transaction::TransactionContext *txn,
                                   common::ManagedPointer<catalog::Catalog> catalog, const std::string &database_name) {
    auto db_oid = catalog->CreateDatabase(common::ManagedPointer(txn), database_name, true /* bootstrap */);
    EXPECT_TRUE(db_oid != catalog::INVALID_DATABASE_OID);
    return db_oid;
  }

  catalog::namespace_oid_t CreateNamespace(transaction::TransactionContext *txn,
                                           common::ManagedPointer<catalog::DatabaseCatalog> db_catalog,
                                           const std::string &namespace_name) {
    auto namespace_oid = db_catalog->CreateNamespace(common::ManagedPointer(txn), namespace_name);
    EXPECT_TRUE(namespace_oid != catalog::INVALID_NAMESPACE_OID);
    return namespace_oid;
  }

  /** A dirty hack that sleeps for a little while so that sockets can clean up. */
  static void DirtySleep(int seconds) { std::this_thread::sleep_for(std::chrono::seconds(seconds)); }
};


// NOLINTNEXTLINE
/*
TEST_F(ReplicationTests, CreateDatabaseTest) {
  replication_logger->set_level(spdlog::level::trace);

  uint16_t port_primary = 15721;
  uint16_t port_replica1 = 15722;

  uint16_t port_messenger_primary = 9022;
  uint16_t port_messenger_replica1 = port_messenger_primary + 1;

  uint16_t port_replication_primary = 15445;
  uint16_t port_replication_replica1 = port_replication_primary + 1;

  volatile bool *done = static_cast<volatile bool *>(
      mmap(nullptr, 2 * sizeof(bool), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
  NOISEPAGE_ASSERT(MAP_FAILED != done, "mmap() failed.");

  done[0] = false;
  done[1] = false;

  auto spin_until_done = [done]() {
    while (!(done[0] && done[1])) {
    }
  };

  VoidFn primary_fn = [=]() {
    auto primary = BuildDBMain(port_primary, port_messenger_primary, port_replication_primary, "primary");
    primary->GetNetworkLayer()->GetServer()->RunServer();

    // Connect to replica1.
    auto replication_manager = primary->GetReplicationManager();
    replication_manager->ReplicaConnect("replica1", "localhost", port_replication_replica1);
    DirtySleep(10);

    // Create a database and commit, we should see this one after replication.
    std::string database_name = "testdb";
    //std::string namespace_name = "testns";
    auto *txn = primary->GetTransactionLayer()->GetTransactionManager()->BeginTransaction();
    auto db_oid = CreateDatabase(txn, primary->GetCatalogLayer()->GetCatalog(), database_name);
    //auto db_catalog = primary->GetCatalogLayer()->GetCatalog()->GetDatabaseCatalog(common::ManagedPointer(txn), db_oid);
    //EXPECT_TRUE(db_catalog);
    //CreateNamespace(txn, db_catalog, namespace_name);
    primary->GetTransactionLayer()->GetTransactionManager()->Commit(txn, transaction::TransactionUtil::EmptyCallback,
                                                                    nullptr);

    // Send message.
    //replication_manager->ReplicaSend("replica1", ReplicationManager::MessageType::RECOVER,
    //                                 replication_manager->SerializeLogRecords(), true);

    REPLICATION_LOG_INFO("Waiting on replica...");

    while (!done[1]) {
    }

    REPLICATION_LOG_INFO("Primary done.");
    done[0] = true;
    spin_until_done();
    REPLICATION_LOG_INFO("Primary exit.");
  };

  VoidFn replica1_fn = [=]() {
    auto replica1 = BuildDBMain(port_replica1, port_messenger_replica1, port_replication_replica1, "replica1");
    replica1->GetNetworkLayer()->GetServer()->RunServer();

    // Connect to primary.
    auto replication_manager = replica1->GetReplicationManager();
    replication_manager->ReplicaConnect("primary", "localhost", port_replication_primary);
    DirtySleep(10);

    // Wait short time for recovery.
    //replica1->GetRecoveryManager()->StartRecovery();
    //DirtySleep(5);

    auto txn = replica1->GetTransactionLayer()->GetTransactionManager()->BeginTransaction();
    catalog::db_oid_t db_oid{0};
    //catalog::namespace_oid_t ns_oid{0};
    EXPECT_EQ(db_oid, replica1->GetCatalogLayer()->GetCatalog()->GetDatabaseOid(common::ManagedPointer(txn), "testdb"));
    //auto recovered_db_catalog = replica1->GetCatalogLayer()->GetCatalog()->GetDatabaseCatalog(common::ManagedPointer(txn), db_oid);
    //EXPECT_TRUE(recovered_db_catalog);
    //EXPECT_EQ(ns_oid, recovered_db_catalog->GetNamespaceOid(common::ManagedPointer(txn), "testns"));
    replica1->GetTransactionLayer()->GetTransactionManager()->Commit(txn, transaction::TransactionUtil::EmptyCallback,
                                                                     nullptr);

    REPLICATION_LOG_INFO("Replica 1 done.");
    done[1] = true;
    spin_until_done();
    REPLICATION_LOG_INFO("Replica 1 exit.");
  };

  std::vector<pid_t> pids = ForkTests({primary_fn, replica1_fn});

  // Spin until all done.
  while (!(done[0] && done[1])) {
  }

  DirtySleep(5);

  UNUSED_ATTRIBUTE int munmap_retval = munmap(static_cast<void *>(const_cast<bool *>(done)), 2 * sizeof(bool));
  NOISEPAGE_ASSERT(-1 != munmap_retval, "munmap() failed.");
}*/


// This test inserts some tuples into a single table. It then recreates the test table from
// the log, and verifies that this new table is the same as the original table
// NOLINTNEXTLINE
TEST_F(ReplicationTests, SingleTableTest) {
  LargeSqlTableTestConfiguration config = LargeSqlTableTestConfiguration::Builder()
                                              .SetNumDatabases(1)
                                              .SetNumTables(1)
                                              .SetMaxColumns(5)
                                              .SetInitialTableSize(1000)
                                              .SetTxnLength(5)
                                              .SetInsertUpdateSelectDeleteRatio({0.2, 0.5, 0.2, 0.1})
                                              .SetVarlenAllowed(true)
                                              .Build();
  replication_logger->set_level(spdlog::level::trace);

  uint16_t port_primary = 15721;
  uint16_t port_replica1 = 15722;

  uint16_t port_messenger_primary = 9022;
  uint16_t port_messenger_replica1 = port_messenger_primary + 1;

  uint16_t port_replication_primary = 15445;
  uint16_t port_replication_replica1 = port_replication_primary + 1;

  std::unique_ptr<DBMain> primary = BuildDBMain(port_primary, port_messenger_primary, port_replication_primary, "primary");
  std::unique_ptr<DBMain> replica1 = BuildDBMain(port_replica1, port_messenger_replica1, port_replication_replica1, "replica1");
  
  primary->GetNetworkLayer()->GetServer()->RunServer();
  auto replication_manager = primary->GetReplicationManager();
  replication_manager->ReplicaConnect("replica1", "localhost", port_replication_replica1);
  RunTest(primary, config);
}

}  // namespace noisepage::replication
