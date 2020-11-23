#include <sys/mman.h>
#include <unistd.h>

#include <vector>

#include "gtest/gtest.h"
#include "loggers/replication_logger.h"
#include "main/db_main.h"
#include "messenger/connection_destination.h"
#include "replication/replication_manager.h"
#include "test_util/test_harness.h"

namespace noisepage::replication {

class ReplicationTests : public TerrierTest {
 protected:
  /** A generic function that takes no arguments and returns no output. */
  using VoidFn = std::function<void(void)>;

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

  catalog::db_oid_t CreateDatabase(transaction::TransactionContext *txn,
                                   common::ManagedPointer<catalog::Catalog> catalog, const std::string &database_name) {
    auto db_oid = catalog->CreateDatabase(common::ManagedPointer(txn), database_name, true /* bootstrap */);
    EXPECT_TRUE(db_oid != catalog::INVALID_DATABASE_OID);
    return db_oid;
  }

  /** A dirty hack that sleeps for a little while so that sockets can clean up. */
  static void DirtySleep(int seconds) { std::this_thread::sleep_for(std::chrono::seconds(seconds)); }
};

// NOLINTNEXTLINE
TEST_F(ReplicationTests, CreateDatabaseAsynchronousTest) {
  replication_logger->set_level(spdlog::level::info);

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
    auto *txn = primary->GetTransactionLayer()->GetTransactionManager()->BeginTransaction();
    CreateDatabase(txn, primary->GetCatalogLayer()->GetCatalog(), database_name);
    primary->GetTransactionLayer()->GetTransactionManager()->Commit(txn, transaction::TransactionUtil::EmptyCallback,
                                                                    nullptr);

    // Send message.
    replication_manager->ReplicaSend("replica1", ReplicationManager::MessageType::RECOVER,
                                     replication_manager->SerializeLogRecords(), false);

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
    DirtySleep(15);

    // Wait short time for recovery.
    auto txn = replica1->GetTransactionLayer()->GetTransactionManager()->BeginTransaction();
    catalog::db_oid_t oid{0};
    EXPECT_EQ(oid, replica1->GetCatalogLayer()->GetCatalog()->GetDatabaseOid(common::ManagedPointer(txn), "testdb"));
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
}

// NOLINTNEXTLINE
TEST_F(ReplicationTests, CreateDatabaseSynchronousTest) {
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
    auto *txn = primary->GetTransactionLayer()->GetTransactionManager()->BeginTransaction();
    CreateDatabase(txn, primary->GetCatalogLayer()->GetCatalog(), database_name);
    primary->GetTransactionLayer()->GetTransactionManager()->Commit(txn, transaction::TransactionUtil::EmptyCallback,
                                                                    nullptr);

    // Send message.
    replication_manager->ReplicaSend("replica1", ReplicationManager::MessageType::RECOVER,
                                     replication_manager->SerializeLogRecords(), true);

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
    DirtySleep(15);

    // Wait short time for recovery.
    auto txn = replica1->GetTransactionLayer()->GetTransactionManager()->BeginTransaction();
    catalog::db_oid_t oid{0};
    EXPECT_EQ(oid, replica1->GetCatalogLayer()->GetCatalog()->GetDatabaseOid(common::ManagedPointer(txn), "testdb"));
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
}

}  // namespace noisepage::replication
