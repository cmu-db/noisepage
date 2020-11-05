#include "messenger/messenger.h"

#include <sys/mman.h>
#include <unistd.h>

#include <vector>

#include "gtest/gtest.h"
#include "loggers/messenger_logger.h"
#include "main/db_main.h"
#include "messenger/connection_destination.h"
#include "test_util/test_harness.h"

namespace noisepage::storage {

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
      MESSENGER_LOG_TRACE(fmt::format("Parent {} forking.", ::getpid()));
      pid_t pid = fork();

      switch (pid) {
        case -1: {
          throw MESSENGER_EXCEPTION("Unable to fork.");
        }
        case 0: {
          // Child process. Execute the given function and break out.
          MESSENGER_LOG_TRACE(fmt::format("Child {} running.", ::getpid()));
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
  static std::unique_ptr<DBMain> BuildDBMain(uint16_t network_port, uint16_t messenger_port,
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
                       .SetUseExecution(true)
                       .Build();

    return db_main;
  }

  /** A dirty hack that sleeps for a little while so that sockets can clean up. */
  static void DirtySleep() { std::this_thread::sleep_for(std::chrono::seconds(5)); }

  catalog::db_oid_t CreateDatabase(transaction::TransactionContext *txn,
                                   common::ManagedPointer<catalog::Catalog> catalog, const std::string &database_name) {
    auto db_oid = catalog->CreateDatabase(common::ManagedPointer(txn), database_name, true /* bootstrap */);
    EXPECT_TRUE(db_oid != catalog::INVALID_DATABASE_OID);
    return db_oid;
  }
};

// NOLINTNEXTLINE
TEST_F(ReplicationTests, CreateDatabaseTest) {
  messenger::messenger_logger->set_level(spdlog::level::trace);

  uint16_t port_replica = 20000;
  uint16_t port_primary = port_replica + 1;

  uint16_t port_messenger_replica = 9022;
  uint16_t port_messenger_primary = port_messenger_replica + 1;

  // done[2] is shared memory (mmap) so that the forked processes can coordinate on when they are done.
  // This is done instead of waitpid() because I can't find a way to stop googletest from freaking out on waitpid().
  // done[0] : replica, done[1] : primary
  volatile bool *done = static_cast<volatile bool *>(
      mmap(nullptr, 2 * sizeof(bool), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
  NOISEPAGE_ASSERT(MAP_FAILED != done, "mmap() failed.");

  done[0] = false;
  done[1] = false;

  auto spin_until_done = [done]() {
    while (!(done[0] && done[1])) {
    }
  };

  VoidFn replica_fn = [=]() {
    auto replica = BuildDBMain(port_replica, port_messenger_replica, "replica");
    replica->GetNetworkLayer()->GetServer()->RunServer();
    common::ManagedPointer<messenger::Messenger> messenger = replica->GetMessengerLayer()->GetMessenger();
    common::ManagedPointer<ReplicationManager> replication_manager = replica->GetReplicationManager();
    common::ManagedPointer<catalog::Catalog> catalog = replica->GetCatalogLayer()->GetCatalog();
    common::ManagedPointer<transaction::TransactionManager> txn_manager =
        replica->GetTransactionLayer()->GetTransactionManager();
    common::ManagedPointer<transaction::DeferredActionManager> deferred_action_manager = replica->GetTransactionLayer()->GetDeferredActionManager();

    std::chrono::seconds replication_timeout{10};
    storage::BlockStore block_store{100, 100};
    std::unique_ptr<storage::ReplicationLogProvider> log_provider = std::make_unique<storage::ReplicationLogProvider>(replication_timeout, false);
    
    std::unique_ptr<storage::RecoveryManager> recovery_manager = std::make_unique<storage::RecoveryManager>(
        common::ManagedPointer<storage::AbstractLogProvider>(static_cast<storage::AbstractLogProvider*>(log_provider.get())),
        catalog,
        txn_manager,
        deferred_action_manager,
        common::ManagedPointer(replica->GetThreadRegistry()), common::ManagedPointer(&block_store));

    bool received = false;
    messenger->SetCallback(3, [&received, &replication_manager, &txn_manager, &catalog, &recovery_manager](
                                  std::string_view sender_id, std::string_view message) {
      std::string msg{message};
      replication_manager->RecoverFromSerializedLogRecords(msg);
      received = true;
      recovery_manager->StartRecovery();
      auto replication_delay_estimate = std::chrono::seconds(2);
      std::this_thread::sleep_for(replication_delay_estimate);

      auto txn = txn_manager->BeginTransaction();
      catalog::db_oid_t oid{0};
      EXPECT_EQ(oid, catalog->GetDatabaseOid(common::ManagedPointer(txn), "testdb"));
      txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    });

    while (!done[1]) {
    }

    MESSENGER_LOG_TRACE("Replica 1 done.");
    done[0] = true;
    spin_until_done();
  };

  VoidFn primary_fn = [=]() {
    auto primary = BuildDBMain(port_primary, port_messenger_primary, "primary");
    primary->GetNetworkLayer()->GetServer()->RunServer();

    // Create a database and commit, we should see this one after replication
    std::string database_name = "testdb";
    auto *txn = primary->GetTransactionLayer()->GetTransactionManager()->BeginTransaction();
    auto oid = CreateDatabase(txn, primary->GetCatalogLayer()->GetCatalog(), database_name);
    primary->GetTransactionLayer()->GetTransactionManager()->Commit(txn, transaction::TransactionUtil::EmptyCallback,
                                                                     nullptr);

    // Set up a connection to the replica.
    auto messenger = primary->GetMessengerLayer()->GetMessenger();
    messenger::ConnectionDestination dest_replica = messenger::Messenger::GetEndpointIPC("replica", port_messenger_replica);
    auto con_replica = messenger->MakeConnection(dest_replica);

    // Send using replication manager.
    primary->GetReplicationManager()->SendSerializedLogRecords(con_replica);

    MESSENGER_LOG_TRACE("Primary done.");
    done[1] = true;
    spin_until_done();
  };

  std::vector<pid_t> pids = ForkTests({replica_fn, primary_fn});

  // Spin until all done.
  while (!(done[0] && done[1])) {
  }

  UNUSED_ATTRIBUTE int munmap_retval = munmap(static_cast<void *>(const_cast<bool *>(done)), 2 * sizeof(bool));
  NOISEPAGE_ASSERT(-1 != munmap_retval, "munmap() failed.");
}

}  // namespace noisepage::storage
