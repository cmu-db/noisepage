#include "messenger/messenger.h"

#include <sys/mman.h>
#include <unistd.h>

#include <vector>

#include "gtest/gtest.h"
#include "loggers/messenger_logger.h"
#include "storage/recovery/recovery_manager.h"
#include "main/db_main.h"
#include "messenger/connection_destination.h"
#include "test_util/test_harness.h"

namespace terrier::messenger {

class MessengerTests : public TerrierTest {
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
          exit(0);
        }
        default: {
          // Parent process. Continues to fork.
          TERRIER_ASSERT(pid > 0, "Parent's kid has a bad pid.");
          pids.emplace_back(pid);
        }
      }
    }

    return pids;
  }

  /** @return Unique pointer to built DBMain that has the relevant parameters configured. */
  static std::unique_ptr<DBMain> BuildDBMain(uint16_t network_port, uint16_t messenger_port,
                                             const std::string &messenger_identity) {
    std::unordered_map<settings::Param, settings::ParamInfo> param_map;
    terrier::settings::SettingsManager::ConstructParamMap(param_map);

    auto db_main = terrier::DBMain::Builder()
                       .SetSettingsParameterMap(std::move(param_map))
                       .SetUseSettingsManager(true)
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
};

// NOLINTNEXTLINE
TEST_F(MessengerTests, BasicReplicationTest) {
  // TODO(WAN): remove this after demo at meeting.
  messenger_logger->set_level(spdlog::level::trace);

  uint16_t port_primary = 20000;
  uint16_t port_replica1 = port_primary + 1;
  uint16_t port_replica2 = port_primary + 2;

  uint16_t port_messenger_primary = 9022;
  uint16_t port_messenger_replica1 = port_messenger_primary + 1;
  uint16_t port_messenger_replica2 = port_messenger_primary + 2;

  // done[3] is shared memory (mmap) so that the forked processes can coordinate on when they are done.
  // This is done instead of waitpid() because I can't find a way to stop googletest from freaking out on waitpid().
  // done[0] : primary, done[1] : replica1, done[2] : replica2.
  bool *done =
      static_cast<bool *>(mmap(nullptr, 3 * sizeof(bool), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
  TERRIER_ASSERT(MAP_FAILED != done, "mmap() failed.");

  done[0] = false;
  done[1] = false;
  done[2] = false;

  auto spin_until_done = [done]() {
    while (!(done[0] && done[1] && done[2])) {
    }
  };

  VoidFn primary_fn = [=]() {
    auto primary = BuildDBMain(port_primary, port_messenger_primary, "primary");
    primary->GetNetworkLayer()->GetServer()->RunServer();

    while (!done[1]) {
    }

    MESSENGER_LOG_TRACE("Primary done.");
    done[0] = true;
    spin_until_done();
  };

  VoidFn replica1_fn = [=]() {
    auto replica1 = BuildDBMain(port_replica1, port_messenger_replica1, "replica1");
    replica1->GetNetworkLayer()->GetServer()->RunServer();

    // Set up a connection to the primary.
    auto messenger = replica1->GetMessengerLayer()->GetMessenger();
    ConnectionDestination dest_primary = Messenger::GetEndpointIPC("primary", port_messenger_primary);
    auto con_primary = messenger->MakeConnection(dest_primary);

    // Send "potato" to the primary and expect "potato" as a reply.
    bool reply_primary_potato = false;
    {
      messenger->SendMessage(
          common::ManagedPointer(&con_primary), "potato",
          [&reply_primary_potato](std::string_view sender_id, std::string_view message) {
            MESSENGER_LOG_TRACE(fmt::format("Replica 1 received from {}: {}", sender_id, message));
            EXPECT_EQ("potato", message);
            reply_primary_potato = true;
          },
          static_cast<uint8_t>(Messenger::BuiltinCallback::ECHO));
    }

    // Send "tomato" to the primary and expect "tomato" as a reply.
    bool reply_primary_tomato = false;
    {
      messenger->SendMessage(
          common::ManagedPointer(&con_primary), "tomato",
          [&reply_primary_tomato](std::string_view sender_id, std::string_view message) {
            MESSENGER_LOG_TRACE(fmt::format("Replica 1 received from {}: {}", sender_id, message));
            EXPECT_EQ("tomato", message);
            reply_primary_tomato = true;
          },
          static_cast<uint8_t>(Messenger::BuiltinCallback::ECHO));
    }

    while (!(reply_primary_potato && reply_primary_tomato)) {
    }

    MESSENGER_LOG_TRACE("Replica 1 done.");
    done[1] = true;
    spin_until_done();
  };

  VoidFn replica2_fn = [=]() {
    auto replica2 = BuildDBMain(port_replica2, port_messenger_replica2, "replica2");
    replica2->GetNetworkLayer()->GetServer()->RunServer();

    // Set up a connection to the primary.
    auto messenger = replica2->GetMessengerLayer()->GetMessenger();
    ConnectionDestination dest_primary = Messenger::GetEndpointIPC("primary", port_messenger_primary);
    auto con_primary = messenger->MakeConnection(dest_primary);

    // Send "elephant" to the primary and expect "elephant" as a reply.
    bool reply_primary_elephant = false;
    {
      messenger->SendMessage(
          common::ManagedPointer(&con_primary), "elephant",
          [&reply_primary_elephant](std::string_view sender_id, std::string_view message) {
            MESSENGER_LOG_TRACE(fmt::format("Replica 2 received from {}: {}", sender_id, message));
            EXPECT_EQ("elephant", message);
            reply_primary_elephant = true;
          },
          static_cast<uint8_t>(Messenger::BuiltinCallback::ECHO));
    }

    // Send "correct HORSE battery staple" to the primary and expect "correct HORSE battery staple" as a reply.
    bool reply_primary_chbs = false;
    {
      messenger->SendMessage(
          common::ManagedPointer(&con_primary), "correct HORSE battery staple",
          [&reply_primary_chbs](std::string_view sender_id, std::string_view message) {
            MESSENGER_LOG_TRACE(fmt::format("Replica 2 received from {}: {}", sender_id, message));
            EXPECT_EQ("correct HORSE battery staple", message);
            reply_primary_chbs = true;
          },
          static_cast<uint8_t>(Messenger::BuiltinCallback::ECHO));
    }

    while (!(reply_primary_elephant && reply_primary_chbs)) {
    }

    MESSENGER_LOG_TRACE("Replica 2 done.");
    done[2] = true;
    spin_until_done();
  };

  std::vector<pid_t> pids = ForkTests({primary_fn, replica1_fn, replica2_fn});

  // Spin until all done.
  while (!(done[0] && done[1] && done[2])) {
  }

  UNUSED_ATTRIBUTE int munmap_retval = munmap(done, 3 * sizeof(bool));
  TERRIER_ASSERT(-1 != munmap_retval, "munmap() failed.");
}

// NOLINTNEXTLINE
  TEST_F(MessengerTests, ReplicationTest) {
    // TODO(WAN): remove this after demo at meeting.
    messenger_logger->set_level(spdlog::level::trace);

    uint16_t port_primary = 20000;
    uint16_t port_replica1 = port_primary + 1;

    uint16_t port_messenger_primary = 9022;
    uint16_t port_messenger_replica1 = port_messenger_primary + 1;

    // done[3] is shared memory (mmap) so that the forked processes can coordinate on when they are done.
    // This is done instead of waitpid() because I can't find a way to stop googletest from freaking out on waitpid().
    // done[0] : primary, done[1] : replica1, done[2] : replica2.
    bool *done =
        static_cast<bool *>(mmap(nullptr, 2 * sizeof(bool), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    TERRIER_ASSERT(MAP_FAILED != done, "mmap() failed.");

    done[0] = false;
    done[1] = false;

    auto spin_until_done = [done]() {
      while (!(done[0] && done[1])) {
      }
    };

    VoidFn primary_fn = [=]() {
      auto primary = BuildDBMain(port_primary, port_messenger_primary, "primary");
      primary->GetNetworkLayer()->GetServer()->RunServer();
      auto messenger = primary->GetMessengerLayer()->GetMessenger();
      auto replication_manager = primary->GetReplicationManager();

      const std::chrono::seconds replication_timeout_{10};
      auto catalog = primary->GetCatalogLayer()->GetCatalog();
      auto txn_manager = primary->GetTransactionLayer()->GetTransactionManager();
      auto* log_provider_ = new storage::ReplicationLogProvider(replication_timeout_);
      storage::BlockStore block_store_{100, 100};
      auto recovery_manager_ = new storage::RecoveryManager(
          common::ManagedPointer<storage::AbstractLogProvider>(log_provider_), common::ManagedPointer(primary->GetCatalogLayer()->GetCatalog()),
          common::ManagedPointer(primary->GetTransactionLayer()->GetTransactionManager()), common::ManagedPointer(primary->GetTransactionLayer()->GetDeferredActionManager()),
          common::ManagedPointer(primary->GetThreadRegistry()), common::ManagedPointer(&block_store_));


      bool received = false;
      messenger->SetCallback(3, [&received, &replication_manager, &txn_manager, &catalog, &recovery_manager_](std::string_view sender_id, std::string_view message) {
        std::string msg{message};
        replication_manager->Recover(msg);
        received = true;
        recovery_manager_->StartRecovery();
        auto replication_delay_estimate_ = std::chrono::seconds(2);
        std::this_thread::sleep_for(replication_delay_estimate_);

        auto txn = txn_manager->BeginTransaction();
        catalog::db_oid_t oid{0};
        EXPECT_EQ(oid, catalog->GetDatabaseOid(common::ManagedPointer(txn), "testdb"));
        txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      });

      while (!done[1]) {
      }

      MESSENGER_LOG_TRACE("Primary done.");
      done[0] = true;
      spin_until_done();
    };

    VoidFn replica1_fn = [=]() {
      auto replica1 = BuildDBMain(port_replica1, port_messenger_replica1, "replica1");
      replica1->GetNetworkLayer()->GetServer()->RunServer();

      std::string database_name = "testdb";
      // Create a database and commit, we should see this one after replication
      auto *txn = replica1->GetTransactionLayer()->GetTransactionManager()->BeginTransaction();
      auto oid = CreateDatabase(txn, replica1->GetCatalogLayer()->GetCatalog(), database_name);
      STORAGE_LOG_ERROR("oid: ", oid);
      replica1->GetTransactionLayer()->GetTransactionManager()->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

      // Set up a connection to the primary.
      auto messenger = replica1->GetMessengerLayer()->GetMessenger();
      ConnectionDestination dest_primary = Messenger::GetEndpointIPC("primary", port_messenger_primary);
      auto con_primary = messenger->MakeConnection(dest_primary);

      // Send using replication manager.
      replica1->GetReplicationManager()->SendMessage(messenger, con_primary);

      MESSENGER_LOG_TRACE("Replica 1 done.");
      done[1] = true;
      spin_until_done();
    };

    std::vector<pid_t> pids = ForkTests({primary_fn, replica1_fn});

    // Spin until all done.
    while (!(done[0] && done[1])) {
    }

    UNUSED_ATTRIBUTE int munmap_retval = munmap(done, 2 * sizeof(bool));
    TERRIER_ASSERT(-1 != munmap_retval, "munmap() failed.");
  }

}  // namespace terrier::messenger
