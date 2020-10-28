#include "messenger/messenger.h"

#include <sys/mman.h>
#include <unistd.h>

#include <vector>

#include "gtest/gtest.h"
#include "loggers/messenger_logger.h"
#include "main/db_main.h"
#include "messenger/connection_destination.h"
#include "test_util/test_harness.h"

namespace noisepage::messenger {

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
    std::unordered_map<settings::Param, settings::ParamInfo> param_map;
    noisepage::settings::SettingsManager::ConstructParamMap(param_map);

    auto db_main = noisepage::DBMain::Builder()
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

  /** A dirty hack that sleeps for a little while so that sockets can clean up. */
  static void DirtySleep() { std::this_thread::sleep_for(std::chrono::seconds(5)); }
};

// NOLINTNEXTLINE
TEST_F(MessengerTests, BasicReplicationTest) {
  messenger_logger->set_level(spdlog::level::trace);

  uint16_t port_primary = 15721;
  uint16_t port_replica1 = port_primary + 1;
  uint16_t port_replica2 = port_primary + 2;

  uint16_t port_messenger_primary = 9022;
  uint16_t port_messenger_replica1 = port_messenger_primary + 1;
  uint16_t port_messenger_replica2 = port_messenger_primary + 2;

  // done[3] is shared memory (mmap) so that the forked processes can coordinate on when they are done.
  // This is done instead of waitpid() because I can't find a way to stop googletest from freaking out on waitpid().
  // done[0] : primary, done[1] : replica1, done[2] : replica2.
  volatile bool *done = static_cast<volatile bool *>(
      mmap(nullptr, 3 * sizeof(bool), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
  NOISEPAGE_ASSERT(MAP_FAILED != done, "mmap() failed.");

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

    while (!(done[1] && done[2])) {
    }

    MESSENGER_LOG_TRACE("Primary done.");
    done[0] = true;
    spin_until_done();
    MESSENGER_LOG_TRACE("Primary exit.");
  };

  VoidFn replica1_fn = [=]() {
    auto replica1 = BuildDBMain(port_replica1, port_messenger_replica1, "replica1");
    replica1->GetNetworkLayer()->GetServer()->RunServer();

    // Set up a connection to the primary.
    auto messenger = replica1->GetMessengerLayer()->GetMessenger();
    ConnectionDestination dest_primary = Messenger::GetEndpointIPC("primary", port_messenger_primary);
    auto con_primary = messenger->MakeConnection(dest_primary);

    // Send "potato" to the primary and expect "potato" as a reply.
    volatile bool reply_primary_potato = false;
    {
      messenger->SendMessage(
          common::ManagedPointer(&con_primary), "potato",
          [&reply_primary_potato](common::ManagedPointer<Messenger> messenger, std::string_view sender_id,
                                  std::string_view message, uint64_t recv_cb_id) {
            MESSENGER_LOG_TRACE(fmt::format("Replica 1 received from {}: {}", sender_id, message));
            EXPECT_EQ("potato", message);
            reply_primary_potato = true;
          },
          static_cast<uint8_t>(Messenger::BuiltinCallback::ECHO));
    }

    // Send "tomato" to the primary and expect "tomato" as a reply.
    volatile bool reply_primary_tomato = false;
    {
      messenger->SendMessage(
          common::ManagedPointer(&con_primary), "tomato",
          [&reply_primary_tomato](common::ManagedPointer<Messenger> messenger, std::string_view sender_id,
                                  std::string_view message, uint64_t recv_cb_id) {
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
    MESSENGER_LOG_TRACE("Replica 1 exit.");
  };

  VoidFn replica2_fn = [=]() {
    auto replica2 = BuildDBMain(port_replica2, port_messenger_replica2, "replica2");
    replica2->GetNetworkLayer()->GetServer()->RunServer();

    // Set up a connection to the primary.
    auto messenger = replica2->GetMessengerLayer()->GetMessenger();
    ConnectionDestination dest_primary = Messenger::GetEndpointIPC("primary", port_messenger_primary);
    auto con_primary = messenger->MakeConnection(dest_primary);

    // Send "elephant" to the primary and expect "elephant" as a reply.
    volatile bool reply_primary_elephant = false;
    {
      messenger->SendMessage(
          common::ManagedPointer(&con_primary), "elephant",
          [&reply_primary_elephant](common::ManagedPointer<Messenger> messenger, std::string_view sender_id,
                                    std::string_view message, uint64_t recv_cb_id) {
            MESSENGER_LOG_TRACE(fmt::format("Replica 2 received from {}: {}", sender_id, message));
            EXPECT_EQ("elephant", message);
            reply_primary_elephant = true;
          },
          static_cast<uint8_t>(Messenger::BuiltinCallback::ECHO));
    }

    // Send "correct HORSE battery staple" to the primary and expect "correct HORSE battery staple" as a reply.
    volatile bool reply_primary_chbs = false;
    {
      messenger->SendMessage(
          common::ManagedPointer(&con_primary), "correct HORSE battery staple",
          [&reply_primary_chbs](common::ManagedPointer<Messenger> messenger, std::string_view sender_id,
                                std::string_view message, uint64_t recv_cb_id) {
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
    MESSENGER_LOG_TRACE("Replica 2 exit.");
  };

  std::vector<pid_t> pids = ForkTests({primary_fn, replica1_fn, replica2_fn});

  // Spin until all done.
  while (!(done[0] && done[1] && done[2])) {
  }

  DirtySleep();

  UNUSED_ATTRIBUTE int munmap_retval = munmap(static_cast<void *>(const_cast<bool *>(done)), 3 * sizeof(bool));
  NOISEPAGE_ASSERT(-1 != munmap_retval, "munmap() failed.");
}

// NOLINTNEXTLINE
TEST_F(MessengerTests, BasicListenTest) {
  messenger_logger->set_level(spdlog::level::trace);

  uint16_t port_primary = 15721;
  uint16_t port_replica1 = 15722;

  uint16_t port_messenger_primary = 9022;
  uint16_t port_messenger_replica1 = port_messenger_primary + 1;

  uint16_t port_listen = 8030;

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
    auto primary = BuildDBMain(port_primary, port_messenger_primary, "primary");
    primary->GetNetworkLayer()->GetServer()->RunServer();

    ConnectionDestination dest_listen = Messenger::GetEndpointIPC("listen", port_listen);
    primary->GetMessengerLayer()->GetMessenger()->ListenForConnection(
        dest_listen, "listen",
        [](common::ManagedPointer<Messenger> messenger, std::string_view sender_id, std::string_view message,
           uint64_t recv_cb_id) {
          MESSENGER_LOG_TRACE("Messenger (listen) received from {}: {} {}", sender_id, message,
                              message.compare("KILLME") == 0);
          if (message.compare("KILLME") == 0) {
            messenger->SendMessage(messenger->GetConnectionRouter("listen"), std::string(sender_id), "QUIT",
                                   CallbackFns::Noop, recv_cb_id);
          }
        });

    while (!done[1]) {
    }

    MESSENGER_LOG_TRACE("Primary done.");
    done[0] = true;
    spin_until_done();
    MESSENGER_LOG_TRACE("Primary exit.");
  };

  VoidFn replica1_fn = [=]() {
    auto replica1 = BuildDBMain(port_replica1, port_messenger_replica1, "replica1");
    replica1->GetNetworkLayer()->GetServer()->RunServer();

    // Set up a connection to the primary via the listen endpoint.
    auto messenger = replica1->GetMessengerLayer()->GetMessenger();
    ConnectionDestination dest_listen = Messenger::GetEndpointIPC("listen", port_listen);
    auto con_primary = messenger->MakeConnection(dest_listen);

    // Send "replica1" to the primary to let them know who we are.
    messenger->SendMessage(common::ManagedPointer(&con_primary), "replica1", CallbackFns::Noop,
                           static_cast<uint8_t>(Messenger::BuiltinCallback::ECHO));

    // Send "KILLME" to the primary and expect "QUIT" as a reply.
    volatile bool reply_primary_quit = false;
    {
      messenger->SendMessage(
          common::ManagedPointer(&con_primary), "KILLME",
          [&reply_primary_quit](common::ManagedPointer<Messenger> messenger, std::string_view sender_id,
                                std::string_view message, uint64_t recv_cb_id) {
            MESSENGER_LOG_TRACE(fmt::format("Replica 1 received from {}: {}", sender_id, message));
            EXPECT_EQ("QUIT", message);
            reply_primary_quit = true;
          },
          static_cast<uint8_t>(Messenger::BuiltinCallback::NOOP));
    }

    while (!reply_primary_quit) {
    }

    // Send "BYE" to the primary to let them know we're going.
    messenger->SendMessage(common::ManagedPointer(&con_primary), "BYE", CallbackFns::Noop,
                           static_cast<uint8_t>(Messenger::BuiltinCallback::NOOP));

    MESSENGER_LOG_TRACE("Replica 1 done.");
    done[1] = true;
    spin_until_done();
    MESSENGER_LOG_TRACE("Replica 1 exit.");
  };

  std::vector<pid_t> pids = ForkTests({primary_fn, replica1_fn});

  // Spin until all done.
  while (!(done[0] && done[1])) {
  }

  DirtySleep();

  UNUSED_ATTRIBUTE int munmap_retval = munmap(static_cast<void *>(const_cast<bool *>(done)), 2 * sizeof(bool));
  NOISEPAGE_ASSERT(-1 != munmap_retval, "munmap() failed.");
}

}  // namespace noisepage::messenger
