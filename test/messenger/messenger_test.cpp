#include "messenger/messenger.h"

#include <sys/mman.h>
#include <sys/wait.h>

#include <vector>

#include "gtest/gtest.h"
#include "loggers/messenger_logger.h"
#include "main/db_main.h"
#include "messenger/connection_destination.h"
#include "test_util/test_harness.h"

namespace terrier::messenger {

class MessengerTests : public TerrierTest {
 protected:
  using VoidFn = std::function<void(void)>;

  static std::vector<pid_t> ForkTests(const std::vector<VoidFn> &funcs) {
    std::vector<pid_t> pids;
    pids.reserve(funcs.size());

    for (const auto &func : funcs) {
      MESSENGER_LOG_INFO(fmt::format("Parent {} forking.", ::getpid()));
      pid_t pid = fork();
      if (-1 == pid) {
        throw MESSENGER_EXCEPTION("Unable to fork.");
      }
      if (0 == pid) {
        // Child process. Execute the given function and break out.
        MESSENGER_LOG_INFO(fmt::format("Child {} running.", ::getpid()));
        func();
        exit(testing::Test::HasFailure());
      }
      // Parent process. Continues to fork.
      TERRIER_ASSERT(pid > 0, "Parent's kid has a bad pid.");
      pids.emplace_back(pid);
    }

    return pids;
  }

  static std::unique_ptr<DBMain> BuildDBMain(uint16_t network_port, uint16_t messenger_port,
                                             std::string &&messenger_identity) {
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
                       .SetMessengerIdentity(std::move(messenger_identity))
                       .SetUseExecution(true)
                       .Build();

    return db_main;
  }
};

// NOLINTNEXTLINE
TEST_F(MessengerTests, BasicReplicationTest) {
  std::string messenger_host = "localhost";

  uint16_t port_primary = 15721;
  uint16_t port_replica1 = port_primary + 1;
  uint16_t port_replica2 = port_primary + 2;

  uint16_t port_messenger_primary = 9022;
  uint16_t port_messenger_replica1 = port_messenger_primary + 1;
  uint16_t port_messenger_replica2 = port_messenger_primary + 2;

  bool *done =
      static_cast<bool *>(mmap(nullptr, 3 * sizeof(bool), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
  TERRIER_ASSERT(MAP_FAILED != done, "mmap() failed.");

  done[0] = false;
  done[1] = false;
  done[2] = false;

  VoidFn primary_fn = [=]() {
    auto primary = BuildDBMain(port_primary, port_messenger_primary, "primary");
    primary->GetNetworkLayer()->GetServer()->RunServer();

    while (!done[1]) {
    }

    MESSENGER_LOG_INFO("Primary done.");
    done[0] = true;

    // Spin until all done.
    while (!(done[0] && done[1] && done[2])) {
    }
  };
  VoidFn replica1_fn = [=]() {
    auto replica1 = BuildDBMain(port_replica1, port_messenger_replica1, "replica1");
    replica1->GetNetworkLayer()->GetServer()->RunServer();

    auto messenger = replica1->GetMessengerLayer()->GetMessenger();
    ConnectionDestination dest = Messenger::GetEndpointIPC(port_messenger_primary);
    auto con_id = messenger->MakeConnection(dest);
    bool reply = false;
    messenger->SendMessage(
        common::ManagedPointer(&con_id), "potato",
        [&reply](std::string_view a, std::string_view b) {
          std::cout << a << std::endl;
          std::cout << b << std::endl;
          reply = true;
        },
        0);

    while (!reply) {
    }

    MESSENGER_LOG_INFO("Replica 1 done.");
    done[1] = true;

    // Spin until all done.
    while (!(done[0] && done[1] && done[2])) {
    }
  };
  VoidFn replica2_fn = [=]() {
    auto replica2 = BuildDBMain(port_replica2, port_messenger_replica2, "replica2");
    replica2->GetNetworkLayer()->GetServer()->RunServer();
    MESSENGER_LOG_INFO("Replica 2 done.");
    done[2] = true;

    // Spin until all done.
    while (!(done[0] && done[1] && done[2])) {
    }
  };

  std::vector<pid_t> pids = ForkTests({primary_fn, replica1_fn, replica2_fn});

  // Spin until all done.
  while (!(done[0] && done[1] && done[2])) {
  }

  UNUSED_ATTRIBUTE int munmap_retval = munmap(done, 3 * sizeof(bool));
  TERRIER_ASSERT(-1 != munmap_retval, "munmap() failed.");
}

}  // namespace terrier::messenger
