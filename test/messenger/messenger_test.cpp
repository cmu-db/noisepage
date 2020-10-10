#include <sys/wait.h>

#include <vector>

#include "gtest/gtest.h"
#include "loggers/messenger_logger.h"
#include "main/db_main.h"
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

  static void WaitForPids(const std::vector<pid_t> &pids) {
    // TODO(WAN): Dirty testing code. Can't figure out how to shut GoogleTest up about "No such process".
    for (const auto pid : pids) {
      int status;
      waitpid(pid, &status, 0);
      MESSENGER_LOG_INFO(fmt::format("Parent {} waitpid for child {}, got status {} (exit code {}).", ::getpid(), pid,
                                     status, WEXITSTATUS(status)));
    }
  }

  static std::unique_ptr<DBMain> BuildDBMain(uint16_t network_port, uint16_t messenger_port) {
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
                       .SetUseExecution(true)
                       .Build();

    return db_main;
  }
};

// NOLINTNEXTLINE
TEST_F(MessengerTests, BasicReplicationTest) {
  uint16_t port_primary = 15721;
  uint16_t port_replica1 = port_primary + 1;
  uint16_t port_replica2 = port_primary + 2;

  uint16_t port_messenger_primary = 9022;
  uint16_t port_messenger_replica1 = port_messenger_primary + 1;
  uint16_t port_messenger_replica2 = port_messenger_primary + 2;

  VoidFn primary_fn = [port_primary, port_messenger_primary]() {
    auto primary = BuildDBMain(port_primary, port_messenger_primary);
    primary->GetNetworkLayer()->GetServer()->RunServer();
  };
  VoidFn replica1_fn = [port_replica1, port_messenger_replica1]() {
    auto replica1 = BuildDBMain(port_replica1, port_messenger_replica1);
    replica1->GetNetworkLayer()->GetServer()->RunServer();
  };
  VoidFn replica2_fn = [port_replica2, port_messenger_replica2]() {
    auto replica2 = BuildDBMain(port_replica2, port_messenger_replica2);
    replica2->GetNetworkLayer()->GetServer()->RunServer();
  };

  std::vector<pid_t> pids = ForkTests({primary_fn, replica1_fn, replica2_fn});
  WaitForPids(pids);
}

}  // namespace terrier::messenger
