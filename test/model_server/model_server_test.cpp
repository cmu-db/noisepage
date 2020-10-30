#include "messenger/messenger.h"
#include "model_server/model_server_manager.h"

#include <sys/mman.h>
#include <unistd.h>

#include <vector>

#include "gtest/gtest.h"
#include "loggers/messenger_logger.h"
#include "main/db_main.h"
#include "messenger/connection_destination.h"
#include "test_util/test_harness.h"

namespace noisepage::model {

class ModelServerTest : public TerrierTest {
 protected:
  /** A generic function that takes no arguments and returns no output. */
  using VoidFn = std::function<void(void)>;

  /** @return Unique pointer to built DBMain that has the relevant parameters configured. */
  static std::unique_ptr<DBMain> BuildDBMain(uint16_t network_port, uint16_t messenger_port,
                                             const std::string &messenger_identity) {

    auto db_main = noisepage::DBMain::Builder()
        .SetUseSettingsManager(false)
        .SetUseMessenger(true)
        .SetUseCatalog(true)
        .SetWithPilot(true)
        .SetUseNetwork(true)
        .SetUseGC(true)
        .SetUseExecution(true)
        .SetUseStatsStorage(true)
        .SetUseTrafficCop(true)
        .SetModelServerPath("/Users/chenxu/github/noisepage/script/model/model_server.py")
        .Build();


    return db_main;
  }
};

// NOLINTNEXTLINE
TEST_F(ModelServerTest, TerminalTest) {
  messenger::messenger_logger->set_level(spdlog::level::trace);

  uint16_t port_primary = 15721;
  uint16_t port_messenger_primary = 9022;
  auto primary = BuildDBMain(port_primary, port_messenger_primary, "primary");
  primary->GetNetworkLayer()->GetServer()->RunServer();

  auto ms_manager = primary->GetModelServerManager();
  char instruction;
  std::string msg, model, seq_files_dir, data_file, model_map_path;
  bool stop = false;

  // Wait for the model server process to be forked
  while(!ms_manager->ModelServerStarted()) {}

  std::cout << "Running at" << ms_manager->GetModelPid() << std::endl;
  ms_manager->PrintMessage("Hello");
  while(!stop) {
      std::cin >> instruction;
      switch(instruction) {
          case 's':
              std::cin >> msg;
              std::cout << "Sending " << msg;
              ms_manager->PrintMessage(msg);
              break;
          case 'q':
              stop = true;
              break;
          case 't':
              std::cin >> model;
              std::cin >> seq_files_dir;
              ms_manager->TrainWith(model, seq_files_dir);
              break;
          case 'i':
              std::cin >> model_map_path;
              std::cin >> data_file;

              ms_manager->DoInference(data_file, model_map_path);
              break;
          default:
              std::cout << "Unknown..." << std::endl;
      }
  }
}

}  // namespace noisepage::model
