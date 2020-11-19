
#include <vector>

#include "gtest/gtest.h"
#include "loggers/messenger_logger.h"
#include "loggers/model_logger.h"
#include "main/db_main.h"
#include "test_util/test_harness.h"

namespace noisepage::model {

class ModelServerTest : public TerrierTest {
 protected:
  /** A generic function that takes no arguments and returns no output. */
  using VoidFn = std::function<void(void)>;

  /** @return Unique pointer to built DBMain that has the relevant parameters configured. */
  static std::unique_ptr<DBMain> BuildDBMain() {
    auto db_main = noisepage::DBMain::Builder()
                       .SetUseSettingsManager(false)
                       .SetUseMessenger(true)
                       .SetUseCatalog(true)
                       .SetWithModelServer(true)
                       .SetUseNetwork(true)
                       .SetUseGC(true)
                       .SetUseExecution(true)
                       .SetUseStatsStorage(true)
                       .SetUseTrafficCop(true)
                       .SetModelServerPath("../../script/model/model_server.py")
                       .Build();

    return db_main;
  }
};

// NOLINTNEXTLINE
TEST_F(ModelServerTest, DISABLED_TerminalTest) {
  messenger::messenger_logger->set_level(spdlog::level::trace);
  model_logger->set_level(spdlog::level::info);

  auto primary = BuildDBMain();
  primary->GetNetworkLayer()->GetServer()->RunServer();

  auto ms_manager = primary->GetModelServerManager();
  char instruction;
  std::string msg, model, seq_files_dir, data_file, model_map_path;
  bool stop = false;

  // Wait for the model server process to be forked
  while (!ms_manager->ModelServerStarted()) {
  }

  while (!stop) {
    std::cin >> instruction;
    switch (instruction) {
      case 's':
        std::cin >> msg;
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
        MODEL_LOG_INFO("Unknown command {}", instruction);
    }
  }
}

}  // namespace noisepage::model
