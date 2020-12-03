#include <vector>

#include "gtest/gtest.h"
#include "loggers/messenger_logger.h"
#include "loggers/model_server_logger.h"
#include "main/db_main.h"
#include "self_driving/model_server/model_server_manager.h"
#include "test_util/test_harness.h"

namespace noisepage::modelserver {

/**
 * @warning Running this test requires external dependency to be located at specific paths.
 *  The environment BUILD_ABS_PATH needs to be set to be the build directory such that:
 *      1. BUILD_ABS_PATH/../script/model/model_server.py is executable
 */
class ModelServerTest : public TerrierTest {
 protected:
  static constexpr const char *BUILD_ABS_PATH = "BUILD_ABS_PATH";

  /** @return Unique pointer to built DBMain that has the relevant parameters configured. */
  static std::unique_ptr<DBMain> BuildDBMain() {
    std::string project_build_path = ::getenv(BUILD_ABS_PATH);
    auto model_server_path = project_build_path + "/../script/model/model_server.py";

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
                       .SetModelServerPath(model_server_path)
                       .Build();

    return db_main;
  }
};

// NOLINTNEXTLINE
TEST_F(ModelServerTest, PipelineTest) {
  char *project_build_path = ::getenv(BUILD_ABS_PATH);
  // This has to be set
  ASSERT_NE(project_build_path, nullptr);
  messenger::messenger_logger->set_level(spdlog::level::info);
  model_server_logger->set_level(spdlog::level::info);

  auto primary = BuildDBMain();
  primary->GetNetworkLayer()->GetServer()->RunServer();

  auto ms_manager = primary->GetModelServerManager();

  // Wait for the model server process to start
  while (!ms_manager->ModelServerStarted()) {
  }

  std::vector<std::vector<double>> features{
      {0, 10000, 4, 1, 10000, 1, 0, 0},
      {0, 10000, 4, 1, 10000, 1, 0, 0},
      {0, 10000, 4, 1, 10000, 1, 0, 0},
      {0, 10000, 4, 1, 10000, 1, 0, 0},
  };

  // Send a message
  std::string msg = "ModelServerTest";
  ms_manager->PrintMessage(msg);

  // Perform a training
  std::vector<std::string> models{"lr", "gbm"};
  std::string save_path = "/tmp/model_server_test.pickle";

  ModelServerFuture<std::string> future;
  ms_manager->TrainWith(models, std::string(project_build_path) + "/bin", save_path, &future);
  auto res = future.Wait();
  ASSERT_EQ(res.second, true);  // Training succeeds

  // Perform inference
  auto result = ms_manager->DoInference("OP_INTEGER_PLUS_OR_MINUS", save_path, features);
  ASSERT_EQ(result.size(), features.size());
  result = ms_manager->DoInference("OP_DECIMAL_COMPARE", save_path, features);
  ASSERT_EQ(result.size(), features.size());
  result = ms_manager->DoInference("OP_INTEGER_MULTIPLY", save_path, features);
  ASSERT_EQ(result.size(), features.size());

  // Model at another path should not exist
  std::string non_exist_path("/tmp/model_server_test_non_exist.pickle");
  result = ms_manager->DoInference("OP_INTEGER_PLUS_OR_MINUS", non_exist_path, features);
  ASSERT_EQ(result.size(), 0);

  // Inference with invalid opunit name will fail
  result = ms_manager->DoInference("OP_SUPER_MAGICAL_DIVIDE", non_exist_path, features);
  ASSERT_EQ(result.size(), 0);

  // Quit
  ms_manager->StopModelServer();
}
}  // namespace noisepage::modelserver
