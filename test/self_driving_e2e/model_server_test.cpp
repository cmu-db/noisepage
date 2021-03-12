#include <vector>

#include "gtest/gtest.h"
#include "loggers/messenger_logger.h"
#include "loggers/model_server_logger.h"
#include "main/db_main.h"
#include "self_driving/model_server/model_server_manager.h"
#include "self_driving/modeling/operating_unit_util.h"
#include "test_util/test_harness.h"

namespace noisepage::modelserver {

/**
 * @warning Running this test requires external dependency to be located at specific paths.
 *
 * The environment BUILD_ABS_PATH needs to be set to be the build directory such that
 * BUILD_ABS_PATH/../script/model/model_server.py is executable. Note that on Jenkins,
 * tests do not run with their current working directory set to the build subdirectory
 * from which "../script/self_driving/model_server.py" is valid. BUILD_ABS_PATH (an
 * optional env arg) is used to allow this test to run both locally and remotely
 * without modifying the test itself.
 */
class ModelServerTest : public TerrierTest {
 protected:
  static constexpr const char *BUILD_ABS_PATH = "BUILD_ABS_PATH";

  /** @return Unique pointer to built DBMain that has the relevant parameters configured. */
  static std::unique_ptr<DBMain> BuildDBMain() {
    const char *env = ::getenv(BUILD_ABS_PATH);
    std::string project_build_path = (env != nullptr ? env : ".");
    auto model_server_path = project_build_path + "/../script/self_driving/model_server.py";

    auto db_main = noisepage::DBMain::Builder()
                       .SetUseSettingsManager(false)
                       .SetUseMessenger(true)
                       .SetUseCatalog(true)
                       .SetUseModelServer(true)
                       .SetUseNetwork(true)
                       .SetUseGC(true)
                       .SetUseExecution(true)
                       .SetUseStatsStorage(true)
                       .SetUseTrafficCop(true)
                       .SetModelServerPath(model_server_path)
                       .SetModelServerEnablePythonCoverage(true)
                       .Build();

    return db_main;
  }

  /**
   * Wrapper to hide long chain of function name resolution
   * @param unit_type OpUnit
   * @return  string representation of the opunit
   */
  static std::string OpUnitToString(selfdriving::ExecutionOperatingUnitType unit_type) {
    return selfdriving::OperatingUnitUtil::ExecutionOperatingUnitTypeToString(unit_type);
  }
};

// NOLINTNEXTLINE
TEST_F(ModelServerTest, OUAndInterferenceModelTest) {
  messenger::messenger_logger->set_level(spdlog::level::info);
  model_server_logger->set_level(spdlog::level::info);

  auto primary = BuildDBMain();
  primary->GetNetworkLayer()->GetServer()->RunServer();

  auto ms_manager = primary->GetModelServerManager();

  // Wait for the model server process to start
  while (!ms_manager->ModelServerStarted()) {
  }

  // Send a message
  std::string msg = "ModelServer OU Model and Interference Model Test";
  ms_manager->PrintMessage(msg);

  // -------------------------------------------------------
  // Start the OU model test
  // -------------------------------------------------------

  std::vector<std::vector<double>> features{
      {0, 0, 10000, 4, 1, 10000, 1, 0, 0},
      {0, 0, 10000, 4, 1, 10000, 1, 0, 0},
      {0, 0, 10000, 4, 1, 10000, 1, 0, 0},
      {0, 0, 10000, 4, 1, 10000, 1, 0, 0},
  };

  // Perform a training of the opunit models with {lr, rf} as training methods.
  std::vector<std::string> methods{"lr", "rf"};
  std::string ou_model_save_path = "ou_model_map.pickle";

  ModelServerFuture<std::string> future;
  const char *env = ::getenv(BUILD_ABS_PATH);
  std::string project_build_path = (env != nullptr ? env : ".");
  ms_manager->TrainModel(ModelType::Type::OperatingUnit, methods, project_build_path + "/bin", ou_model_save_path,
                         nullptr, common::ManagedPointer<ModelServerFuture<std::string>>(&future));
  auto res = future.Wait();
  ASSERT_EQ(res.second, true);  // Training succeeds

  // Perform inference on the trained opunit model for various opunits
  auto result = ms_manager->InferOUModel(
      OpUnitToString(selfdriving::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS), ou_model_save_path, features);
  ASSERT_TRUE(result.second);
  ASSERT_EQ(result.first.size(), features.size());
  result = ms_manager->InferOUModel(OpUnitToString(selfdriving::ExecutionOperatingUnitType::OP_REAL_COMPARE),
                                    ou_model_save_path, features);
  ASSERT_TRUE(result.second);
  ASSERT_EQ(result.first.size(), features.size());
  result = ms_manager->InferOUModel(OpUnitToString(selfdriving::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY),
                                    ou_model_save_path, features);
  ASSERT_TRUE(result.second);
  ASSERT_EQ(result.first.size(), features.size());

  // Model at another path should not exist
  std::string non_exist_path("model_server_test_non_exist.pickle");
  result = ms_manager->InferOUModel(OpUnitToString(selfdriving::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS),
                                    non_exist_path, features);
  ASSERT_FALSE(result.second);

  // Inference with invalid opunit name will fail
  result = ms_manager->InferOUModel("OP_SUPER_MAGICAL_DIVIDE", ou_model_save_path, features);
  ASSERT_FALSE(result.second);

  // -------------------------------------------------------
  // Start the interference model test
  // (the interference model test cannot be a separate test because it needs the OU models during training)
  // -------------------------------------------------------

  // Each input feature vector has 27 dimensions. 4 inputs in total.
  std::vector<std::vector<double>> interference_features(
      4, std::vector<double>{3021.72995431072,     9289.31287954857,  9.13346247415147,
                             3.76759508844315,     0.972971217834767, 2.27029084406291E-18,
                             2.27029084406291E-18, 26.6452774591955,  1,
                             16190.893782285,      50564.2242540171,  46.9915652841846,
                             20.4591411058655,     5.22476606718781,  1.4160105054196E-17,
                             1.4160105054196E-17,  114.192815613623,  5.34687321575982,
                             833.981961039051,     2586.53600380169,  3.07546373087657,
                             1.11544080377168,     0.269083658337621, 9.21785213010602E-19,
                             9.21785213010602E-19, 10.8371832304431,  0.275654276087222});

  // Perform a training of the opunit models with {rf} as training methods.
  // Usually we use "nn" for the interference model. But to avoid the prediction to explode with a small amount of
  // training data, we use "rf" here.
  std::vector<std::string> interference_methods{"rf"};
  // input_path and sample_rate are specified during the data generation phase, which is before the invocation of
  // this test (see Jenkinsfile configuration)
  std::string input_path = project_build_path + "/concurrent_runner_input";
  uint64_t sample_rate = 2;
  std::string interference_model_save_path = "interference_direct_model.pickle";

  ms_manager->TrainInterferenceModel(interference_methods, input_path, interference_model_save_path, ou_model_save_path,
                                     sample_rate, common::ManagedPointer<ModelServerFuture<std::string>>(&future));
  auto interference_res = future.Wait();
  ASSERT_EQ(interference_res.second, true);  // Training succeeds

  // Perform inference on the trained opunit model for various opunits
  auto interference_result = ms_manager->InferInterferenceModel(interference_model_save_path, interference_features);
  ASSERT_TRUE(interference_result.second);
  ASSERT_EQ(interference_result.first.size(), interference_features.size());

  // Model at another path should not exist
  result = ms_manager->InferInterferenceModel(non_exist_path, interference_features);
  ASSERT_FALSE(result.second);

  // Quit
  ms_manager->StopModelServer();
}

// NOLINTNEXTLINE
TEST_F(ModelServerTest, ForecastModelTest) {
  messenger::messenger_logger->set_level(spdlog::level::info);
  model_server_logger->set_level(spdlog::level::info);

  auto primary = BuildDBMain();
  primary->GetNetworkLayer()->GetServer()->RunServer();

  auto ms_manager = primary->GetModelServerManager();

  // Wait for the model server process to start
  while (!ms_manager->ModelServerStarted()) {
  }

  // Send a message
  std::string msg = "ModelServer Forecasting Model Test";
  ms_manager->PrintMessage(msg);

  // Perform a training of the opunit models with {LSTM} as training methods.
  std::vector<std::string> methods{"LSTM"};
  uint64_t interval = 500000;
  const char *env = ::getenv(BUILD_ABS_PATH);
  std::string project_build_path = (env != nullptr ? env : ".");
  std::string save_path = "model.pickle";
  std::string input_path = project_build_path + "/query_trace.csv";

  ModelServerFuture<std::string> future;
  ms_manager->TrainForecastModel(methods, input_path, save_path, interval,
                                 common::ManagedPointer<ModelServerFuture<std::string>>(&future));
  auto res = future.Wait();
  ASSERT_EQ(res.second, true);  // Training succeeds

  // Perform inference on the trained opunit model for various opunits
  auto result = ms_manager->InferForecastModel(input_path, save_path, methods, nullptr, interval);
  ASSERT_TRUE(result.second);

  // Quit
  ms_manager->StopModelServer();
}

}  // namespace noisepage::modelserver
