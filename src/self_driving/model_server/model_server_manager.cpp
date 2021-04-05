#include "self_driving/model_server/model_server_manager.h"

#if __APPLE__
// macOS doesn't have prctl.h, but needs csignal for ::kill.
#include <csignal>
#else
#include <sys/prctl.h>
#endif
#include <sys/wait.h>

#include <thread>  // NOLINT

#include "common/json.h"
#include "loggers/model_server_logger.h"
#include "messenger/connection_destination.h"
#include "messenger/messenger.h"
#include "self_driving/forecasting/workload_forecast.h"

namespace noisepage::modelserver {
static constexpr const char *MODEL_CONN_ID_NAME = "model-server-conn";
static constexpr const char *MODEL_TARGET_NAME = "model";
static constexpr const char *MODEL_IPC_PATH = "model-server-ipc";
static constexpr const char *COVERAGE_COMMAND = "coverage";
static constexpr const char *COVERAGE_RUN = "run";
// used to generate a unique coverage file name and then potentially combine multiple coverage files together
static constexpr const char *COVERAGE_PARALLEL = "-p";
static constexpr const char *COVERAGE_INCLUDE = "--include";
static constexpr const char *COVERAGE_INCLUDE_PATH = "*/script/self_driving/*";

/**
 * Use 128 as convention to indicate failure in a subprocess:
 * https://www.gnu.org/software/libc/manual/html_node/Exit-Status.html
 */
static constexpr const unsigned char MODEL_SERVER_SUBPROCESS_ERROR = 128;

common::ManagedPointer<messenger::ConnectionRouter> ListenAndMakeConnection(
    const common::ManagedPointer<messenger::Messenger> &messenger, const std::string &ipc_path,
    messenger::CallbackFn model_server_logic) {
  // Create an IPC connection that the Python process will talk to.
  auto destination = messenger::ConnectionDestination::MakeIPC(MODEL_TARGET_NAME, ipc_path);

  // Listen for the connection
  messenger->ListenForConnection(destination, MODEL_CONN_ID_NAME, std::move(model_server_logic));
  return messenger->GetConnectionRouter(MODEL_CONN_ID_NAME);
}

}  // namespace noisepage::modelserver

namespace noisepage::modelserver {

ModelServerManager::ModelServerManager(const std::string &model_bin,
                                       const common::ManagedPointer<messenger::Messenger> &messenger,
                                       bool enable_python_coverage)
    : messenger_(messenger), thd_(std::thread([this, &model_bin, enable_python_coverage] {
        while (!shut_down_) {
          this->StartModelServer(model_bin, enable_python_coverage);
        }
      })) {
  // Model Initialization handling logic
  auto msm_handler = [&](common::ManagedPointer<messenger::Messenger> messenger, const messenger::ZmqMessage &msg) {
    uint64_t sender_id UNUSED_ATTRIBUTE = msg.GetSourceCallbackId();
    uint64_t recv_cb_id UNUSED_ATTRIBUTE = msg.GetDestinationCallbackId();
    std::string_view message = msg.GetMessage();

    // ModelServer connected
    MODEL_SERVER_LOG_TRACE("[PID={},SENDER_ID={}] Messenger RECV: {}, {}", ::getpid(), sender_id, message, recv_cb_id);

    Callback cb_id;
    try {
      nlohmann::json j = nlohmann::json::parse(message);
      cb_id = j.at("action").get<Callback>();
    } catch (nlohmann::json::exception &e) {
      MODEL_SERVER_LOG_WARN("Wrong message format, missing action field: {}, {}", message, e.what());
      return;
    }

    // Check for ModelServerManager specific callback actions
    switch (cb_id) {
      case Callback::NOOP:
        break;
      case Callback::CONNECTED:
        MODEL_SERVER_LOG_INFO("[PID={}] ModelServer connected", ::getpid());
        connected_ = true;
        break;
      default:
        MODEL_SERVER_LOG_WARN("Unknown callback {}", cb_id);
    }
  };
  router_ = ListenAndMakeConnection(messenger, MODEL_IPC_PATH, msm_handler);
}

void ModelServerManager::StartModelServer(const std::string &model_path, bool enable_python_coverage) {
#if __APPLE__
  // do nothing
#else
  pid_t parent_pid = ::getpid();
#endif
  py_pid_ = ::fork();
  if (py_pid_ < 0) {
    MODEL_SERVER_LOG_ERROR("Failed to fork to spawn model process");
    return;
  }

  // Fork success
  // TODO(lin): The LightGBM (Python) library has a known hanging issue when multithreading and using forking in
  //  Linux at the same time:
  //  https://lightgbm.readthedocs.io/en/latest/FAQ
  //  .html#lightgbm-hangs-when-multithreading-openmp-and-using-forking-in-linux-at-the-same-time
  //  So this may cause a subsequent command that issues a model training using LightGBM under multi-threading to
  //  hang (not always). Don't have a good solution right now.
  if (py_pid_ > 0) {
    // Parent Process Routine
    MODEL_SERVER_LOG_INFO("Model Server Process running at : {}", py_pid_);

    // Wait for the child to exit
    int status;
    pid_t wait_pid;

    // Wait for the child
    wait_pid = ::waitpid(py_pid_, &status, 0);

    if (wait_pid < 0) {
      MODEL_SERVER_LOG_ERROR("Failed to wait for the child process...");
      return;
    }

    // waitpid() returns indicating the ModelServer child process no longer active, thus disconnected
    connected_.store(false);

    if (WIFEXITED(status) && WEXITSTATUS(status) == MODEL_SERVER_SUBPROCESS_ERROR) {
      MODEL_SERVER_LOG_ERROR("Stop model server");
      shut_down_.store(true);
    }
  } else {
    // Child process. Run the ModelServer Python script.
#if __APPLE__
    // do nothing
#else
    // Install the parent death signal on the child process
    int status = ::prctl(PR_SET_PDEATHSIG, SIGTERM);
    if (status == -1) {
      MODEL_SERVER_LOG_ERROR("Failed to install parent death signal");
      ::_exit(MODEL_SERVER_SUBPROCESS_ERROR);
    }
    if (getppid() != parent_pid) {
      // The original parent exited just before the prctl() call
      MODEL_SERVER_LOG_ERROR("Parent exited before prctl() call");
      ::_exit(MODEL_SERVER_SUBPROCESS_ERROR);
    }
#endif
    std::string ipc_path = MODEL_IPC_PATH;
    char exec_name[model_path.size() + 1];
    ::strncpy(exec_name, model_path.data(), sizeof(exec_name));
    // Args to set up Python code coverage then execute model server
    std::string coverage_command = COVERAGE_COMMAND;
    std::string coverage_run = COVERAGE_RUN;
    std::string coverage_parallel = COVERAGE_PARALLEL;
    std::string coverage_include = COVERAGE_INCLUDE;
    std::string coverage_include_path = COVERAGE_INCLUDE_PATH;
    char *coverage_args[] = {
        coverage_command.data(),      coverage_run.data(), coverage_parallel.data(), coverage_include.data(),
        coverage_include_path.data(), exec_name,           ipc_path.data(),          nullptr};
    // Args to directly execute model server
    char *direct_args[] = {exec_name, ipc_path.data(), nullptr};
    MODEL_SERVER_LOG_TRACE("Inovking ModelServer at :{}", std::string(exec_name));
    // It's tricky to assign to char *[], so we just invoke the commands with/without coverage separately
    if (enable_python_coverage) {
      if (execvp(coverage_args[0], coverage_args) < 0) {
        MODEL_SERVER_LOG_ERROR("Failed to execute model binary: {}, {}", strerror(errno), errno);
        // Shutting down
        ::_exit(MODEL_SERVER_SUBPROCESS_ERROR);
      }
    } else {
      if (execvp(direct_args[0], direct_args) < 0) {
        MODEL_SERVER_LOG_ERROR("Failed to execute model binary: {}, {}", strerror(errno), errno);
        // Shutting down
        ::_exit(MODEL_SERVER_SUBPROCESS_ERROR);
      }
    }
  }
}

bool ModelServerManager::SendMessage(const std::string &payload, messenger::CallbackFn cb) {
  try {
    messenger_->SendMessage(router_, MODEL_TARGET_NAME, payload, std::move(cb),
                            static_cast<uint64_t>(messenger::Messenger::BuiltinCallback::NOOP));
    return true;
  } catch (std::exception &e) {
    MODEL_SERVER_LOG_WARN("[PID={}] ModelServerManager failed to send message: {}. Error: {}", ::getpid(), payload,
                          e.what());
    return false;
  }
}

bool ModelServerManager::PrintMessage(const std::string &msg) {
  nlohmann::json j;
  j["cmd"] = "PRINT";
  j["data"]["message"] = msg;

  return SendMessage(j.dump(), messenger::CallbackFns::Noop);
}

void ModelServerManager::StopModelServer() {
  if (!shut_down_) {
    shut_down_ = true;
    nlohmann::json j;
    j["cmd"] = "QUIT";
    j["data"] = "";
    if (!SendMessage(j.dump(), messenger::CallbackFns::Noop)) {
      MODEL_SERVER_LOG_WARN("Failed to send QUIT message to ModelServer. Forced shutting down");
      ::kill(py_pid_, SIGTERM);
    }
  }
  if (thd_.joinable()) thd_.join();
}

bool ModelServerManager::TrainModel(ModelType::Type model, const std::vector<std::string> &methods,
                                    const std::string &input_path, const std::string &save_path,
                                    nlohmann::json *arguments,
                                    common::ManagedPointer<ModelServerFuture<std::string>> future) {
  nlohmann::json j;
  j["cmd"] = "TRAIN";
  if (arguments != nullptr) {
    j["data"] = *arguments;
  } else {
    j["data"] = {};
  }
  j["data"]["type"] = ModelType::TypeToString(model);
  j["data"]["methods"] = methods;
  j["data"]["input_path"] = input_path;
  j["data"]["save_path"] = save_path;

  // Callback to notify the waiter for result, or failure to parse the result.
  auto callback = [&, future](common::ManagedPointer<messenger::Messenger> messenger,
                              const messenger::ZmqMessage &msg) {
    MODEL_SERVER_LOG_INFO("Callback :recv_cb_id={}, message={}", msg.GetDestinationCallbackId(), msg.GetMessage());
    future->Done(msg.GetMessage());
  };

  return SendMessage(j.dump(), callback);
}

bool ModelServerManager::TrainForecastModel(const std::vector<std::string> &methods, const std::string &input_path,
                                            const std::string &save_path, uint64_t interval_micro,
                                            common::ManagedPointer<ModelServerFuture<std::string>> future) {
  nlohmann::json j;
  j["interval_micro_sec"] = interval_micro;
  return TrainModel(ModelType::Type::Forecast, methods, input_path, save_path, &j, future);
}

bool ModelServerManager::TrainInterferenceModel(const std::vector<std::string> &methods, const std::string &input_path,
                                                const std::string &save_path, const std::string &ou_model_path,
                                                uint64_t pipeline_metrics_sample_rate,
                                                common::ManagedPointer<ModelServerFuture<std::string>> future) {
  nlohmann::json j;
  j["pipeline_metrics_sample_rate"] = pipeline_metrics_sample_rate;
  j["ou_model_path"] = ou_model_path;
  return TrainModel(ModelType::Type::Interference, methods, input_path, save_path, &j, future);
}

template <class Result>
std::pair<Result, bool> ModelServerManager::InferModel(ModelType::Type model, const std::string &model_path,
                                                       nlohmann::json *payload) {
  nlohmann::json j;
  j["cmd"] = "INFER";
  if (payload) {
    j["data"] = *payload;
  } else {
    j["data"] = {};
  }
  j["data"]["type"] = ModelType::TypeToString(model);
  j["data"]["model_path"] = model_path;

  // Sync communication
  ModelServerFuture<Result> future;

  // Callback to notify waiter with result
  auto callback = [&](common::ManagedPointer<messenger::Messenger> messenger, const messenger::ZmqMessage &msg) {
    MODEL_SERVER_LOG_DEBUG("Callback :recv_cb_id={}, message={}", msg.GetDestinationCallbackId(), msg.GetMessage());
    future.Done(msg.GetMessage());
  };

  // Fail to send the message
  if (!SendMessage(j.dump(), callback)) {
    return {{}, false};
  }

  return future.Wait();
}

std::pair<std::vector<std::vector<double>>, bool> ModelServerManager::InferOUModel(
    const std::string &opunit, const std::string &model_path, const std::vector<std::vector<double>> &features) {
  nlohmann::json j;
  j["opunit"] = opunit;
  j["features"] = features;
  return InferModel<std::vector<std::vector<double>>>(ModelType::Type::OperatingUnit, model_path, &j);
}

std::pair<selfdriving::WorkloadForecastPrediction, bool> ModelServerManager::InferForecastModel(
    const std::string &input_path, const std::string &model_path, const std::vector<std::string> &model_names,
    std::string *models_config, uint64_t interval_micro_sec) {
  nlohmann::json j;
  j["input_path"] = input_path;
  j["model_names"] = model_names;
  j["interval_micro_sec"] = interval_micro_sec;
  if (models_config != nullptr) {
    j["models_config"] = *models_config;
  }

  selfdriving::WorkloadForecastPrediction result;
  auto data = InferModel<std::map<std::string, std::map<std::string, std::vector<double>>>>(ModelType::Type::Forecast,
                                                                                            model_path, &j);
  for (auto &cid_pair : data.first) {
    std::unordered_map<uint64_t, std::vector<double>> cid_data;
    for (auto &qid_pair : cid_pair.second) {
      cid_data[std::stoi(qid_pair.first, nullptr)] = std::move(qid_pair.second);
    }
    result[std::stoi(cid_pair.first, nullptr)] = std::move(cid_data);
  }
  return {result, data.second};
}

std::pair<std::vector<std::vector<double>>, bool> ModelServerManager::InferInterferenceModel(
    const std::string &model_path, const std::vector<std::vector<double>> &features) {
  nlohmann::json j;
  j["features"] = features;
  return InferModel<std::vector<std::vector<double>>>(ModelType::Type::Interference, model_path, &j);
}

}  // namespace noisepage::modelserver
