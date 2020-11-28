#include "self_driving/model_server/model_server_manager.h"

#include <sys/wait.h>

#include <filesystem>
#include <thread>  // NOLINT

#include "common/json.h"
#include "loggers/model_server_logger.h"
#include "messenger/connection_destination.h"
#include "messenger/messenger.h"

namespace noisepage::modelserver {

/**
 * This initializes a connection to the model by openning up a zmq connection
 * @param messenger
 * @return A ConnectionId that should be used only to the calling thread
 */
common::ManagedPointer<messenger::ConnectionRouter> ListenAndMakeConnection(
    const common::ManagedPointer<messenger::Messenger> &messenger, const std::string &ipc_path,
    messenger::CallbackFn model_server_logic) {
  // Create an IPC connection that the Python process will talk to.
  auto destination = messenger::ConnectionDestination::MakeIPC(MODEL_TARGET_NAME, ipc_path);

  // Listen for the connection
  messenger->ListenForConnection(destination, MODEL_CONN_ID_NAME, std::move(model_server_logic));
  while (true) {
    try {
      return messenger->GetConnectionRouter(MODEL_CONN_ID_NAME);
    } catch (std::exception &e) {
      ::sleep(1);
    }
  }
}

}  // namespace noisepage::modelserver

namespace noisepage::modelserver {

ModelServerManager::ModelServerManager(const std::string &model_bin,
                                       const common::ManagedPointer<messenger::Messenger> &messenger)
    : messenger_(messenger), thd_(std::thread([this, model_bin] {
        while (!shut_down_) {
          this->StartModelServer(model_bin);
        }
      })) {
  /* Model Initialization handling logic */
  auto msm_handler = [&](common::ManagedPointer<messenger::Messenger> messenger, std::string_view sender_id,
                         std::string_view message, uint64_t recv_cb_id) {
    /* ModelServer connected */
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
        connected_ = true;
        break;
      default:
        MODEL_SERVER_LOG_WARN("Unknown callback {}", cb_id);
    }
  };
  router_ = ListenAndMakeConnection(messenger, MODEL_IPC_PATH, msm_handler);
}

void ModelServerManager::StartModelServer(const std::string &model_path) {
  py_pid_ = ::fork();
  if (py_pid_ < 0) {
    MODEL_SERVER_LOG_ERROR("Failed to fork to spawn model process");
    return;
  }

  // Fork success
  if (py_pid_ > 0) {
    // Parent Process Routine
    MODEL_SERVER_LOG_INFO("Model Server Process running at : {}", py_pid_);

    // Wait for the child to exit
    int status;
    pid_t wait_pid;

    // Wait for the child
    wait_pid = ::waitpid(py_pid_, &status, 0);

    // Oops somehow the ModelServer is now not conncted
    connected_.store(false);

    if (wait_pid < 0) {
      MODEL_SERVER_LOG_ERROR("Failed to wait for the child process...");
      return;
    }

    if (WIFEXITED(status) && WEXITSTATUS(status) == MODEL_SERVER_ERROR_BINARY) {
      MODEL_SERVER_LOG_ERROR("Stop model server");
      shut_down_.store(true);
    }
  } else {
    // Run the script in in a child
    std::string ipc_path = IPCPath();
    char exec_name[model_path.size() + 1];
    ::strncpy(exec_name, model_path.data(), sizeof(exec_name));
    char *args[] = {exec_name, ipc_path.data(), nullptr};
    MODEL_SERVER_LOG_TRACE("Inovking ModelServer at :{}", std::string(exec_name));
    if (execvp(args[0], args) < 0) {
      MODEL_SERVER_LOG_ERROR("Failed to execute model binary: {}, {}", strerror(errno), errno);
      /* Shutting down */
      ::_exit(MODEL_SERVER_ERROR_BINARY);
    }
  }
}

void ModelServerManager::PrintMessage(const std::string &msg) {
  nlohmann::json j;
  j["cmd"] = "PRINT";
  j["data"]["message"] = msg;
  try {
    messenger_->SendMessage(router_, MODEL_TARGET_NAME, j.dump(), messenger::CallbackFns::Noop, 0);
  } catch (std::exception &e) {
    MODEL_SERVER_LOG_WARN("[PID={}] ModelServerManager failed to PrintMessage {} to the python-ModelServer. Error: {}",
                          ::getpid(), j.dump(), e.what());
  }
}

void ModelServerManager::StopModelServer() {
  if (!shut_down_) {
    shut_down_ = true;
    nlohmann::json j;
    j["cmd"] = "QUIT";
    j["data"] = "";
    try {
      messenger_->SendMessage(router_, MODEL_TARGET_NAME, j.dump(), messenger::CallbackFns::Noop, 0);
    } catch (std::exception &e) {
      MODEL_SERVER_LOG_WARN("[PID={}] ModelServerManager failed to StopModelServer. Error: {}", ::getpid(), e.what());
    }
  }
  if (thd_.joinable()) thd_.join();
}

void ModelServerManager::TrainWith(const std::vector<std::string> &models, const std::string &seq_files_dir,
                                   ModelServerFuture<std::string> *future) {
  nlohmann::json j;
  j["cmd"] = "TRAIN";
  j["data"]["models"] = models;
  j["data"]["seq_files"] = seq_files_dir;
  j["data"]["raw_data"] = "";

  auto callback = [&, future](common::ManagedPointer<messenger::Messenger> messenger, std::string_view sender_id,
                              std::string_view message, uint64_t recv_cb_id) {
    MODEL_SERVER_LOG_INFO("Callback :recv_cb_id={}, message={}", recv_cb_id, message);
    nlohmann::json res = nlohmann::json::parse(message);
    // Deserialize the message result
    auto result = res.at("result").get<std::string>();

    future->Done(result);
  };
  try {
    messenger_->SendMessage(router_, MODEL_TARGET_NAME, j.dump(), callback, 0);
  } catch (std::exception &e) {
    MODEL_SERVER_LOG_WARN("[PID={}] ModelServerManager failed to invoke TrainWith. Error: {}", ::getpid(), e.what());
  }
}

void ModelServerManager::TrainWith(
    const std::vector<std::string> &models,
    std::unordered_map<selfdriving::ExecutionOperatingUnitType,
                       std::pair<std::vector<std::vector<double>>, std::vector<std::vector<double>>>> &raw_data) {
  nlohmann::json j;
  j["cmd"] = "TRAIN";
  j["data"]["models"] = models;
  j["data"]["seq_files"] = "";
  j["data"]["raw_data"] = raw_data;
  try {
    messenger_->SendMessage(router_, MODEL_TARGET_NAME, j.dump(), messenger::CallbackFns::Noop, 0);
  } catch (std::exception &e) {
    MODEL_SERVER_LOG_WARN("[PID={}] ModelServerManager failed to invoke TrainWith. Error: {}", ::getpid(), e.what());
  }
}

std::vector<std::vector<double>> ModelServerManager::DoInference(const std::string &opunit,
                                                                 const std::vector<std::vector<double>> &features) {
  nlohmann::json j;
  j["cmd"] = "INFER";
  j["data"]["opunit"] = opunit;
  j["data"]["features"] = features;

  // Sync communication
  ModelServerFuture<std::vector<std::vector<double>>> future;

  auto callback = [&](common::ManagedPointer<messenger::Messenger> messenger, std::string_view sender_id,
                      std::string_view message, uint64_t recv_cb_id) {
    MODEL_SERVER_LOG_INFO("Callback :recv_cb_id={}, message={}", recv_cb_id, message);
    nlohmann::json res = nlohmann::json::parse(message);
    // Deserialize the message result
    std::vector<std::vector<double>> result = res.at("result").get<std::vector<std::vector<double>>>();

    future.Done(result);
  };

  // Register NOOP at the messenger
  try {
    messenger_->SendMessage(router_, MODEL_TARGET_NAME, j.dump(), callback, 0);
  } catch (std::exception &e) {
    MODEL_SERVER_LOG_WARN("[PID={}] ModelServerManager failed to invoke DoInference. Error: {}", ::getpid(), e.what());
  }

  return future.Wait().first;
}

}  // namespace noisepage::modelserver
