#include "self_driving/model_server/model_server_manager.h"

#if __APPLE__
// nothing to include since macOS doesn't have prctl.h
#else
#include <sys/prctl.h>
#endif
#include <sys/wait.h>
#include <thread>  // NOLINT

#include "common/json.h"
#include "loggers/model_server_logger.h"
#include "messenger/connection_destination.h"
#include "messenger/messenger.h"

namespace noisepage::modelserver {
static constexpr const char *MODEL_CONN_ID_NAME = "model-server-conn";
static constexpr const char *MODEL_TARGET_NAME = "model";
static constexpr const char *MODEL_IPC_PATH = "model-server-ipc";

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

  // TODO(ricky): pass in a cvar so that the messenger could signal that the router has been added.
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
    : messenger_(messenger), thd_(std::thread([this, &model_bin] {
        while (!shut_down_) {
          this->StartModelServer(model_bin);
        }
      })) {
  // Model Initialization handling logic
  auto msm_handler = [&](common::ManagedPointer<messenger::Messenger> messenger, std::string_view sender_id,
                         std::string_view message, uint64_t recv_cb_id) {
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

void ModelServerManager::StartModelServer(const std::string &model_path) {
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
    char *args[] = {exec_name, ipc_path.data(), nullptr};
    MODEL_SERVER_LOG_TRACE("Inovking ModelServer at :{}", std::string(exec_name));
    if (execvp(args[0], args) < 0) {
      MODEL_SERVER_LOG_ERROR("Failed to execute model binary: {}, {}", strerror(errno), errno);
      // Shutting down
      ::_exit(MODEL_SERVER_SUBPROCESS_ERROR);
    }
  }
}

bool ModelServerManager::SendMessage(const std::string &payload, messenger::CallbackFn cb) {
  try {
    messenger_->SendMessage(router_, MODEL_TARGET_NAME, payload, std::move(cb), 0);
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

bool ModelServerManager::TrainWith(const std::vector<std::string> &methods, const std::string &seq_files_dir,
                                   const std::string &save_path,
                                   common::ManagedPointer<ModelServerFuture<std::string>> future) {
  nlohmann::json j;
  j["cmd"] = "TRAIN";
  j["data"]["methods"] = methods;
  j["data"]["seq_files"] = seq_files_dir;
  j["data"]["save_path"] = save_path;

  // Callback to notify the waiter for result, or failure to parse the result.
  auto callback = [&, future](common::ManagedPointer<messenger::Messenger> messenger, std::string_view sender_id,
                              std::string_view message, uint64_t recv_cb_id) {
    MODEL_SERVER_LOG_INFO("Callback :recv_cb_id={}, message={}", recv_cb_id, message);
    future->Done(message);
  };

  return SendMessage(j.dump(), callback);
}

std::pair<std::vector<std::vector<double>>, bool> ModelServerManager::DoInference(
    const std::string &opunit, const std::string &model_path, const std::vector<std::vector<double>> &features) {
  nlohmann::json j;
  j["cmd"] = "INFER";
  j["data"]["opunit"] = opunit;
  j["data"]["features"] = features;
  j["data"]["model_path"] = model_path;

  // Sync communication
  ModelServerFuture<std::vector<std::vector<double>>> future;

  // Callback to notify waiter with result
  auto callback = [&](common::ManagedPointer<messenger::Messenger> messenger, std::string_view sender_id,
                      std::string_view message, uint64_t recv_cb_id) {
    MODEL_SERVER_LOG_INFO("Callback :recv_cb_id={}, message={}", recv_cb_id, message);
    future.Done(message);
  };

  // Fail to send the message
  if (!SendMessage(j.dump(), callback)) {
    return {{}, false};
  }

  return future.Wait();
}

}  // namespace noisepage::modelserver
