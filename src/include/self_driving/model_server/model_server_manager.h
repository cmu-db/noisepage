/**
 *  The ModelServerManager is responsible for starting up, stopping, and communicating with the ModelServer.
 *  Currently, the operations supported are:
 *  - Training an Opunit model map from a sequence file directory.
 *  - Inferencing on one trained Opunit model with features.
 *  - Sending string message to the ModelServer
 *  - Quiting the ModelServer
 *
 *  The ModelServerManager will restart the ModelServer once the ModelServer goes down. Models trained will persist
 *  across a ModelServer's restart. So ModelServer failure handling will be transparent to users.
 */

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "common/json.h"
#include "common/managed_pointer.h"

namespace noisepage::messenger {
class Messenger;
}  // namespace noisepage::messenger

namespace noisepage::modelserver {

/**
 * ModelServerFuture is a wrapper over a condition_variable, and the condition.
 *
 * It is used to build synchronous API over asynchronous function calls:
 * ```c++
 *      ModelServerFuture future;
 *      AsyncCall(future);
 *
 *      auto result = future.Wait();
 *      bool success = result.first;
 *      auto data = result.second;
 * ```
 *
 * @tparam Result the type of the future's result
 */
template <class Result>
class ModelServerFuture {
 public:
  /**
   * Initialize a future object
   */
  ModelServerFuture() = default;

  /**
   * Suspends the current thread and wait for the result to be ready
   * @return Result, and success/fail
   */
  std::pair<Result, bool> Wait() {
    {
      std::unique_lock<std::mutex> lock(mtx_);

      // Wait until the future is completed by someone with successful result or failure
      cvar_.wait(lock, [&] { return done_.load(); });
    }

    return {result_, success_};
  }

  /**
   * Indicate a future is done by parsing the message from the ModelServer
   * this will unblock waiters that have called future->Wait()
   *
   * @param message
   */
  void Done(std::string_view message) {
    try {
      nlohmann::json res = nlohmann::json::parse(message);
      // Deserialize the message result
      auto result = res.at("result").get<Result>();
      auto success = res.at("success").get<bool>();
      auto err = res.at("err").get<std::string>();
      if (success) {
        Success(result);
      } else {
        Fail(err);
      }
    } catch (nlohmann::json::exception &e) {
      Fail("WRONG_RESULT_FORMAT");
    }
  }

  /**
   * Indicate this future is done with result supplied by the ModelServer
   * @param result ModelServer's response
   */
  void Success(const Result &result) {
    {
      std::unique_lock<std::mutex> lock(mtx_);
      result_ = result;
      done_ = true;
      success_ = true;
    }

    // A future will only be completed by one thread, but there could be waited by multiple threads.
    // An example could be multiple threads waiting for the training process completion.
    cvar_.notify_all();
  }

  /**
   * Indicate this future fails to retrieve expected results from the asynchronous call to the ModelServer.
   * It could either be an error on the ModelServer, or failure of sending message by the Messenger
   */
  void Fail(const std::string &reason) {
    {
      std::unique_lock<std::mutex> lock(mtx_);
      done_ = true;
      success_ = false;
      fail_msg_ = reason;
    }
    cvar_.notify_all();
  }

  /**
   * @return A message describing why the operation failed
   */
  const std::string &FailMessage() const { return fail_msg_; }

 private:
  /** Result for the future */
  Result result_;

  /** Condition variable for waiter of this future to wait for it being ready */
  std::condition_variable cvar_;

  /** True If async operation done */
  std::atomic<bool> done_ = false;

  /** True If async operation succeeds */
  std::atomic<bool> success_ = false;

  /** Reason for failure */
  std::string fail_msg_;

  /** Mutex associated with the condition variable */
  std::mutex mtx_;
};

/**
 * This initializes a connection to the model by opening up a zmq connection
 * @param messenger
 * @return A ConnectionId that should be used only to the calling thread
 */
common::ManagedPointer<messenger::ConnectionRouter> ListenAndMakeConnection(
    const common::ManagedPointer<messenger::Messenger> &messenger, const std::string &ipc_path,
    messenger::CallbackFn model_server_logic);

/**
 * Interface for ModelServerManager related operations
 */
class ModelServerManager {
  static constexpr const int INVALID_PID = 0;
  enum class Callback : uint64_t { NOOP = 0, CONNECTED, DEFAULT };

 public:
  /**
   * Construct a ModelServerManager with the given executable script to the Python ModelServer
   * @param model_bin Python script path
   * @param messenger Messenger pointer
   */
  ModelServerManager(const std::string &model_bin, const common::ManagedPointer<messenger::Messenger> &messenger);

  /**
   * Stop the Python ModelServer when exits
   */
  ~ModelServerManager() { StopModelServer(); }

  /**
   * Stop the Python-ModelServer daemon by sending a message to the Python model server
   */
  void StopModelServer();

  /**
   * Check if the model server has started.
   * The user of this function should poll this until this returns True.
   *
   * A true return value means the model server script is running and ready to receive message.
   *
   * @return true if model server has started
   */
  bool ModelServerStarted() const { return connected_; }

  /**
   * Get the Python model-server's PID
   * @return  pid
   */
  pid_t GetModelPid() const { return py_pid_; }

  /*******************************************************
   * ModelServer <-> ModelServerManager logic routines
   *******************************************************/

  /**
   * Ask the model server to print a message
   * This function does not expect a callback
   *
   * This function is a blocking API call to the ModelServer, and only returns when result is sent back.
   *
   * @param msg Message to print
   * @return True if sending succeeds
   */
  bool PrintMessage(const std::string &msg);

  /**
   * Train a model with the given seq files directory
   *
   *  This function will be invoked asynchronously.
   *  The caller should wait on the future if it wants to synchronize with the training process.
   *
   *  The caller should use the save_path as a handle to the trained model for inference later on.
   *
   * @param methods list of candidates methods that will be used for training
   * @param save_path path to where the trained model map will be stored at
   * @param seq_files_dir Seq files's enclosing directory (seq files generated by mini_runner)
   * @param future A future object which the caller waits for training to be done
   * @return True if sending train request suceeds
   */
  bool TrainWith(const std::vector<std::string> &methods, const std::string &seq_files_dir,
                 const std::string &save_path, common::ManagedPointer<ModelServerFuture<std::string>> future);

  /**
   * Perform inference on the given data file.
   *
   * This function is a blocking API call to the ModelServer, and only returns when result is sent back.
   *
   * @param opunit Model for which to invoke
   * @param model_path Path to a model that has been trained. (In pickle format)
   * @param features Feature vectors
   * @return a vector of results returned by ModelServer and if API succeeds (True when succeeds)
   *    When API fails, the return results will be an empty vector
   */
  std::pair<std::vector<std::vector<double>>, bool> DoInference(const std::string &opunit,
                                                                const std::string &model_path,
                                                                const std::vector<std::vector<double>> &features);

 private:
  /**
   * This should be run as a thread routine.
   * 1. Make connection with the messenger
   * 2. Prepare arguments and forks to initialize a Python daemon
   * 3. Record the pid
   */
  void StartModelServer(const std::string &model_path);

  /**
   * A customized ModelServerManager server callback
   * @param messenger Messenger handle
   * @param sender_id  The sender's id (from the other end of communication)
   * @param message  Message string content in JSON format
   * @param recv_cb_id  Id of Callback to be invoked, will be interpreted by the server callback
   */
  static void ModelServerHandler(common::ManagedPointer<messenger::Messenger> messenger, std::string_view sender_id,
                                 std::string_view message, uint64_t recv_cb_id);

  /**
   * Send a marshalled message string in JSON format through the Messenger
   * @param payload serialized JSON message payload
   * @param cb callback to invoke when receive a reply as a result of this message
   * @return True if message sent successfully
   */
  bool SendMessage(const std::string &payload, messenger::CallbackFn cb);

  /** Messenger handler */
  common::ManagedPointer<messenger::Messenger> messenger_;

  /** Connection router */
  common::ManagedPointer<messenger::ConnectionRouter> router_;

  /** Thread the ModelServerManager runs in */
  std::thread thd_;

  /** Python model pid */
  pid_t py_pid_ = INVALID_PID;

  /** Bool shutting down */
  std::atomic<bool> shut_down_ = false;

  /** If ModelServer is connected */
  std::atomic<bool> connected_ = false;
};

}  // namespace noisepage::modelserver
