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

#include <atomic>
#include <condition_variable>  // NOLINT
#include <string>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include "common/json.h"
#include "common/managed_pointer.h"
#include "messenger/messenger_defs.h"
#include "self_driving/forecasting/workload_forecast.h"

namespace noisepage::messenger {
class ConnectionRouter;
class Messenger;
}  // namespace noisepage::messenger

namespace noisepage::modelserver {

/**
 * Describes the type of model to be interacted with using the ModelServerManager.
 */
class ModelType {
 public:
  /**
   * Enum describing the specific type of model (i.e., forecast, operating unit, interference)
   */
  enum class Type : uint32_t { Forecast, OperatingUnit, Interference };

  /**
   * Converts the Type enum to a readable string format
   * @param t Type to convert
   * @return string representation
   */
  static std::string TypeToString(Type t) {
    switch (t) {
      case Type::Forecast:
        return "FORECAST";
      case Type::OperatingUnit:
        return "OPERATING_UNIT";
      case Type::Interference:
        return "INTERFERENCE";
      default:
        NOISEPAGE_ASSERT(false, "Invalid ModelType::Type");
        return "";
    }
  }
};

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
   * @param enable_python_coverage Whether to enable the Python code coverage. Should only be true in tests.
   */
  ModelServerManager(const std::string &model_bin, const common::ManagedPointer<messenger::Messenger> &messenger,
                     bool enable_python_coverage);

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
   * Train a model
   *
   * This function will be invoked asynchronously.
   * The caller should wait on the future if it wants to synchronize with the training process.
   *
   * The caller should use the save_path as a handle to the trained model for inference later on.
   *
   * @param model model type to train
   * @param methods list of candidates methods that will be used for training
   * @param input_path Path to input files for training model (seq file directory for MiniRunnerModel)
   * @param save_path path to where the trained model will be stored at
   * @param arguments Extra arguments to pass
   * @param future A future object which the caller waits for training to be done
   * @return True if sending train request suceeds
   */
  bool TrainModel(ModelType::Type model, const std::vector<std::string> &methods, const std::string &input_path,
                  const std::string &save_path, nlohmann::json *arguments,
                  common::ManagedPointer<ModelServerFuture<std::string>> future);

  /**
   * Train an interference model
   *
   * This function will be invoked asynchronously.
   * The caller should wait on the future if it wants to synchronize with the training process.
   *
   * The caller should use the save_path as a handle to the trained model for inference later on.
   *
   * @param methods list of candidates methods that will be used for training
   * @param input_path Path to input files for training model (seq file directory for MiniRunnerModel)
   * @param save_path path to where the trained interference model will be stored at
   * @param ou_model_path path to where the trained OU model map is stored at
   * @param pipeline_metrics_sample_rate Sample rate percentage for the pipeline metrics
   * @param future A future object which the caller waits for training to be done
   * @return True if sending train request suceeds
   */
  bool TrainInterferenceModel(const std::vector<std::string> &methods, const std::string &input_path,
                              const std::string &save_path, const std::string &ou_model_path,
                              uint64_t pipeline_metrics_sample_rate,
                              common::ManagedPointer<ModelServerFuture<std::string>> future);

  /**
   * Train a forecast model
   *
   * This function will be invoked asynchronously.
   * The caller should wait on the future if it wants to synchronize with the training process.
   *
   * The caller should use the save_path as a handle to the trained model for inference later on.
   *
   * @param methods list of candidates methods that will be used for training
   * @param input_path Path to input files for training model (seq file directory for MiniRunnerModel)
   * @param save_path path to where the trained model map will be stored at
   * @param interval_micro interval in microseconds
   * @param future A future object which the caller waits for training to be done
   * @return True if sending train request suceeds
   */
  bool TrainForecastModel(const std::vector<std::string> &methods, const std::string &input_path,
                          const std::string &save_path, uint64_t interval_micro,
                          common::ManagedPointer<ModelServerFuture<std::string>> future);

  /**
   * Perform inference on the given data file using a forecast model
   *
   * This function is a blocking API call to the ModelServer, and only returns when result is sent back.
   *
   * @param input_path Input path to query trace file
   * @param model_path Path to a model that has been trained. (In pickle format)
   * @param model_names List of model names to train
   * @param models_config Optional parameter for model config
   * @param interval_micro_sec Forecast interval in microseconds
   * @return a map<cluster_id, map<query_id, vector<segment predictions>>>  returned by ModelServer and
   *    if API succeeds (True when succeeds). When API fails, the return results will be an empty map
   */
  std::pair<selfdriving::WorkloadForecastPrediction, bool> InferForecastModel(
      const std::string &input_path, const std::string &model_path, const std::vector<std::string> &model_names,
      std::string *models_config, uint64_t interval_micro_sec);

  /**
   * Perform inference on the given data file using an OU model
   *
   * This function is a blocking API call to the ModelServer, and only returns when result is sent back.
   *
   * @param opunit Model for which to invoke
   * @param model_path Path to a model that has been trained. (In pickle format)
   * @param features Feature vectors
   * @return a vector of results returned by ModelServer and if API succeeds (True when succeeds)
   *    When API fails, the return results will be an empty vector
   */
  std::pair<std::vector<std::vector<double>>, bool> InferOUModel(const std::string &opunit,
                                                                 const std::string &model_path,
                                                                 const std::vector<std::vector<double>> &features);

  /**
   * Perform inference on the given data file using the interference model
   *
   * This function is a blocking API call to the ModelServer, and only returns when result is sent back.
   *
   * @param model_path Path to a model that has been trained. (In pickle format)
   * @param features Feature vectors
   * @return a vector of results returned by ModelServer and if API succeeds (True when succeeds)
   *    When API fails, the return results will be an empty vector
   */
  std::pair<std::vector<std::vector<double>>, bool> InferInterferenceModel(
      const std::string &model_path, const std::vector<std::vector<double>> &features);

 private:
  /**
   * Perform inference
   *
   * This function is a blocking API call to the ModelServer, and only returns when result is sent back.
   *
   * @param model type of model to invoke (i.e., forecast or mini-runner)
   * @param model_path Path to a model that has been trained. (In pickle format)
   * @param payload Payload to pass as the "data" field to the ModelServer
   * @return pair comprising the result and a bool flag for success/failure
   */
  template <class Result>
  std::pair<Result, bool> InferModel(ModelType::Type model, const std::string &model_path, nlohmann::json *payload);

  /**
   * This should be run as a thread routine.
   * 1. Make connection with the messenger
   * 2. Prepare arguments and forks to initialize a Python daemon
   * 3. Record the pid
   */
  void StartModelServer(const std::string &model_path, bool enable_python_coverage);

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
