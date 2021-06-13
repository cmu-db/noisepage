#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/exec_defs.h"
#include "metrics/query_trace_metric.h"
#include "self_driving/forecasting/workload_forecast.h"

namespace noisepage {

namespace metrics {
class MetricsThread;
}

namespace modelserver {
class ModelServerManager;
}

namespace settings {
class SettingsManager;
}

namespace task {
class TaskManager;
}

}  // namespace noisepage

namespace noisepage::selfdriving {

/**
 * Class that handles the training and inference of workload forecasts
 */
class Forecaster {
 public:
  /** The default timeout for training forecasting models. */
  static constexpr std::chrono::seconds TRAIN_FUTURE_TIMEOUT{300};
  /** The default timeout for executing internal queries. */
  static constexpr std::chrono::seconds INTERNAL_QUERY_FUTURE_TIMEOUT{2};

  /** Describes how the workload forecast should be initialized */
  enum class WorkloadForecastInitMode : uint8_t {
    /**
     * Construct the workload forecast solely from data stored in internal tables.
     * Passes data read from internal tables to perform inference
     */
    INTERNAL_TABLES_WITH_INFERENCE,

    /**
     * Construct the workload forecast by inferencing data located on disk.
     * The inference result is not stored to internal tables in this mode.
     */
    DISK_WITH_INFERENCE,

    /**
     * Construct the workload forecast directly from data on disk.
     * No inference is performed in this case.
     */
    DISK_ONLY
  };

  /**
   * Constructor for Forecaster
   * @param forecast_model_save_path forecast model save path
   * @param metrics_thread metrics thread for metrics manager
   * @param model_server_manager model server manager
   * @param settings_manager settings manager
   * @param task_manager task manager to submit internal jobs to
   * @param workload_forecast_interval Interval used in the Forecaster
   * @param sequence_length Length of a planning sequence
   * @param horizon_length Length of the planning horizon
   */
  explicit Forecaster(std::string forecast_model_save_path,
                      common::ManagedPointer<metrics::MetricsThread> metrics_thread,
                      common::ManagedPointer<modelserver::ModelServerManager> model_server_manager,
                      common::ManagedPointer<settings::SettingsManager> settings_manager,
                      common::ManagedPointer<task::TaskManager> task_manager, uint64_t workload_forecast_interval,
                      uint64_t sequence_length, uint64_t horizon_length)
      : forecast_model_save_path_(std::move(forecast_model_save_path)),
        metrics_thread_(metrics_thread),
        model_server_manager_(model_server_manager),
        settings_manager_(settings_manager),
        task_manager_(task_manager),
        workload_forecast_interval_(workload_forecast_interval),
        sequence_length_(sequence_length),
        horizon_length_(horizon_length) {}

  /**
   * Loads workload forecast information
   * @param mode Mode to initialize forecast information
   */
  std::unique_ptr<selfdriving::WorkloadForecast> LoadWorkloadForecast(WorkloadForecastInitMode mode);

  /**
   * Performs training of the forecasting model
   */
  void PerformTraining();

 private:
  /**
   * Rerieve segment information
   * TODO(wz2): Addressing clustering based on this data will be left for the future.
   *
   * @param bounds (inclusive) bounds of the time range
   * @param success [out] indicator of whether query succeeded or not
   * @return segment information
   */
  std::unordered_map<int64_t, std::vector<double>> GetSegmentInformation(std::pair<uint64_t, uint64_t> bounds,
                                                                         bool *success);

  /**
   * Retrieve workload metadata
   * @param bounds (inclusive) bounds of the time range to pull data
   * @param out_metadata Query Metadata from metrics
   * @param out_params Query parameters fro mmetrics
   * @return pair where first is metadata and second is flag of success
   */
  std::pair<selfdriving::WorkloadMetadata, bool> RetrieveWorkloadMetadata(
      std::pair<uint64_t, uint64_t> bounds,
      const std::unordered_map<execution::query_id_t, metrics::QueryTraceMetadata::QueryMetadata> &out_metadata,
      const std::unordered_map<execution::query_id_t, std::vector<std::string>> &out_params);

  /**
   * Record the workload forecast to the internal tables
   * @param timestamp Timestamp to record forecast at
   * @param prediction Forecast model prediction
   * @param metadata Metadata about the queries
   */
  void RecordWorkloadForecastPrediction(uint64_t timestamp, const selfdriving::WorkloadForecastPrediction &prediction,
                                        const WorkloadMetadata &metadata);

  /**
   * Computes the valid range of data to be pulling from the internal tables.
   * @param now Current timestamp of the planning/training
   * @param train Whether data is for training or inference
   * @return inclusive start and end bounds of data to query
   */
  std::pair<uint64_t, uint64_t> ComputeTimestampDataRange(uint64_t now, bool train);

  std::string forecast_model_save_path_;
  common::ManagedPointer<metrics::MetricsThread> metrics_thread_;
  common::ManagedPointer<modelserver::ModelServerManager> model_server_manager_;
  common::ManagedPointer<settings::SettingsManager> settings_manager_;
  common::ManagedPointer<task::TaskManager> task_manager_;
  uint64_t workload_forecast_interval_;
  uint64_t sequence_length_;
  uint64_t horizon_length_;
};
}  // namespace noisepage::selfdriving
