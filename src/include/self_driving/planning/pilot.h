#pragma once

#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "common/action_context.h"
#include "common/macros.h"
#include "common/managed_pointer.h"
#include "execution/exec_defs.h"
#include "metrics/query_trace_metric.h"
#include "self_driving/forecasting/workload_forecast.h"
#include "self_driving/planning/action/action_defs.h"

namespace noisepage {
namespace messenger {
class Messenger;
}

namespace metrics {
class MetricsThread;
}

namespace modelserver {
class ModelServerManager;
}

namespace optimizer {
class StatsStorage;
}

namespace planner {
class AbstractPlanNode;
}

namespace settings {
class SettingsManager;
}

namespace transaction {
class TransactionManager;
}

namespace util {
class QueryExecUtil;
}  // namespace util

namespace task {
class TaskManager;
}

}  // namespace noisepage

namespace noisepage::selfdriving {
namespace pilot {
class AbstractAction;
class MonteCarloTreeSearch;
class TreeNode;
}  // namespace pilot

class PilotUtil;

/**
 * The pilot processes the query trace predictions by executing them and extracting pipeline features
 */
class Pilot {
 public:
  /** The default timeout for pilot futures. Inferences take milliseconds, but CI is flaky. */
  static constexpr std::chrono::seconds FUTURE_TIMEOUT{10};

  /**
   * Whether to use "what-if" API during the action search.
   * If true, the pilot only create the entries in the catalog for the indexes during the search. And the pilot uses
   * the stats to generate OU features.
   * If false, the pilot populate the candidate indexes during the search and execute queries to get OU features.
   */
  static constexpr bool WHAT_IF = true;

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
   * Constructor for Pilot
   * @param ou_model_save_path OU model save path
   * @param interference_model_save_path interference model save path
   * @param forecast_model_save_path forecast model save path
   * @param catalog catalog
   * @param metrics_thread metrics thread for metrics manager
   * @param model_server_manager model server manager
   * @param settings_manager settings manager
   * @param stats_storage stats_storage
   * @param txn_manager transaction manager
   * @param query_exec_util query execution utility for the pilot to use
   * @param task_manager task manager to submit internal jobs to
   * @param workload_forecast_interval Interval used in the forecastor
   * @param sequence_length Length of a planning sequence
   * @param horizon_length Length of the planning horizon
   */
  Pilot(std::string ou_model_save_path, std::string interference_model_save_path, std::string forecast_model_save_path,
        common::ManagedPointer<catalog::Catalog> catalog, common::ManagedPointer<metrics::MetricsThread> metrics_thread,
        common::ManagedPointer<modelserver::ModelServerManager> model_server_manager,
        common::ManagedPointer<settings::SettingsManager> settings_manager,
        common::ManagedPointer<optimizer::StatsStorage> stats_storage,
        common::ManagedPointer<transaction::TransactionManager> txn_manager,
        std::unique_ptr<util::QueryExecUtil> query_exec_util, common::ManagedPointer<task::TaskManager> task_manager,
        uint64_t workload_forecast_interval, uint64_t sequence_length, uint64_t horizon_length);

  /**
   * Get model save path
   * @return save path of the mini model
   */
  const std::string &GetOUModelSavePath() { return ou_model_save_path_; }

  /**
   * Get pointer to model server manager
   * @return pointer to model server manager
   */
  common::ManagedPointer<modelserver::ModelServerManager> GetModelServerManager() { return model_server_manager_; }

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
   * Loads workload forecast information
   * @param mode Mode to initialize forecast information
   */
  void LoadWorkloadForecast(WorkloadForecastInitMode mode);

  /**
   * Performs Pilot Logic, load and execute the predicted queries while extracting pipeline features
   */
  void PerformPlanning();

  /**
   * Performs training of the forecasting model
   */
  void PerformForecasterTrain();

  /**
   * Search for and apply the best action for the current timestamp
   * @param best_action_seq pointer to the vector to be filled with the sequence of best actions to take at current time
   */
  void ActionSearch(std::vector<std::pair<const std::string, catalog::db_oid_t>> *best_action_seq);

 private:
  /**
   * WorkloadForecast object performing the query execution and feature gathering
   */
  std::unique_ptr<selfdriving::WorkloadForecast> forecast_;

  /**
   * Empty Setter Callback for setting bool value for flags
   */
  static void EmptySetterCallback(common::ManagedPointer<common::ActionContext> action_context UNUSED_ATTRIBUTE) {}

  /**
   * Execute, collect pipeline metrics, and get ou prediction for each pipeline under different query parameters for
   * queries between start and end segment indices (both inclusive) in workload forecast.
   * @param start_segment_index start segment index in forecast to be considered
   * @param end_segment_index end segment index in forecast to be considered
   * @param query_info <query id, <num_param of this query executed, total number of collected ous for this query>>
   * @param segment_to_offset start index of ou records belonging to a segment in input to the interference model
   * @param interference_result_matrix stores the final results of the interference model
   */
  void ExecuteForecast(uint64_t start_segment_index, uint64_t end_segment_index,
                       std::map<execution::query_id_t, std::pair<uint8_t, uint64_t>> *query_info,
                       std::map<uint32_t, uint64_t> *segment_to_offset,
                       std::vector<std::vector<double>> *interference_result_matrix);

  /**
   * Computes the valid range of data to be pulling from the internal tables.
   * @param now Current timestamp of the planning/training
   * @param train Whether data is for training or inference
   * @return inclusive start and end bounds of data to query
   */
  std::pair<uint64_t, uint64_t> ComputeTimestampDataRange(uint64_t now, bool train);

  std::string ou_model_save_path_;
  std::string interference_model_save_path_;
  std::string forecast_model_save_path_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<metrics::MetricsThread> metrics_thread_;
  common::ManagedPointer<modelserver::ModelServerManager> model_server_manager_;
  common::ManagedPointer<settings::SettingsManager> settings_manager_;
  common::ManagedPointer<optimizer::StatsStorage> stats_storage_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  std::unique_ptr<util::QueryExecUtil> query_exec_util_;
  common::ManagedPointer<task::TaskManager> task_manager_;
  uint64_t workload_forecast_interval_{1000000};
  uint64_t sequence_length_{10};
  uint64_t horizon_length_{30};
  uint64_t action_planning_horizon_{5};
  uint64_t simulation_number_{20};
  friend class noisepage::selfdriving::PilotUtil;
  friend class noisepage::selfdriving::pilot::MonteCarloTreeSearch;
};

}  // namespace noisepage::selfdriving
