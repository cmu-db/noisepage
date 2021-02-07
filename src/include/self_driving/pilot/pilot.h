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
#include "self_driving/forecast/workload_forecast.h"
#include "self_driving/pilot/action/action_defs.h"

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
 protected:
  /** @return Name of the environment variable to be set as the absolute path of build directory */
  static constexpr const char *BUILD_ABS_PATH = "BUILD_ABS_PATH";

 public:
  /**
   * Constructor for Pilot
   * @param model_save_path model save path
   * @param catalog catalog
   * @param metrics_thread metrics thread for metrics manager
   * @param model_server_manager model server manager
   * @param settings_manager settings manager
   * @param stats_storage stats_storage
   * @param txn_manager transaction manager
   * @param workload_forecast_interval Interval used in the forecastor
   */
  Pilot(std::string model_save_path, common::ManagedPointer<catalog::Catalog> catalog,
        common::ManagedPointer<metrics::MetricsThread> metrics_thread,
        common::ManagedPointer<modelserver::ModelServerManager> model_server_manager,
        common::ManagedPointer<settings::SettingsManager> settings_manager,
        common::ManagedPointer<optimizer::StatsStorage> stats_storage,
        common::ManagedPointer<transaction::TransactionManager> txn_manager, uint64_t workload_forecast_interval);

  /**
   * Get model save path
   * @return save path of the mini model
   */
  const std::string &GetModelSavePath() { return model_save_path_; }

  /**
   * Get pointer to model server manager
   * @return pointer to model server manager
   */
  common::ManagedPointer<modelserver::ModelServerManager> GetModelServerManager() { return model_server_manager_; }

  /**
   * Performs Pilot Logic, load and execute the predicted queries while extracting pipeline features
   */
  void PerformPlanning();

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
   * @param pipeline_to_prediction to be populated, map from a pipeline in forecasted queries to the list of ou
   * prediction for different parameters, each ou prediction is a 2D double array
   * @param start_segment_index start segment index in forecast to be considered
   * @param end_segment_index end segment index in forecast to be considered
   */
  void ExecuteForecast(std::map<std::pair<execution::query_id_t, execution::pipeline_id_t>,
                                std::vector<std::vector<std::vector<double>>>> *pipeline_to_prediction,
                       uint64_t start_segment_index, uint64_t end_segment_index);

  std::string model_save_path_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<metrics::MetricsThread> metrics_thread_;
  common::ManagedPointer<modelserver::ModelServerManager> model_server_manager_;
  common::ManagedPointer<settings::SettingsManager> settings_manager_;
  common::ManagedPointer<optimizer::StatsStorage> stats_storage_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  uint64_t workload_forecast_interval_{10000000};
  uint64_t action_planning_horizon_{5};
  uint64_t simulation_number_{20};
  friend class noisepage::selfdriving::PilotUtil;
  friend class noisepage::selfdriving::pilot::MonteCarloTreeSearch;
};

}  // namespace noisepage::selfdriving
