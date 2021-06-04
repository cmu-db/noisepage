#pragma once

#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
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
#include "self_driving/forecasting/forecaster.h"
#include "self_driving/forecasting/workload_forecast.h"
#include "self_driving/planning/action/action_defs.h"
#include "self_driving/planning/memory_info.h"
#include "self_driving/planning/planning_context.h"

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

namespace noisepage::selfdriving::pilot {
class AbstractAction;
class TreeNode;
class ActionTreeNode;
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
   * @param workload_forecast_interval Interval used in the Forecaster
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
   * Performs Pilot Logic, load and execute the predicted queries while extracting pipeline features
   * NOTE: Currently we assume that the planning performs on a snapshot of a database (i.e., a "special replica") that
   * only does the planning and does not execute client queries. That assumption may be relaxed since we create
   * transaction contexts for all databases at the beginning of the planning and abort them when planning finishes.
   * But we still need to isolate the knob (setting) changes if we want to relax the assumption.
   */
  void PerformPlanning();

  /**
   * Search for and apply the best action for the current timestamp
   * @param best_action_seq pointer to the vector to be filled with the sequence of best actions to take at current time
   */
  void ActionSearch(std::vector<ActionTreeNode> *best_action_seq);

  /**
   * Search for and apply the best set of actions for the current timestamp using the Sequence Tuning baseline
   * @param best_actions_seq pointer to the vector to be filled with the sequence of set of best actions to take at
   * current time
   */
  void ActionSearchBaseline(std::vector<std::set<std::pair<const std::string, catalog::db_oid_t>>> *best_actions_seq);

  /**
   * Performs training of the forecasting model
   */
  void PerformForecasterTrain() {
    std::unique_lock<std::mutex> lock(forecaster_train_mutex_);
    forecaster_.PerformTraining();
  }

 private:
  /**
   * WorkloadForecast object performing the query execution and feature gathering
   */
  std::unique_ptr<selfdriving::WorkloadForecast> forecast_;

  /**
   * Empty Setter Callback for setting bool value for flags
   */
  static void EmptySetterCallback(common::ManagedPointer<common::ActionContext> action_context UNUSED_ATTRIBUTE) {}

  PlanningContext planning_context_;

  Forecaster forecaster_;
  uint64_t action_planning_horizon_{15};
  uint64_t simulation_number_{20};

  std::mutex forecaster_train_mutex_;
};

}  // namespace noisepage::selfdriving::pilot
