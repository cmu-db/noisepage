#pragma once

#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "common/action_context.h"
#include "common/macros.h"
#include "common/managed_pointer.h"
#include "execution/exec_defs.h"
#include "self_driving/forecast/workload_forecast.h"

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

namespace settings {
class SettingsManager;
}

namespace transaction {
class TransactionManager;
}

}  // namespace noisepage

namespace noisepage::selfdriving {
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
   * Performs Pilot Logic, load and execute the predict queries while extracting pipeline features
   */
  void PerformPlanning();

 private:
  /**
   * WorkloadForecast object performing the query execution and feature gathering
   */
  std::unique_ptr<selfdriving::WorkloadForecast> forecast_;

  /**
   * Empty Setter Callback for setting bool value for flags
   */
  static void EmptySetterCallback(common::ManagedPointer<common::ActionContext> action_context UNUSED_ATTRIBUTE) {}

  void ExecuteForecast();

  std::string model_save_path_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<metrics::MetricsThread> metrics_thread_;
  common::ManagedPointer<modelserver::ModelServerManager> model_server_manager_;
  common::ManagedPointer<settings::SettingsManager> settings_manager_;
  common::ManagedPointer<optimizer::StatsStorage> stats_storage_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  uint64_t workload_forecast_interval_{10000000};
  friend class noisepage::selfdriving::PilotUtil;
};

}  // namespace noisepage::selfdriving
