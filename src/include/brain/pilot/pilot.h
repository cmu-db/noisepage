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

#include "brain/forecast/workload_forecast.h"
#include "brain/operating_unit.h"
#include "common/action_context.h"
#include "common/error/exception.h"
#include "common/shared_latch.h"
#include "execution/exec/execution_settings.h"
#include "execution/exec_defs.h"
#include "gflags/gflags.h"
#include "loggers/settings_logger.h"
#include "parser/expression/constant_value_expression.h"
#include "settings/settings_param.h"
#include "type/type_id.h"

namespace noisepage::brain {

/**
 * The pilot processes the query trace predictions by executing them and extracting pipeline features
 */
class Pilot {
 public:
  /**
   * Constructor for Pilot
   * @param db_main Managed Pointer to db_main
   * @param forecast_interval Interval used in the forecastor
   */
  Pilot(common::ManagedPointer<DBMain> db_main, uint64_t forecast_interval);

  /**
   * WorkloadForecast object performing the query execution and feature gathering
   */
  std::unique_ptr<brain::WorkloadForecast> forecast_;

  /**
   * Empty Setter Callback for setting bool value for flags
   */
  static void EmptySetterCallback(common::ManagedPointer<common::ActionContext> action_context UNUSED_ATTRIBUTE) {}

  /**
   * Performs Pilot Logic, load and execute the predict queries while extracting pipeline features
   */
  void PerformPlanning();

 private:
  void ExecuteForecast();
  common::ManagedPointer<DBMain> db_main_;

  uint64_t forecast_interval_{10000000};
};

}  // namespace noisepage::brain
