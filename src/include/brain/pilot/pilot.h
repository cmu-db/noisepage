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
  explicit Pilot(const common::ManagedPointer<DBMain> db_main, uint64_t forecast_interval);

  /**
   * WorkloadForecast object performing the query execution and feature gathering
   */
  std::unique_ptr<brain::WorkloadForecast> forecastor_;

  /**
   * Empty Setter Callback for setting bool value for flags
   */
  static void EmptySetterCallback(common::ManagedPointer<common::ActionContext> action_context UNUSED_ATTRIBUTE) {}

  /**
   * Performs Pilot Logic, load and execute the predict queries while extracting pipeline features
   */
  void PerformPlanning();

 private:
  void LoadQueryTrace();
  void LoadQueryText();
  void ExecuteForecast();
  
  common::ManagedPointer<DBMain> db_main_;

  std::map<uint64_t, std::pair<execution::query_id_t, uint64_t>> query_timestamp_to_id_;
  std::unordered_map<execution::query_id_t, std::vector<std::vector<parser::ConstantValueExpression>>>
      query_id_to_params_;
  std::unordered_map<execution::query_id_t, std::string> query_id_to_text_;
  std::unordered_map<std::string, execution::query_id_t> query_text_to_id_;
  // map id to a vector of execution times
  std::unordered_map<execution::query_id_t, std::vector<uint64_t>> num_executions_;
  uint64_t num_sample_{5};
  uint64_t forecast_interval_{10000000};
};

}  // namespace noisepage::brain
