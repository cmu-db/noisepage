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
 * The pilot
 */
class Pilot {
 public:
  /**
   * Constructor for the
   * @param
   * @param
   */
  explicit Pilot(const common::ManagedPointer<DBMain> db_main, uint64_t forecast_interval);

  /**
   *
   * @return
   */
  // void PerformPilotLogic();

  std::unique_ptr<brain::WorkloadForecast> forecastor_;
  // void EnablePlanning();
  // void DisablePlanning();
  void PerformPlanning();

 private:
  void LoadQueryTrace();
  void LoadQueryText();
  void ExecuteForecast();
  // static void EmptySetterCallback(common::ManagedPointer<common::ActionContext> action_context UNUSED_ATTRIBUTE) {}
  // static execution::exec::ExecutionSettings GetExecutionSettings();

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
