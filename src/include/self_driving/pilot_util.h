#pragma once

#include <list>
#include <map>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "binder/bind_node_visitor.h"
#include "common/action_context.h"
#include "common/error/exception.h"
#include "common/macros.h"
#include "common/managed_pointer.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/executable_query.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/execution_settings.h"
#include "execution/exec_defs.h"
#include "main/db_main.h"
#include "metrics/metrics_store.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/optimizer.h"
#include "parser/expression/constant_value_expression.h"
#include "self_driving/forecast/workload_forecast.h"
#include "traffic_cop/traffic_cop_util.h"
#include "transaction/transaction_manager.h"

namespace noisepage::selfdriving {

/**
 * Utility class for helper functions
 */
class PilotUtil {
 public:
  /**
   * Executing forecasted queries and collect pipeline features for cost estimation to be used in action selection
   * @param db_main pointer to DBMain to access settings & metrics managers, transaction layer, and catalog layer
   * @param forecast pointer to object storing result of workload forecast
   */
  static void CollectPipelineFeatures(common::ManagedPointer<DBMain> db_main,
                                      common::ManagedPointer<selfdriving::WorkloadForecast> forecast);

  /**
   * Executing forecasted queries and collect pipeline features for cost estimation to be used in action selection
   * @param db_main pointer to DBMain to access settings & metrics managers, transaction layer, and catalog layer
   * @param forecast pointer to object storing result of workload forecast
   */
  static void GroupFeaturesByOU(
      std::multimap<std::tuple<execution::query_id_t, execution::pipeline_id_t>, uint64_t> *pipeline_to_ou_position,
      const std::list<metrics::PipelineMetricRawData::PipelineData> &pipeline_data,
      std::unordered_map<ExecutionOperatingUnitType, std::vector<std::vector<double>>> *ou_to_features);
};

}  // namespace noisepage::selfdriving
