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
   * @param pilot pointer to the pilot to access settings, metrics, and transaction managers, and catalog 
   * @param forecast pointer to object storing result of workload forecast
   */
  static const std::list<metrics::PipelineMetricRawData::PipelineData> &CollectPipelineFeatures(
      common::ManagedPointer<selfdriving::Pilot> pilot, common::ManagedPointer<selfdriving::WorkloadForecast> forecast);

  /**
   * Perform inference through model server manager with collected pipeline metrics
   * To recover the result for each pipeline, also maintain a multimap pipeline_to_ou_position
   * @param ms_manager model server manager
   * @param pipeline_data collected pipeline metrics after executing the forecasted queries
   * @param pipeline_to_prediction list of tuples of query id, pipeline id and result of prediction
   */
  static void InferenceWithFeatures(
      common::ManagedPointer<modelserver::ModelServerManager> ms_manager,
      const std::list<metrics::PipelineMetricRawData::PipelineData> &pipeline_data,
      std::list<std::tuple<execution::query_id_t, execution::pipeline_id_t, std::vector<std::vector<double>>>>
          *pipeline_to_prediction);

 private:
  /**
   * Group pipeline features by ou for block inference
   * To recover the result for each pipeline, also maintain a multimap pipeline_to_ou_position
   * @param pipeline_to_ou_position list of tuples describing the pipelines associated with each ou sample
   * @param pipeline_data const reference of the collected pipeline data
   * @param ou_to_features map from ExecutionOperatingUnitType to a matrix
   */
  static void GroupFeaturesByOU(
      std::list<std::tuple<execution::query_id_t, execution::pipeline_id_t,
                           std::vector<std::tuple<ExecutionOperatingUnitType, uint64_t>>>> *pipeline_to_ou_position,
      const std::list<metrics::PipelineMetricRawData::PipelineData> &pipeline_data,
      std::unordered_map<ExecutionOperatingUnitType, std::vector<std::vector<double>>> *ou_to_features);
};

}  // namespace noisepage::selfdriving
