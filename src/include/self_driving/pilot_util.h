#pragma once

#include <list>
#include <map>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "metrics/metrics_store.h"
#include "parser/expression/constant_value_expression.h"

namespace noisepage {
namespace modelserver {
class ModelServerManager;
}

namespace transaction {
class TransactionManager;
}

}  // namespace noisepage

namespace noisepage::selfdriving {
class WorkloadForecast;
class Pilot;

/**
 * Utility class for helper functions
 */
class PilotUtil {
 public:
  /**
   * Executing forecasted queries and collect pipeline features for cost estimation to be used in action selection
   * @param pilot pointer to the pilot to access settings, metrics, and transaction managers, and catalog
   * @param forecast pointer to object storing result of workload forecast
   * @returns const pointer to the collected pipeline data
   */
  static const std::list<metrics::PipelineMetricRawData::PipelineData> &CollectPipelineFeatures(
      common::ManagedPointer<selfdriving::Pilot> pilot, common::ManagedPointer<selfdriving::WorkloadForecast> forecast);

  /**
   * Perform inference through model server manager with collected pipeline metrics
   * To recover the result for each pipeline, also maintain a multimap pipeline_to_ou_position
   * @param model_save_path model save path
   * @param model_server_manager model server manager
   * @param pipeline_data collected pipeline metrics after executing the forecasted queries
   * @param pipeline_to_prediction list of tuples of query id, pipeline id and result of prediction
   */
  static void InferenceWithFeatures(
      const std::string &model_save_path, common::ManagedPointer<modelserver::ModelServerManager> model_server_manager,
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
                           std::vector<std::pair<ExecutionOperatingUnitType, uint64_t>>>> *pipeline_to_ou_position,
      const std::list<metrics::PipelineMetricRawData::PipelineData> &pipeline_data,
      std::unordered_map<ExecutionOperatingUnitType, std::vector<std::vector<double>>> *ou_to_features);
};

}  // namespace noisepage::selfdriving
