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

namespace planner {
class AbstractPlanNode;
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
      common::ManagedPointer<Pilot> pilot, common::ManagedPointer<WorkloadForecast> forecast,
      uint64_t start_segment_index, uint64_t end_segment_index, std::vector<execution::query_id_t> *pipeline_qids);

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
      const std::vector<execution::query_id_t> &pipeline_qids,
      const std::list<metrics::PipelineMetricRawData::PipelineData> &pipeline_data,
      std::map<std::pair<execution::query_id_t, execution::pipeline_id_t>,
               std::vector<std::vector<std::vector<double>>>> *pipeline_to_prediction);

  /**
   * Apply an action supplied through its query string to databases specified
   * @param pointer to the pilot
   * @param db_oids db_oids relevant to current action
   * @param sql_query query of the action to be executed
   */
  static void ApplyAction(common::ManagedPointer<Pilot> pilot, const std::vector<uint64_t> &db_oids,
                          const std::string &sql_query);

  /**
   * Retrieve all query plans associated with queries in the interval of forecasted segments
   * @param pilot pointer to the pilot
   * @param forecast pointer to the forecast segments
   * @param start_segment_index start index (inclusive)
   * @param end_segment_index end index (inclusive)
   * @return vector of query plans
   */
  static std::vector<std::unique_ptr<planner::AbstractPlanNode>> GetQueryPlans(common::ManagedPointer<Pilot> pilot,
                                                                               common::ManagedPointer<WorkloadForecast> forecast,
                                                                               uint64_t start_segment_index,
                                                                               uint64_t end_segment_index);

  /**
   * Compute cost of executed queries in the segments between start and end index (both inclusive)
   * @param pilot pointer to the pilot
   * @param forecast pointer to the forecast segments
   * @param start_segment_index start index (inclusive)
   * @param end_segment_index end index (inclusive)
   * @return average latency of queries weighted by their num of exec
   */
  static uint64_t ComputeCost(common::ManagedPointer<Pilot> pilot, common::ManagedPointer<WorkloadForecast> forecast,
                              uint64_t start_segment_index, uint64_t end_segment_index);

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
      const std::vector<execution::query_id_t> &pipeline_qids,
      const std::list<metrics::PipelineMetricRawData::PipelineData> &pipeline_data,
      std::unordered_map<ExecutionOperatingUnitType, std::vector<std::vector<double>>> *ou_to_features);
};

}  // namespace noisepage::selfdriving
