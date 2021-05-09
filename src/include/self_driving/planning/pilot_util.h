#pragma once

#include <list>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "metrics/metrics_store.h"
#include "parser/expression/constant_value_expression.h"

namespace noisepage {
namespace catalog {
class CatalogAccessor;
}

namespace modelserver {
class ModelServerManager;
}

namespace transaction {
class TransactionManager;
}

namespace optimizer {
class StatsStorage;
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
   * @param start_segment_index start index of segments of interest (inclusive)
   * @param end_segment_index end index of segments of interest (inclusive)
   * @param pipeline_qids vector of real pipeline qids to be populated (necessary to restore the qids to the original
   * forecasted qids due to the auto-incremental nature of the qids in pipeline metrics)
   * @param execute_query whether to execute the queries to get the correct features
   * @returns unique pointer to the collected pipeline data
   */
  static std::unique_ptr<metrics::PipelineMetricRawData> CollectPipelineFeatures(
      common::ManagedPointer<Pilot> pilot, common::ManagedPointer<WorkloadForecast> forecast,
      uint64_t start_segment_index, uint64_t end_segment_index, std::vector<execution::query_id_t> *pipeline_qids,
      bool execute_query);

  /**
   * Perform inference on OU models through model server manager with collected pipeline metrics
   * To recover the result for each pipeline, also maintain a multimap pipeline_to_ou_position
   * @param model_save_path model save path
   * @param model_server_manager model server manager
   * @param pipeline_qids vector of real qids (those from forecast) for pipelines in pipeline data; necessary since the
   * auto-incremental nature of qid in pipeline metrics
   * @param pipeline_data collected pipeline metrics after executing the forecasted queries
   * @param pipeline_to_prediction list of tuples of query id, pipeline id and result of prediction
   */
  static void OUModelInference(const std::string &model_save_path,
                               common::ManagedPointer<modelserver::ModelServerManager> model_server_manager,
                               const std::vector<execution::query_id_t> &pipeline_qids,
                               const std::list<metrics::PipelineMetricRawData::PipelineData> &pipeline_data,
                               std::map<std::pair<execution::query_id_t, execution::pipeline_id_t>,
                                        std::vector<std::vector<std::vector<double>>>> *pipeline_to_prediction);

  /**
   * Perform inference on the interference model through model server manager
   * @param interference_model_save_path Model save path
   * @param model_server_manager Model server manager
   * @param pipeline_to_prediction List of tuples of query id, pipeline id and result of prediction
   * @param forecast The predicted workload
   * @param start_segment_index The start segment in the workload forecast to do inference
   * @param end_segment_index The end segment in the workload forecast to do inference
   * @param query_info Query id, <num_param of this query executed, total number of collected ous for this query>
   * @param segment_to_offset The start index of ou records belonging to a segment in input to the interference model
   * @param interference_result_matrix Stores the inference results as return values
   */
  static void InterferenceModelInference(
      const std::string &interference_model_save_path,
      common::ManagedPointer<modelserver::ModelServerManager> model_server_manager,
      const std::map<std::pair<execution::query_id_t, execution::pipeline_id_t>,
                     std::vector<std::vector<std::vector<double>>>> &pipeline_to_prediction,
      common::ManagedPointer<selfdriving::WorkloadForecast> forecast, uint64_t start_segment_index,
      uint64_t end_segment_index, std::map<execution::query_id_t, std::pair<uint8_t, uint64_t>> *query_info,
      std::map<uint32_t, uint64_t> *segment_to_offset, std::vector<std::vector<double>> *interference_result_matrix);

  /**
   * Apply an action supplied through its query string to the database specified
   * @param pilot pointer to the pilot
   * @param sql_query query of the action to be executed
   * @param db_oid oid of the database where this action should be applied
   * @param what_if whether this is a "what-if" API call (e.g., only create the index entry in the catalog without
   * populating it)
   */
  static void ApplyAction(common::ManagedPointer<Pilot> pilot, const std::string &sql_query, catalog::db_oid_t db_oid,
                          bool what_if);

  /**
   * Retrieve all query plans associated with queries in the interval of forecasted segments
   * @param pilot pointer to the pilot
   * @param forecast pointer to the forecast segments
   * @param end_segment_index end index (inclusive)
   * @param txn the transaction context that would be used for action generation as well
   * @param plan_vecs the vector that would store the generated abstract plans of forecasted queries before the end
   * index
   */
  static void GetQueryPlans(common::ManagedPointer<Pilot> pilot, common::ManagedPointer<WorkloadForecast> forecast,
                            uint64_t end_segment_index, transaction::TransactionContext *txn,
                            std::vector<std::unique_ptr<planner::AbstractPlanNode>> *plan_vecs);

  /**
   * Compute cost of executed queries in the segments between start and end index (both inclusive)
   * @param pilot pointer to the pilot
   * @param forecast pointer to the forecast segments
   * @param start_segment_index start index (inclusive)
   * @param end_segment_index end index (inclusive)
   * @return total latency of queries calculated based on their num of exec
   */
  static double ComputeCost(common::ManagedPointer<Pilot> pilot, common::ManagedPointer<WorkloadForecast> forecast,
                            uint64_t start_segment_index, uint64_t end_segment_index);

 private:
  /**
   * Add features to existing features
   * @param feature The original feature to add in-place
   * @param delta_feature The amount to add
   * @param normalization Divide delta_feature by this value
   */
  static void SumFeatureInPlace(std::vector<double> *feature, const std::vector<double> &delta_feature,
                                double normalization);

  /**
   * Populate interference with first 9 dimension as feature vector normalized by the last dimension (ELAPSED_US);
   * next 9 dimension as sum of ou features in current segment normazlied by interval of segment;
   * last 9 dimension as all zeros.
   * @param feature
   * @param normalization
   * @return
   */
  static std::vector<double> GetInterferenceFeature(const std::vector<double> &feature,
                                                    const std::vector<double> &normalized_feat_sum);

  /**
   * Group pipeline features by ou for block inference
   * To recover the result for each pipeline, also maintain a multimap pipeline_to_ou_position
   * @param pipeline_to_ou_position list of tuples describing the ous associated with each pipeline, there could be
   * multiple entry with the same query id and pipeline id, since we consider pipeline under different query parameters
   * @param pipeline_qids vector of real pipeline qids, aka qids from workload forecast (this is to fix the
   * auto-incremental qids of pipeline metrics)
   * @param pipeline_data const reference to the collected pipeline metrics data
   * @param ou_to_features map from ExecutionOperatingUnitType to a vector of ou predictions (each prediction is a
   * double vector)
   */
  static void GroupFeaturesByOU(
      std::list<std::tuple<execution::query_id_t, execution::pipeline_id_t,
                           std::vector<std::pair<ExecutionOperatingUnitType, uint64_t>>>> *pipeline_to_ou_position,
      const std::vector<execution::query_id_t> &pipeline_qids,
      const std::list<metrics::PipelineMetricRawData::PipelineData> &pipeline_data,
      std::unordered_map<ExecutionOperatingUnitType, std::vector<std::vector<double>>> *ou_to_features);

  static const uint64_t INTERFERENCE_DIMENSION{27};
};

}  // namespace noisepage::selfdriving
