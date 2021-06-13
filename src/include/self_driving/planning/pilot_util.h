#pragma once

#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "metrics/metrics_store.h"
#include "parser/expression/constant_value_expression.h"
#include "self_driving/planning/action/action_defs.h"

namespace noisepage {
namespace catalog {
class CatalogAccessor;
class Catalog;
}  // namespace catalog

namespace modelserver {
class ModelServerManager;
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

namespace pilot {
class Pilot;
class CreateIndexAction;
class DropIndexAction;
struct MemoryInfo;
class ActionState;
class AbstractAction;
class PlanningContext;

/**
 * Utility class for helper functions
 */
class PilotUtil {
 public:
  /**
   * Executing forecasted queries and collect pipeline features for cost estimation to be used in action selection
   * @param planning_context pilot planning context
   * @param forecast pointer to object storing result of workload forecast
   * @param start_segment_index start index of segments of interest (inclusive)
   * @param end_segment_index end index of segments of interest (inclusive)
   * @param pipeline_qids vector of real pipeline qids to be populated (necessary to restore the qids to the original
   * forecasted qids due to the auto-incremental nature of the qids in pipeline metrics)
   * @param execute_query whether to execute the queries to get the correct features
   * @returns unique pointer to the collected pipeline data
   */
  static std::unique_ptr<metrics::PipelineMetricRawData> CollectPipelineFeatures(
      const pilot::PlanningContext &planning_context, common::ManagedPointer<WorkloadForecast> forecast,
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
   * @param planning_context pilot planning context
   * @param sql_query query of the action to be executed
   * @param db_oid oid of the database where this action should be applied
   * @param what_if whether this is a "what-if" API call (e.g., only create the index entry in the catalog without
   * populating it)
   */
  static void ApplyAction(const pilot::PlanningContext &planning_context, const std::string &sql_query,
                          catalog::db_oid_t db_oid, bool what_if);

  /**
   * Retrieve all query plans associated with queries in the interval of forecasted segments
   * @param planning_context pilot planning context
   * @param forecast pointer to the forecast segments
   * @param end_segment_index end index (inclusive)
   * @param plan_vecs the vector that would store the generated abstract plans of forecasted queries before the end
   * index
   */
  static void GetQueryPlans(const pilot::PlanningContext &planning_context,
                            common::ManagedPointer<WorkloadForecast> forecast, uint64_t end_segment_index,
                            std::vector<std::unique_ptr<planner::AbstractPlanNode>> *plan_vecs);

  /**
   * Compute cost of executed queries in the segments between start and end index (both inclusive)
   * @param planning_context pilot planning context
   * @param forecast pointer to the forecast segments
   * @param start_segment_index start index (inclusive)
   * @param end_segment_index end index (inclusive)
   * @return total latency of queries calculated based on their num of exec
   */
  static double ComputeCost(const pilot::PlanningContext &planning_context,
                            common::ManagedPointer<WorkloadForecast> forecast, uint64_t start_segment_index,
                            uint64_t end_segment_index);

  /**
   * Predict the runtime metrics of a create index action
   * @param planning_context pilot planning context
   * @param create_action Pointer to the CreateIndexAction
   * @param drop_action Pointer to the DropIndexAction (reverse action)
   */
  static void EstimateCreateIndexAction(const pilot::PlanningContext &planning_context,
                                        pilot::CreateIndexAction *create_action, pilot::DropIndexAction *drop_action);

  /**
   * Calculate the memory consumption given a specific forecasted workload segment with a specific action state
   * @param memory_info Pre-calculated memory information
   * @param action_state The state of the actions
   * @param segment_index Which forecasted interval to compute memory consumption for
   * @param action_map Reference of the map from action id to action pointers
   * @return The memory consumption estimation
   */
  static size_t CalculateMemoryConsumption(
      const pilot::MemoryInfo &memory_info, const pilot::ActionState &action_state, uint64_t segment_index,
      const std::map<pilot::action_id_t, std::unique_ptr<pilot::AbstractAction>> &action_map);

  /**
   * Construct the MemoryInfo object with information to ensure the memory constraint
   * @param planning_context pilot planning context
   * @param forecast pointer to the forecast segments
   * @return MemoryInfo object
   */
  static pilot::MemoryInfo ComputeMemoryInfo(const pilot::PlanningContext &planning_context,
                                             const WorkloadForecast *forecast);

  /**
   * Utility function for printing a configuration (set of structures)
   * @param config_set configuration
   * @return string representation
   */
  static std::string ConfigToString(const std::set<pilot::action_id_t> &config_set);

 private:
  /**
   * Get the ratios between estimated future table sizes (given the forecasted workload) and current table sizes
   * @param planning_context pilot planning context
   * @param forecast Workload forecast information
   * @param task_manager Task manager pointer
   * @param memory_info Object that stores the returned table size info
   */
  static void ComputeTableSizeRatios(const pilot::PlanningContext &planning_context, const WorkloadForecast *forecast,
                                     pilot::MemoryInfo *memory_info);

  /**
   * Get the current table and index heap memory usage
   * TODO(lin): we should get this information from the stats if the pilot is not running on the primary. But since
   *   we don't have this in stats yet we're directly getting the information from c++ objects.
   * @param planning_context pilot planning context
   * @param memory_info Object that stores the returned memory info
   */
  static void ComputeTableIndexSizes(const pilot::PlanningContext &planning_context, pilot::MemoryInfo *memory_info);

  /**
   * Execute, collect pipeline metrics, and get ou prediction for each pipeline under different query parameters for
   * queries between start and end segment indices (both inclusive) in workload forecast.
   * @param planning_context pilot planning context
   * @param forecast workload forecast information
   * @param start_segment_index start segment index in forecast to be considered
   * @param end_segment_index end segment index in forecast to be considered
   * @param query_info <query id, <num_param of this query executed, total number of collected ous for this query>>
   * @param segment_to_offset start index of ou records belonging to a segment in input to the interference model
   * @param interference_result_matrix stores the final results of the interference model
   */
  static void ExecuteForecast(const pilot::PlanningContext &planning_context,
                              common::ManagedPointer<selfdriving::WorkloadForecast> forecast,
                              uint64_t start_segment_index, uint64_t end_segment_index,
                              std::map<execution::query_id_t, std::pair<uint8_t, uint64_t>> *query_info,
                              std::map<uint32_t, uint64_t> *segment_to_offset,
                              std::vector<std::vector<double>> *interference_result_matrix);

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

}  // namespace pilot
}  // namespace noisepage::selfdriving
