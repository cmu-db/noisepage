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
   * @param pipeline_qids vector of real qids (those from forecast) for pipelines in pipeline data; necessary since the
   * auto-incremental nature of qid in pipeline metrics
   * @param pipeline_data collected pipeline metrics after executing the forecasted queries
   * @param pipeline_to_prediction list of tuples of query id, pipeline id and result of prediction
   */
  static void InferenceWithFeatures(const std::string &model_save_path,
                                    common::ManagedPointer<modelserver::ModelServerManager> model_server_manager,
                                    const std::vector<execution::query_id_t> &pipeline_qids,
                                    const std::list<metrics::PipelineMetricRawData::PipelineData> &pipeline_data,
                                    std::map<std::pair<execution::query_id_t, execution::pipeline_id_t>,
                                             std::vector<std::vector<std::vector<double>>>> *pipeline_to_prediction);

  /**
   * Apply an action supplied through its query string to the database specified
   * @param pilot pointer to the pilot
   * @param sql_query query of the action to be executed
   * @param db_oid oid of the database where this action should be applied
   */
  static void ApplyAction(common::ManagedPointer<Pilot> pilot, const std::string &sql_query, catalog::db_oid_t db_oid);

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
   * Generate abstract plan from a query's parse result and parameters
   * @param txn current transaction
   * @param accessor pointer to catalog accessor
   * @param params pointer to parameters of the query, nullptr for actions
   * @param param_types pointer to parameters of the query, nullptr for actions
   * @param stmt_list statement list that's the parse result
   * @param db_oid database oid for the query
   * @param stats_storage stats storage
   * @param optimizer_timeout optimizer timeout
   * @return the abstract plan generated
   */
  static std::unique_ptr<planner::AbstractPlanNode> GenerateQueryPlan(
      transaction::TransactionContext *txn, common::ManagedPointer<catalog::CatalogAccessor> accessor,
      common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
      common::ManagedPointer<std::vector<type::TypeId>> param_types,
      common::ManagedPointer<parser::ParseResult> stmt_list, catalog::db_oid_t db_oid,
      common::ManagedPointer<optimizer::StatsStorage> stats_storage, uint64_t optimizer_timeout);

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
};

}  // namespace noisepage::selfdriving
