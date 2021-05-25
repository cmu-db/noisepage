#include "util/self_driving_recording_util.h"
#include "catalog/catalog_defs.h"
#include "execution/sql/value_util.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "task/task.h"
#include "task/task_manager.h"

namespace noisepage::util {

void SelfDrivingRecordingUtil::RecordQueryMetadata(
    const std::unordered_map<execution::query_id_t, metrics::QueryTraceMetadata::QueryMetadata> &qmetadata,
    common::ManagedPointer<task::TaskManager> task_manager) {
  std::vector<type::TypeId> param_types = {type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::VARCHAR,
                                           type::TypeId::VARCHAR};
  std::vector<std::vector<parser::ConstantValueExpression>> params_vec;
  for (auto &data : qmetadata) {
    std::vector<parser::ConstantValueExpression> params(4);
    params[0] = parser::ConstantValueExpression(
        type::TypeId::INTEGER, execution::sql::Integer(static_cast<int64_t>(data.second.db_oid_.UnderlyingValue())));
    params[1] =
        parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(data.first.UnderlyingValue()));
    {
      const auto string = std::string_view(data.second.text_);
      auto string_val = execution::sql::ValueUtil::CreateStringVal(string);
      params[2] =
          parser::ConstantValueExpression(type::TypeId::VARCHAR, string_val.first, std::move(string_val.second));
    }

    {
      const auto string = std::string_view(data.second.param_type_);
      auto string_val = execution::sql::ValueUtil::CreateStringVal(string);
      params[3] =
          parser::ConstantValueExpression(type::TypeId::VARCHAR, string_val.first, std::move(string_val.second));
    }
    params_vec.emplace_back(std::move(params));
  }

  if (!params_vec.empty()) {
    std::string query = SelfDrivingRecordingUtil::QUERY_TEXT_INSERT_STMT;
    task_manager->AddTask(std::make_unique<task::TaskDML>(catalog::INVALID_DATABASE_OID, query,
                                                          std::make_unique<optimizer::TrivialCostModel>(), false,
                                                          std::move(params_vec), std::move(param_types)));
  }
}

void SelfDrivingRecordingUtil::RecordQueryParameters(
    uint64_t timestamp_to_record,
    std::unordered_map<execution::query_id_t, common::ReservoirSampling<std::string>> *params,
    common::ManagedPointer<task::TaskManager> task_manager,
    std::unordered_map<execution::query_id_t, std::vector<std::string>> *out_params) {
  std::vector<type::TypeId> param_types = {type::TypeId::BIGINT, type::TypeId::INTEGER, type::TypeId::VARCHAR};
  std::vector<std::vector<parser::ConstantValueExpression>> params_vec;
  for (auto &data : (*params)) {
    std::vector<std::string> samples = data.second.TakeSamples();
    for (auto &sample : samples) {
      std::vector<parser::ConstantValueExpression> param_vec(3);
      param_vec[0] =
          parser::ConstantValueExpression(type::TypeId::BIGINT, execution::sql::Integer(timestamp_to_record));
      param_vec[1] =
          parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(data.first.UnderlyingValue()));

      const auto string = std::string_view(sample);
      auto string_val = execution::sql::ValueUtil::CreateStringVal(string);
      param_vec[2] =
          parser::ConstantValueExpression(type::TypeId::VARCHAR, string_val.first, std::move(string_val.second));
      params_vec.emplace_back(std::move(param_vec));
    }

    if (out_params != nullptr) {
      (*out_params)[data.first] = std::move(samples);
    }
  }

  if (!params_vec.empty()) {
    std::string query_text = SelfDrivingRecordingUtil::QUERY_PARAMETERS_INSERT_STMT;
    task_manager->AddTask(std::make_unique<task::TaskDML>(catalog::INVALID_DATABASE_OID, query_text,
                                                          std::make_unique<optimizer::TrivialCostModel>(), false,
                                                          std::move(params_vec), std::move(param_types)));
  }
}

void SelfDrivingRecordingUtil::RecordForecastClusters(uint64_t timestamp_to_record,
                                                      const selfdriving::WorkloadMetadata &metadata,
                                                      const selfdriving::WorkloadForecastPrediction &prediction,
                                                      common::ManagedPointer<task::TaskManager> task_manager) {
  // This is a bit more memory intensive, since we have to copy all the parameters
  std::vector<std::vector<parser::ConstantValueExpression>> clusters_params_vec;
  for (auto &cluster : prediction) {
    for (auto &qid_info : cluster.second) {
      execution::query_id_t qid{static_cast<uint32_t>(qid_info.first)};

      // This assert is correct because we loaded the entire query history from the internal tables.
      NOISEPAGE_ASSERT(metadata.query_id_to_dboid_.find(qid) != metadata.query_id_to_dboid_.end(),
                       "Expected QID info to exist");
      std::vector<parser::ConstantValueExpression> clusters_params(4);

      // Timestamp of forecast
      clusters_params[0] =
          parser::ConstantValueExpression(type::TypeId::BIGINT, execution::sql::Integer(timestamp_to_record));

      // Cluster ID
      clusters_params[1] =
          parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(cluster.first));

      // Query ID
      clusters_params[2] =
          parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(qid_info.first));

      // DB OID
      clusters_params[3] = parser::ConstantValueExpression(
          type::TypeId::INTEGER,
          execution::sql::Integer(metadata.query_id_to_dboid_.find(qid)->second.UnderlyingValue()));
      clusters_params_vec.emplace_back(std::move(clusters_params));
    }
  }

  if (!clusters_params_vec.empty()) {
    // Clusters
    std::vector<type::TypeId> param_types = {type::TypeId::BIGINT, type::TypeId::INTEGER, type::TypeId::INTEGER,
                                             type::TypeId::INTEGER};
    std::string query_text = SelfDrivingRecordingUtil::FORECAST_CLUSTERS_INSERT_STMT;
    task_manager->AddTask(std::make_unique<task::TaskDML>(catalog::INVALID_DATABASE_OID, query_text,
                                                          std::make_unique<optimizer::TrivialCostModel>(), false,
                                                          std::move(clusters_params_vec), std::move(param_types)));
  }
}

void SelfDrivingRecordingUtil::RecordForecastQueryFrequencies(uint64_t timestamp_to_record,
                                                              const selfdriving::WorkloadMetadata &metadata,
                                                              const selfdriving::WorkloadForecastPrediction &prediction,
                                                              common::ManagedPointer<task::TaskManager> task_manager) {
  std::vector<std::vector<parser::ConstantValueExpression>> forecast_params_vec;
  for (auto &cluster : prediction) {
    for (auto &qid_info : cluster.second) {
      execution::query_id_t qid{static_cast<uint32_t>(qid_info.first)};

      // This assert is correct because we loaded the entire query history from the internal tables.
      NOISEPAGE_ASSERT(metadata.query_id_to_dboid_.find(qid) != metadata.query_id_to_dboid_.end(),
                       "Expected QID info to exist");
      for (size_t interval = 0; interval < qid_info.second.size(); interval++) {
        std::vector<parser::ConstantValueExpression> forecasts_params(4);

        // Timestamp of the forecast
        forecasts_params[0] =
            parser::ConstantValueExpression(type::TypeId::BIGINT, execution::sql::Integer(timestamp_to_record));

        // Query ID
        forecasts_params[1] =
            parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(qid_info.first));

        // Segment number within forecast interval
        forecasts_params[2] = parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(interval));

        // Estimated number of queries
        forecasts_params[3] =
            parser::ConstantValueExpression(type::TypeId::REAL, execution::sql::Real(qid_info.second[interval]));

        forecast_params_vec.emplace_back(std::move(forecasts_params));
      }
    }
  }

  if (!forecast_params_vec.empty()) {
    // Forecasts
    std::vector<type::TypeId> param_types = {type::TypeId::BIGINT, type::TypeId::INTEGER, type::TypeId::INTEGER,
                                             type::TypeId::REAL};
    std::string query_text = SelfDrivingRecordingUtil::FORECAST_FORECASTS_INSERT_STMT;
    task_manager->AddTask(std::make_unique<task::TaskDML>(catalog::INVALID_DATABASE_OID, query_text,
                                                          std::make_unique<optimizer::TrivialCostModel>(), false,
                                                          std::move(forecast_params_vec), std::move(param_types)));
  }
}

void SelfDrivingRecordingUtil::RecordAppliedAction(uint64_t timestamp_to_record,
                                                   selfdriving::pilot::action_id_t action_id, double cost,
                                                   catalog::db_oid_t db_id, const std::string &action_text,
                                                   common::ManagedPointer<task::TaskManager> task_manager) {
  std::vector<parser::ConstantValueExpression> param(5);
  param[0] = parser::ConstantValueExpression(type::TypeId::BIGINT, execution::sql::Integer(timestamp_to_record));
  param[1] =
      parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(action_id.UnderlyingValue()));
  param[2] = parser::ConstantValueExpression(type::TypeId::REAL, execution::sql::Real(cost));
  param[3] = parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(db_id.UnderlyingValue()));

  {
    const auto string = std::string_view(action_text);
    auto string_val = execution::sql::ValueUtil::CreateStringVal(string);
    param[4] = parser::ConstantValueExpression(type::TypeId::VARCHAR, string_val.first, std::move(string_val.second));
  }
  std::vector<std::vector<parser::ConstantValueExpression>> params_vec;
  params_vec.emplace_back(std::move(param));

  std::vector<type::TypeId> param_types = {type::TypeId::BIGINT, type::TypeId::INTEGER, type::TypeId::REAL,
                                           type::TypeId::INTEGER, type::TypeId::VARCHAR};
  std::string query_text = SelfDrivingRecordingUtil::APPLIED_ACTIONS_INSERT_STMT;
  task_manager->AddTask(std::make_unique<task::TaskDML>(catalog::INVALID_DATABASE_OID, query_text,
                                                        std::make_unique<optimizer::TrivialCostModel>(), false,
                                                        std::move(params_vec), std::move(param_types)));
}

void SelfDrivingRecordingUtil::RecordBestActions(
    uint64_t timestamp_to_record, const std::vector<std::vector<selfdriving::pilot::ActionTreeNode>> &actions,
    common::ManagedPointer<task::TaskManager> task_manager) {
  std::vector<std::vector<parser::ConstantValueExpression>> params_vec;
  for (size_t i = 0; i < actions.size(); i++) {
    for (const selfdriving::pilot::ActionTreeNode &node : actions[i]) {
      std::vector<parser::ConstantValueExpression> param(10);
      param[0] = parser::ConstantValueExpression(type::TypeId::BIGINT, execution::sql::Integer(timestamp_to_record));
      param[1] = parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(i));
      param[2] = parser::ConstantValueExpression(type::TypeId::INTEGER,
                                                 execution::sql::Integer(node.GetTreeNodeId().UnderlyingValue()));
      param[3] = parser::ConstantValueExpression(type::TypeId::INTEGER,
                                                 execution::sql::Integer(node.GetParentNodeId().UnderlyingValue()));
      param[4] = parser::ConstantValueExpression(type::TypeId::INTEGER,
                                                 execution::sql::Integer(node.GetActionStartSegmentIndex()));
      param[5] =
          parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(node.GetActionPlanEndIndex()));
      param[6] = parser::ConstantValueExpression(type::TypeId::INTEGER,
                                                 execution::sql::Integer(node.GetActionId().UnderlyingValue()));
      param[7] = parser::ConstantValueExpression(type::TypeId::REAL, execution::sql::Real(node.GetCost()));
      param[8] = parser::ConstantValueExpression(type::TypeId::INTEGER,
                                                 execution::sql::Integer(node.GetDbOid().UnderlyingValue()));

      const auto string = std::string_view(node.GetActionText());
      auto string_val = execution::sql::ValueUtil::CreateStringVal(string);
      param[9] = parser::ConstantValueExpression(type::TypeId::VARCHAR, string_val.first, std::move(string_val.second));

      params_vec.emplace_back(std::move(param));
    }
  }

  std::vector<type::TypeId> param_types = {type::TypeId::BIGINT,  type::TypeId::INTEGER, type::TypeId::INTEGER,
                                           type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER,
                                           type::TypeId::INTEGER, type::TypeId::REAL,    type::TypeId::INTEGER,
                                           type::TypeId::VARCHAR};
  std::string query_text = SelfDrivingRecordingUtil::BEST_ACTIONS_INSERT_STMT;
  task_manager->AddTask(std::make_unique<task::TaskDML>(catalog::INVALID_DATABASE_OID, query_text,
                                                        std::make_unique<optimizer::TrivialCostModel>(), false,
                                                        std::move(params_vec), std::move(param_types)));
}

}  // namespace noisepage::util
