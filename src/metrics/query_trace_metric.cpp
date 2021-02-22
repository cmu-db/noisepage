#include "metrics/query_trace_metric.h"

namespace noisepage::metrics {

uint64_t QueryTraceMetricRawData::QUERY_PARAM_SAMPLE = 5;

void QueryTraceMetadata::RecordQueryParamSample(execution::query_id_t qid, std::string query_param) {
  if (qid_param_samples_.find(qid) == qid_param_samples_.end()) {
    qid_param_samples_.emplace(qid,
                               common::ReservoirSampling<std::string>(QueryTraceMetricRawData::QUERY_PARAM_SAMPLE));
  }

  qid_param_samples_.find(qid)->second.AddSample(query_param);
}

void QueryTraceMetricRawData::ResetAggregation(common::ManagedPointer<util::QueryExecUtil> query_exec_util) {
  ToDB(query_exec_util);
  low_timestamp_ = UINT64_MAX;
  high_timestamp_ = 0;
}

void QueryTraceMetricRawData::ToDB(common::ManagedPointer<util::QueryExecUtil> query_exec_util) {
  /*
  // CREATE TABLE noisepage_forecast_frequencies(iteration INT, query_id INT, interval INT, seen REAL);
  // CREATE TABLE noisepage_forecast_parameters(iteration INT, db_id INT, query_id INT, query_text VARCHAR, types
  VARCHAR, parameters VARCHAR); size_t frequencies_idx = SIZE_MAX; size_t parameters_idx = SIZE_MAX;
  query_exec_util->BeginTransaction();
  {
    std::string query = "INSERT INTO noisepage_forecast_frequencies (?, ?, ?, ?)";
    std::vector<type::TypeId> param_types{type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER,
  type::TypeId::REAL}; std::vector<parser::ConstantValueExpression> params{
      parser::ConstantValueExpression(type::TypeId::INTEGER),
      parser::ConstantValueExpression(type::TypeId::INTEGER),
      parser::ConstantValueExpression(type::TypeId::INTEGER),
      parser::ConstantValueExpression(type::TypeId::REAL)};
    frequencies_idx = query_exec_util->PlanQuery(query,
        common::ManagedPointer(&params),
        common::ManagedPointer(&param_types));
  }

  {
    std::string query = "INSERT INTO noisepage_forecast_parameters (?, ?, ?, ?, ?, ?)";
    std::vector<type::TypeId> param_types{type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER,
  type::TypeId::VARCHAR, type::Typeid::VARCHAR, type::TypeId::VARCHAR}; std::vector<parser::ConstantValueExpression>
  params{ parser::ConstantValueExpression(type::TypeId::INTEGER),
      parser::ConstantValueExpression(type::TypeId::INTEGER),
      parser::ConstantValueExpression(type::TypeId::INTEGER),
      parser::ConstantValueExpression(type::TypeId::VARCHAR),
      parser::ConstantValueExpression(type::TypeId::VARCHAR),
      parser::ConstantValueExpression(type::TypeId::VARCHAR)};
    parameters_idx = query_exec_util->PlanQuery(query,
        common::ManagedPointer(&params),
        common::ManagedPointer(&param_types));
  }



  query_exec_util->EndTransaction(true);
  */
}

}  // namespace noisepage::metrics
