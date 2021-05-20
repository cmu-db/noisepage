#include "metrics/query_trace_metric.h"
#include "common/json.h"
#include "execution/sql/value_util.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "self_driving/planning/pilot.h"
#include "task/task_manager.h"
#include "util/self_driving_recording_util.h"

namespace noisepage::metrics {

uint64_t QueryTraceMetricRawData::query_param_sample = 5;
uint64_t QueryTraceMetricRawData::query_segment_interval = 0;

void QueryTraceMetadata::RecordQueryParamSample(uint64_t timestamp, execution::query_id_t qid,
                                                std::string query_param) {
  if (qid_param_samples_.find(qid) == qid_param_samples_.end()) {
    qid_param_samples_.emplace(qid,
                               common::ReservoirSampling<std::string>(QueryTraceMetricRawData::query_param_sample));
  }

  // Record the sample and time event
  qid_param_samples_.find(qid)->second.AddSample(std::move(query_param));
  timeseries_.Push(QueryTimeId{timestamp, qid});
}

void QueryTraceMetricRawData::SubmitFrequencyRecordJob(uint64_t timestamp,
                                                       std::unordered_map<execution::query_id_t, int> &&freqs,
                                                       common::ManagedPointer<task::TaskManager> task_manager) {
  std::string query = QueryTraceMetricRawData::QUERY_OBSERVED_INSERT_STMT;
  std::vector<std::vector<parser::ConstantValueExpression>> params_vec;
  for (auto &info : freqs) {
    std::vector<parser::ConstantValueExpression> param_vec(3);

    // Since the frequency information is per segment interval,
    // we record the timestamp (i.e., low_timestamp_) corresponding to it.
    param_vec[0] = parser::ConstantValueExpression(type::TypeId::BIGINT, execution::sql::Integer(timestamp));

    // Record the query identifier.
    param_vec[1] =
        parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(info.first.UnderlyingValue()));

    // Record how many queries seen
    param_vec[2] =
        parser::ConstantValueExpression(type::TypeId::REAL, execution::sql::Real(static_cast<double>(info.second)));
    params_vec.emplace_back(std::move(param_vec));
  }

  // Submit the insert request if not empty
  std::vector<type::TypeId> param_types = {type::TypeId::BIGINT, type::TypeId::INTEGER, type::TypeId::REAL};
  task_manager->AddTask(std::make_unique<task::TaskDML>(catalog::INVALID_DATABASE_OID, query,
                                                        std::make_unique<optimizer::TrivialCostModel>(), false,
                                                        std::move(params_vec), std::move(param_types)));
}

void QueryTraceMetricRawData::ToDB(common::ManagedPointer<task::TaskManager> task_manager) {
  // On regular ToDB calls from metrics manager, we don't want to flush the time data or parameters.
  // Only on a forecast interval should we be doing that. Rather, ToDB will write out time-series data
  // only if a segment has elapsed.
  uint64_t timestamp = metrics::MetricsUtil::Now();
  WriteToDB(task_manager, false, timestamp, nullptr, nullptr);
}

void QueryTraceMetricRawData::WriteToDB(
    common::ManagedPointer<task::TaskManager> task_manager, bool write_parameters, uint64_t write_timestamp,
    std::unordered_map<execution::query_id_t, QueryTraceMetadata::QueryMetadata> *out_metadata,
    std::unordered_map<execution::query_id_t, std::vector<std::string>> *out_params) {
  NOISEPAGE_ASSERT(task_manager != nullptr, "Task Manager not initialized");

  if (write_parameters) {
    util::SelfDrivingRecordingUtil::RecordQueryMetadata(metadata_.qmetadata_, task_manager);
    if (out_metadata != nullptr) {
      *out_metadata = metadata_.qmetadata_;
    }

    util::SelfDrivingRecordingUtil::RecordQueryParameters(write_timestamp, &metadata_.qid_param_samples_, task_manager,
                                                          out_params);
    metadata_.ResetQueryMetadata();
  }

  // We update the high_timestamp under the assumption that metrics::MetricsUtil::Now()
  // gives us a monotonically increasing clock time. This alows the periodic metrics
  // thread to be able to commit segments.
  high_timestamp_ = std::max(high_timestamp_, write_timestamp);
  if (high_timestamp_ - low_timestamp_ < QueryTraceMetricRawData::query_segment_interval) {
    // Not ready to write data records out
    return;
  }

  if (!metadata_.iterator_initialized_) {
    // Initialize the timseries iterator. The beauty of this iterator is that the iterator
    // is resilient to chunks being merge-added (i.e., the iterator will be able to scan
    // through added chunks).
    //
    // ASSUME: Aggregate() and ToDB() cannot be called together.
    metadata_.InitTimeseriesIterator();
  }

  std::unordered_map<execution::query_id_t, int> freqs;
  while (metadata_.iterator_ != metadata_.timeseries_.end()) {
    if (high_timestamp_ < low_timestamp_ + QueryTraceMetricRawData::query_segment_interval) {
      // We don't have enough data to compose a query segment
      break;
    }

    if ((*metadata_.iterator_).timestamp_ >= low_timestamp_ + QueryTraceMetricRawData::query_segment_interval) {
      // In this case, the iterator has moved to a point such that we have a complete segment.
      // Submit the insert job based on the accumulated frequency information.
      if (!freqs.empty()) {
        SubmitFrequencyRecordJob(low_timestamp_, std::move(freqs), task_manager);
        freqs.clear();
      }

      // Bump up the low_timestamp_
      low_timestamp_ += QueryTraceMetricRawData::query_segment_interval;

      // The above line has bumped up the low_timestamp_ into a new segment.
      // The following check makes sure that we have a whole segment of data
      // present before we try to consume it.
      //
      // Assume segment_interval = 10 and the following data points are
      // available: 0, 5, 10, 15, 20, 25; the current high_timestamp_ = 29.
      //
      // In this case, [0, 5] are committed as 1 segment. [10, 15] are committed
      // as another segment. However, [20, 25] cannot be committed yet
      // since high_timestamp_ (29) - low_timestamp_ (20) < segment_interval (10)
      //
      if (high_timestamp_ < low_timestamp_ + QueryTraceMetricRawData::query_segment_interval) {
        break;
      }
    }

    // Update freqs with a frequency information
    freqs[(*metadata_.iterator_).qid_] += 1;

    // Advance the iterator
    metadata_.iterator_++;
  }

  // Since the high_timestamp_ is also controlled by the time the MetricsThread performs
  // logging to disk or internal tables, it is possible for [freqs] to contain a full
  // segment of data.
  //
  // This only happens in the following state:
  // 1. The timeseries data has been fully read. If there is still remaining timeseries
  //    data, then we should not be committing a segment here.
  //
  // 2. A segment interval has passed
  if (high_timestamp_ >= low_timestamp_ + QueryTraceMetricRawData::query_segment_interval && !freqs.empty()) {
    NOISEPAGE_ASSERT(metadata_.iterator_ == metadata_.timeseries_.end(), "Expect the timseries data to be exhausted");

    SubmitFrequencyRecordJob(low_timestamp_, std::move(freqs), task_manager);
    low_timestamp_ += QueryTraceMetricRawData::query_segment_interval;
    freqs.clear();
  }

  // This assert is inserted here to verify that we do not drop data from any segment.
  NOISEPAGE_ASSERT(freqs.empty(), "We should only have been writing out complete segments");
  if (metadata_.iterator_ == metadata_.timeseries_.end()) {
    // If we've exhausted all of the timeseries data, we can delete it.
    // If timeseries data consumes too much memory, we might have to consider
    // pruning some chunks while keeping the iterator stable.
    metadata_.ResetTimeseries();
  }
}

void QueryTraceMetric::RecordQueryText(catalog::db_oid_t db_oid, const execution::query_id_t query_id,
                                       const std::string &query_text,
                                       common::ManagedPointer<const std::vector<parser::ConstantValueExpression>> param,
                                       const uint64_t timestamp) {
  std::ostringstream type_stream;
  std::vector<std::string> type_strs;
  for (const auto &val : (*param)) {
    auto tstr = type::TypeUtil::TypeIdToString(val.GetReturnValueType());
    type_strs.push_back(tstr);
    type_stream << tstr << ";";
  }

  std::string type_str;
  {
    nlohmann::json j = type_strs;
    type_str = j.dump();
  }

  // We need both the JSON-serialized string and the ';'-delimited form.
  GetRawData()->RecordQueryText(db_oid, query_id, "\"" + query_text + "\"", type_stream.str(), type_str, timestamp);
}

void QueryTraceMetric::RecordQueryTrace(
    catalog::db_oid_t db_oid, const execution::query_id_t query_id, const uint64_t timestamp,
    common::ManagedPointer<const std::vector<parser::ConstantValueExpression>> param) {
  std::ostringstream param_stream;
  std::vector<std::string> param_strs;
  for (const auto &val : (*param)) {
    if (val.IsNull()) {
      param_strs.emplace_back("");
      param_stream << "";
    } else {
      auto valstr = val.ToString();
      param_strs.push_back(valstr);
      param_stream << valstr;
    }

    param_stream << ";";
  }

  std::string param_str;
  {
    nlohmann::json j = param_strs;
    param_str = j.dump();
  }

  // We need both the JSON-serialized string and the ';'-delimited form.
  GetRawData()->RecordQueryTrace(db_oid, query_id, timestamp, param_stream.str(), param_str);
}

}  // namespace noisepage::metrics
