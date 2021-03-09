#include "metrics/query_trace_metric.h"
#include "common/json.h"
#include "execution/sql/value_util.h"
#include "self_driving/planning/pilot.h"
#include "util/query_exec_util.h"
#include "util/query_internal_thread.h"

namespace noisepage::metrics {

uint64_t QueryTraceMetricRawData::QUERY_PARAM_SAMPLE = 5;
uint64_t QueryTraceMetricRawData::QUERY_SEGMENT_INTERVAL = 0;

void QueryTraceMetadata::RecordQueryParamSample(uint64_t timestamp, execution::query_id_t qid,
                                                std::string query_param) {
  if (qid_param_samples_.find(qid) == qid_param_samples_.end()) {
    qid_param_samples_.emplace(qid,
                               common::ReservoirSampling<std::string>(QueryTraceMetricRawData::QUERY_PARAM_SAMPLE));
  }

  // Record the sample and time event
  qid_param_samples_.find(qid)->second.AddSample(query_param);
  timeseries_.push(QueryTimeId{timestamp, qid});
}

void QueryTraceMetricRawData::ToDB(common::ManagedPointer<util::QueryExecUtil> query_exec_util,
                                   common::ManagedPointer<util::QueryInternalThread> query_internal_thread) {
  // On regular ToDB calls from metrics manager, we don't want to flush the time data or parameters.
  // Only on a forecast interval should we be doing that. Rather, ToDB will write out time-series data
  // only if a segment has elapsed.
  WriteToDB(query_exec_util, query_internal_thread, false, false, nullptr, nullptr);
}

void QueryTraceMetricRawData::WriteToDB(
    common::ManagedPointer<util::QueryExecUtil> query_exec_util,
    common::ManagedPointer<util::QueryInternalThread> query_internal_thread, bool flush_timeseries,
    bool write_parameters, std::unordered_map<execution::query_id_t, QueryTraceMetadata::QueryMetadata> *out_metadata,
    std::unordered_map<execution::query_id_t, std::vector<std::string>> *out_params) {
  NOISEPAGE_ASSERT(query_exec_util != nullptr && query_internal_thread != nullptr,
                   "Internal execution utility not initialized");

  auto iteration = selfdriving::Pilot::GetCurrentPlanIteration();
  if (write_parameters) {
    {
      // Submit a job to update the query text data
      util::ExecuteRequest texts;
      texts.type_ = util::RequestType::DML;
      texts.notify_ = nullptr;
      texts.db_oid_ = catalog::INVALID_DATABASE_OID;
      texts.query_text_ = "INSERT INTO noisepage_forecast_texts VALUES ($1, $2, $3, $4)";
      texts.param_types_ = {type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::VARCHAR, type::TypeId::VARCHAR};
      for (auto &data : metadata_.qmetadata_) {
        std::vector<parser::ConstantValueExpression> params(4);
        params[0] = parser::ConstantValueExpression(
            type::TypeId::INTEGER,
            execution::sql::Integer(static_cast<int64_t>(data.second.db_oid_.UnderlyingValue())));
        params[1] = parser::ConstantValueExpression(type::TypeId::INTEGER,
                                                    execution::sql::Integer(data.first.UnderlyingValue()));

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
        texts.params_.emplace_back(std::move(params));
      }

      if (!texts.params_.empty()) {
        query_internal_thread->AddRequest(std::move(texts));
      }

      if (out_metadata) {
        *out_metadata = metadata_.qmetadata_;
      }
    }

    {
      // Submit a job to update the parameters table
      util::ExecuteRequest params;
      params.type_ = util::RequestType::DML;
      params.notify_ = nullptr;
      params.db_oid_ = catalog::INVALID_DATABASE_OID;
      params.query_text_ = "INSERT INTO noisepage_forecast_parameters VALUES ($1, $2, $3)";
      params.param_types_ = {type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::VARCHAR};

      for (auto &data : metadata_.qid_param_samples_) {
        std::vector<std::string> samples = data.second.GetSamples();
        for (auto &sample : samples) {
          std::vector<parser::ConstantValueExpression> param_vec(3);
          param_vec[0] = parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(iteration));
          param_vec[1] = parser::ConstantValueExpression(type::TypeId::INTEGER,
                                                         execution::sql::Integer(data.first.UnderlyingValue()));

          const auto string = std::string_view(sample);
          auto string_val = execution::sql::ValueUtil::CreateStringVal(string);
          param_vec[2] =
              parser::ConstantValueExpression(type::TypeId::VARCHAR, string_val.first, std::move(string_val.second));
          params.params_.emplace_back(std::move(param_vec));
        }

        if (out_params != NULL) {
          (*out_params)[data.first] = std::move(samples);
        }
      }

      if (!params.params_.empty()) {
        query_internal_thread->AddRequest(std::move(params));
      }
    }

    metadata_.ResetQueryMetadata();
  }

  if (!flush_timeseries && high_timestamp_ - low_timestamp_ < QUERY_SEGMENT_INTERVAL) {
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

  util::ExecuteRequest seen;
  seen.type_ = util::RequestType::DML;
  seen.notify_ = nullptr;
  seen.db_oid_ = catalog::INVALID_DATABASE_OID;
  seen.query_text_ = "INSERT INTO noisepage_forecast_frequencies VALUES ($1, $2, $3, $4)";
  seen.param_types_ = {type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::REAL};

  std::unordered_map<execution::query_id_t, int> freqs;
  while (metadata_.iterator_ != metadata_.timeseries_.end()) {
    if (!flush_timeseries && (high_timestamp_ < low_timestamp_ + QUERY_SEGMENT_INTERVAL)) {
      // If we aren't flushing and a query segment has not passed
      break;
    }

    if ((*metadata_.iterator_).timestamp_ >= low_timestamp_ + QUERY_SEGMENT_INTERVAL) {
      // In this case, the iterator has moved to a point such that we have a complete segment.
      // Submit the insert job based on the accumulated frequency information.
      if (!freqs.empty()) {
        for (auto &info : freqs) {
          std::vector<parser::ConstantValueExpression> param_vec(4);
          param_vec[0] = parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(iteration));
          param_vec[1] = parser::ConstantValueExpression(type::TypeId::INTEGER,
                                                         execution::sql::Integer(info.first.UnderlyingValue()));
          param_vec[2] =
              parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(segment_number_));
          param_vec[3] = parser::ConstantValueExpression(type::TypeId::REAL,
                                                         execution::sql::Real(static_cast<double>(info.second)));
          seen.params_.emplace_back(std::move(param_vec));
        }

        // Submit the insert request if not empty
        query_internal_thread->AddRequest(std::move(seen));
        freqs.clear();

        // Reset the metadata
        seen.type_ = util::RequestType::DML;
        seen.notify_ = nullptr;
        seen.db_oid_ = catalog::INVALID_DATABASE_OID;
        seen.query_text_ = "INSERT INTO noisepage_forecast_frequencies VALUES ($1, $2, $3, $4)";
        seen.param_types_ = {type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::INTEGER, type::TypeId::REAL};
        segment_number_++;
      }

      // Bumped up the low_timestamp_. We can't publish this data record yet
      // because the segment might not be ready yet. So we loop back around
      // for another round>
      low_timestamp_ += QUERY_SEGMENT_INTERVAL;
      continue;
    } else {
      // Update freqs with a frequency information
      freqs[(*metadata_.iterator_).qid_] += 1;
    }

    // Advance the iterator
    metadata_.iterator_++;
  }

  if (!freqs.empty()) {
    // Flush any remaining data. For instance, if the iterator ended on a segment boundary
    // or if we're flushing all timeseries data out.
    for (auto &info : freqs) {
      std::vector<parser::ConstantValueExpression> param_vec(4);
      param_vec[0] = parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(iteration));
      param_vec[1] =
          parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(info.first.UnderlyingValue()));
      param_vec[2] = parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(segment_number_));
      param_vec[3] =
          parser::ConstantValueExpression(type::TypeId::REAL, execution::sql::Real(static_cast<double>(info.second)));
      seen.params_.emplace_back(std::move(param_vec));
    }

    query_internal_thread->AddRequest(std::move(seen));
  }

  if (flush_timeseries || metadata_.iterator_ == metadata_.timeseries_.end()) {
    // Only reset the segment number if flushed. Note that flush_timeseries
    // corresponds to a new forecast interval
    if (flush_timeseries) {
      segment_number_ = 0;
    }
    metadata_.ResetTimeseries();

    // Reset times since all data flushed
    low_timestamp_ = UINT64_MAX;
    high_timestamp_ = 0;
  } else {
    // Set the low_timestamp to where iterator currently is
    low_timestamp_ = (*metadata_.iterator_).timestamp_;
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
      param_strs.push_back("");
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
