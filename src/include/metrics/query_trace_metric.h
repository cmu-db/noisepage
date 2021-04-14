#pragma once

#include <algorithm>
#include <chrono>  //NOLINT
#include <fstream>
#include <list>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/container/chunked_array.h"
#include "common/json_header.h"
#include "common/managed_pointer.h"
#include "common/reservoir_sampling.h"
#include "execution/exec_defs.h"
#include "metrics/abstract_metric.h"
#include "metrics/metrics_util.h"
#include "parser/expression/constant_value_expression.h"
#include "transaction/transaction_defs.h"

namespace noisepage::metrics {

class QueryTraceMetricRawData;

/**
 * Class responsible for tracking query execution metadata
 * at a granularity beyond record by record. This class is used
 * to maintain query execution information on a per-segment
 * and per-forecast basis.
 */
class QueryTraceMetadata {
 public:
  /** A tight struct for packing timestamp and query id */
  struct QueryTimeId {
    /** Timestamp of when the query was executed */
    uint64_t timestamp_;
    /** Identifier of executed query */
    execution::query_id_t qid_;
  };

  /** A tight struct to track db_oid, text, and param types */
  struct QueryMetadata {
    /** Database OID of the target query */
    catalog::db_oid_t db_oid_;
    /** Text of the target query */
    std::string text_;
    /** JSON-serialized param types of the target query */
    std::string param_type_;
  };

  QueryTraceMetadata() = default;

  /**
   * Records a query text
   * @param qid Query ID
   * @param db_oid Database OID
   * @param text Query text
   * @param param_type JSON serialized parameter types
   */
  void RecordQueryText(execution::query_id_t qid, catalog::db_oid_t db_oid, std::string text, std::string param_type) {
    // Assume qid is unique, don't re-record if already recorded
    NOISEPAGE_ASSERT(qmetadata_.find(qid) == qmetadata_.end(), "Expected query_text recording to be unique for qid");
    qmetadata_[qid] = QueryMetadata{db_oid, std::move(text), std::move(param_type)};
  }

  /**
   * Record a query parameter sample. We consider timestamp here since RecordQueryText
   * does not get invoked on every query execution. Rather, an invocation of a prepared
   * statement will invoke RecordQueryTrace.
   *
   * @param timestamp of query execution
   * @param qid query id
   * @param query_param JSON-serialized query parameter
   */
  void RecordQueryParamSample(uint64_t timestamp, execution::query_id_t qid, std::string query_param);

  /**
   * Merge another QueryTraceMetadata into this one.
   * @param other Other to merge
   */
  void Merge(QueryTraceMetadata *other) {
    // Directly merge hash tables
    qmetadata_.merge(other->qmetadata_);

    // Merge the reservoirs
    for (auto &it : other->qid_param_samples_) {
      // These are duplicate keys so need to merge reservoir
      if (qid_param_samples_.find(it.first) != qid_param_samples_.end()) {
        qid_param_samples_.find(it.first)->second.Merge(&it.second);
      } else {
        qid_param_samples_.emplace(std::move(it));
      }
    }

    // Combine time series
    timeseries_.Merge(&other->timeseries_);
  }

  /** Reset query metadata */
  void ResetQueryMetadata() {
    qmetadata_.clear();
    qid_param_samples_.clear();
  }

  /** Initialize an iterator for timeseries_ */
  void InitTimeseriesIterator() {
    iterator_ = timeseries_.begin();
    iterator_initialized_ = true;
  }

  /** Reset timeseries_ */
  void ResetTimeseries() {
    timeseries_.Clear();
    iterator_initialized_ = false;
  }

 private:
  friend class QueryTraceMetricRawData;
  std::unordered_map<execution::query_id_t, QueryMetadata> qmetadata_;
  std::unordered_map<execution::query_id_t, common::ReservoirSampling<std::string>> qid_param_samples_;
  common::ChunkedArray<QueryTimeId, 32> timeseries_;
  bool iterator_initialized_;
  common::ChunkedArray<QueryTimeId, 32>::Iterator<QueryTimeId, 32> iterator_;
};

/**
 * Raw data object for holding stats collected at logging level
 */
class QueryTraceMetricRawData : public AbstractRawData {
 public:
  /** Parameter of how many query params to keep in sample */
  static uint64_t query_param_sample;

  /** Parameter controlling size of a query segment */
  static uint64_t query_segment_interval;

  /** Query string for recording observed queries */
  static constexpr char QUERY_OBSERVED_INSERT_STMT[] = "INSERT INTO noisepage_forecast_frequencies VALUES ($1, $2, $3)";

  void Aggregate(AbstractRawData *other) override {
    auto other_db_metric = dynamic_cast<QueryTraceMetricRawData *>(other);
    if (!other_db_metric->query_text_.empty()) {
      query_text_.splice(query_text_.cend(), other_db_metric->query_text_);
    }
    if (!other_db_metric->query_trace_.empty()) {
      query_trace_.splice(query_trace_.cend(), other_db_metric->query_trace_);
    }

    // Merge data and update timestamp
    metadata_.Merge(&other_db_metric->metadata_);
    low_timestamp_ = std::min(low_timestamp_, other_db_metric->low_timestamp_);
    high_timestamp_ = std::max(high_timestamp_, other_db_metric->high_timestamp_);
  }

  /**
   * @return the type of the metric this object is holding the data for
   */
  MetricsComponent GetMetricType() const override { return MetricsComponent::QUERY_TRACE; }

  void ToDB(common::ManagedPointer<task::TaskManager> task_manager) final;

  /**
   * Perform a write to the internal tables. Inserts are performed asynchronously on a background thread.
   * In case the data is needed immediately, out_metadata and out_params can be used for passing
   * the data out of this class.
   *
   * @param task_manager Task manager to submit jobs to
   * @param write_parameters Whether to write all parameters or not
   * @param write_timestamp Timestamp at which WriteToDB was invoked
   * @param out_metadata Pass out cached query metadata
   * @param out_params Pass out cached parameters
   */
  void WriteToDB(common::ManagedPointer<task::TaskManager> task_manager, bool write_parameters,
                 uint64_t write_timestamp,
                 std::unordered_map<execution::query_id_t, QueryTraceMetadata::QueryMetadata> *out_metadata,
                 std::unordered_map<execution::query_id_t, std::vector<std::string>> *out_params);

  /**
   * Writes the data out to ofstreams
   * @param outfiles vector of ofstreams to write to that have been opened by the MetricsManager
   */
  void ToCSV(std::vector<std::ofstream> *const outfiles) final {
    NOISEPAGE_ASSERT(outfiles->size() == FILES.size(), "Number of files passed to metric is wrong.");
    NOISEPAGE_ASSERT(std::count_if(outfiles->cbegin(), outfiles->cend(),
                                   [](const std::ofstream &outfile) { return !outfile.is_open(); }) == 0,
                     "Not all files are open.");

    auto &query_text_outfile = (*outfiles)[0];
    auto &query_trace_outfile = (*outfiles)[1];

    for (const auto &data : query_text_) {
      query_text_outfile << data.db_oid_ << ", " << data.query_id_ << ", " << data.timestamp_ << ", "
                         << data.query_text_ << ", " << data.type_string_ << ", ";
      query_text_outfile << std::endl;
    }
    for (const auto &data : query_trace_) {
      query_trace_outfile << data.db_oid_ << ", " << data.query_id_ << ", " << data.timestamp_ << ", "
                          << data.param_string_ << ", ";
      query_trace_outfile << std::endl;
    }
    query_text_.clear();
    query_trace_.clear();
  }

  /**
   * Files to use for writing to CSV.
   */
  static constexpr std::array<std::string_view, 2> FILES = {"./query_text.csv", "./query_trace.csv"};
  /**
   * Columns to use for writing to CSV.
   * Note: This includes the columns for the input feature, but not the output (resource counters)
   */
  static constexpr std::array<std::string_view, 2> FEATURE_COLUMNS = {
      "db_oid, query_id, timestamp, query_text, parameter_type", "db_oid, query_id, timestamp, parameters"};

 private:
  friend class QueryTraceMetric;
  FRIEND_TEST(MetricsTests, QueryCSVTest);

  /**
   * Submit job to update internal history table
   * @param timestamp Timestamp to record frequency information at
   * @param freqs Map of query id to frequency
   * @param task_manager Task Manager to submit the jobs to
   */
  void SubmitFrequencyRecordJob(uint64_t timestamp, std::unordered_map<execution::query_id_t, int> &&freqs,
                                common::ManagedPointer<task::TaskManager> task_manager);

  void RecordQueryText(catalog::db_oid_t db_oid, const execution::query_id_t query_id, const std::string &query_text,
                       const std::string &type_string, const std::string &type_json, const uint64_t timestamp) {
    query_text_.emplace_back(db_oid, query_id, query_text, type_string, timestamp);
    metadata_.RecordQueryText(query_id, db_oid, query_text, type_json);

    // Don't track range of time data for QueryText.
    // When query is run, RecordQueryTrace is invoked but not necessarily RecordQueryText.
  }

  void RecordQueryTrace(catalog::db_oid_t db_oid, const execution::query_id_t query_id, const uint64_t timestamp,
                        const std::string &param_string, const std::string &param_json) {
    query_trace_.emplace_back(db_oid, query_id, timestamp, param_string);
    metadata_.RecordQueryParamSample(timestamp, query_id, param_json);

    // Track range of time data
    low_timestamp_ = std::min(low_timestamp_, timestamp);
    high_timestamp_ = std::max(high_timestamp_, timestamp);
  }

  struct QueryText {
    QueryText(catalog::db_oid_t db_oid, const execution::query_id_t query_id, std::string query_text,
              std::string type_string, const uint64_t timestamp)
        : db_oid_(db_oid),
          query_id_(query_id),
          timestamp_(timestamp),
          query_text_(std::move(query_text)),
          type_string_(std::move(type_string)) {}
    const catalog::db_oid_t db_oid_;
    const execution::query_id_t query_id_;
    const uint64_t timestamp_;
    const std::string query_text_;
    const std::string type_string_;
  };

  struct QueryTrace {
    QueryTrace(catalog::db_oid_t db_oid, const execution::query_id_t query_id, const uint64_t timestamp,
               std::string param_string)
        : db_oid_(db_oid), query_id_(query_id), timestamp_(timestamp), param_string_(std::move(param_string)) {}
    const catalog::db_oid_t db_oid_;
    const execution::query_id_t query_id_;
    const uint64_t timestamp_;
    const std::string param_string_;
  };

  std::list<QueryText> query_text_;
  std::list<QueryTrace> query_trace_;
  QueryTraceMetadata metadata_;
  uint64_t low_timestamp_{UINT64_MAX};
  uint64_t high_timestamp_{0};
};

/**
 * Metrics for the logging components of the system: currently buffer consumer (writes to disk) and the record
 * serializer
 */
class QueryTraceMetric : public AbstractMetric<QueryTraceMetricRawData> {
 private:
  friend class MetricsStore;

  void RecordQueryText(catalog::db_oid_t db_oid, execution::query_id_t query_id, const std::string &query_text,
                       common::ManagedPointer<const std::vector<parser::ConstantValueExpression>> param,
                       uint64_t timestamp);

  void RecordQueryTrace(catalog::db_oid_t db_oid, execution::query_id_t query_id, uint64_t timestamp,
                        common::ManagedPointer<const std::vector<parser::ConstantValueExpression>> param);
};
}  // namespace noisepage::metrics
