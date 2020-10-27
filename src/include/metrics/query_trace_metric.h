#pragma once

#include <algorithm>
#include <chrono>  //NOLINT
#include <fstream>
#include <list>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "metrics/abstract_metric.h"
#include "metrics/metrics_util.h"
#include "transaction/transaction_defs.h"

namespace noisepage::metrics {

/**
 * Raw data object for holding stats collected at logging level
 */
class QueryTraceMetricRawData : public AbstractRawData {
 public:
  void Aggregate(AbstractRawData *const other) override {
    auto other_db_metric = dynamic_cast<QueryTraceMetricRawData *>(other);
    if (!other_db_metric->query_text_.empty()) {
      query_text_.splice(query_text_.cend(), other_db_metric->query_text_);
    }
    if (!other_db_metric->query_trace_.empty()) {
      query_trace_.splice(query_trace_.cend(), other_db_metric->query_trace_);
    }
  }

  /**
   * @return the type of the metric this object is holding the data for
   */
  MetricsComponent GetMetricType() const override { return MetricsComponent::QUERY_TRACE; }

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
      query_text_outfile << data.query_id_ << ", " << data.query_text_ << ", " << data.timestamp_ << ", ";
      query_text_outfile << std::endl;
    }
    for (const auto &data : query_trace_) {
      query_trace_outfile << data.query_id_ << ", " << data.timestamp_ << ", ";
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
  static constexpr std::array<std::string_view, 2> FEATURE_COLUMNS = {"query_id, query_text, timestamp",
                                                                      "query_id, timestamp"};

 private:
  friend class QueryTraceMetric;
  FRIEND_TEST(MetricsTests, QueryCSVTest);

  void RecordQueryText(const execution::query_id_t query_id, const std::string &query_text, const uint64_t timestamp) {
    query_text_.emplace_back(query_id, query_text, timestamp);
  }

  void RecordQueryTrace(const execution::query_id_t query_id, const uint64_t timestamp) {
    query_trace_.emplace_back(query_id, timestamp);
  }

  struct QueryText {
    QueryText(const execution::query_id_t query_id, std::string query_text, const uint64_t timestamp)
        : query_id_(query_id), timestamp_(timestamp), query_text_(std::move(query_text)) {}
    const execution::query_id_t query_id_;
    const uint64_t timestamp_;
    std::string query_text_;
  };

  struct QueryTrace {
    QueryTrace(const execution::query_id_t query_id, const uint64_t timestamp)
        : query_id_(query_id), timestamp_(timestamp) {}
    const execution::query_id_t query_id_;
    const uint64_t timestamp_;
  };

  std::list<QueryText> query_text_;
  std::list<QueryTrace> query_trace_;
};

/**
 * Metrics for the logging components of the system: currently buffer consumer (writes to disk) and the record
 * serializer
 */
class QueryTraceMetric : public AbstractMetric<QueryTraceMetricRawData> {
 private:
  friend class MetricsStore;

  void RecordQueryText(const execution::query_id_t query_id, const std::string &query_text, const uint64_t timestamp) {
    GetRawData()->RecordQueryText(query_id, query_text, timestamp);
  }
  void RecordQueryTrace(const execution::query_id_t query_id, const uint64_t timestamp) {
    GetRawData()->RecordQueryTrace(query_id, timestamp);
  }
};
}  // namespace noisepage::metrics
