#include "self_driving/forecast/workload_forecast.h"

#include <map>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "execution/exec_defs.h"
#include "metrics/query_trace_metric.h"
#include "parser/expression/constant_value_expression.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::selfdriving {

WorkloadForecast::WorkloadForecast(uint64_t forecast_interval) : forecast_interval_(forecast_interval) {
  LoadQueryText();
  LoadQueryTrace();
  CreateSegments();
}

/**
 * Queries in query_timestamp_to_id_ are sorted by their timestamp while allowing duplicate keys,
 * and then partitioned by timestamps and forecast_interval into segments.
 *
 * These segments will eventually store the result of workload/query arrival rate prediction.
 */
void WorkloadForecast::CreateSegments() {
  std::unordered_map<execution::query_id_t, uint64_t> curr_segment;

  uint64_t curr_time = query_timestamp_to_id_.begin()->first;

  // We assume the traces are sorted by timestamp in increasing order
  for (auto &it : query_timestamp_to_id_) {
    if (it.first > curr_time + forecast_interval_) {
      forecast_segments_.emplace_back(std::move(curr_segment));
      curr_time = it.first;
      curr_segment = std::unordered_map<execution::query_id_t, uint64_t>();
    }
    curr_segment[it.second] += 1;
  }

  if (!curr_segment.empty()) {
    forecast_segments_.emplace_back(std::move(curr_segment));
  }
  num_forecast_segment_ = forecast_segments_.size();
}

void WorkloadForecast::LoadQueryText() {
  std::string_view feat_cols = metrics::QueryTraceMetricRawData::FEATURE_COLUMNS[0];
  uint8_t num_cols = std::count(feat_cols.begin(), feat_cols.end(), ',') + 1;
  std::string_view tmp = feat_cols.substr(0, feat_cols.find("query_text"));
  uint8_t query_text_col = std::count(tmp.begin(), tmp.end(), ',');
  // Parse qid and query text, assuming they are the first two columns, with query text wrapped in quotations marks

  // Create an input filestream
  std::ifstream query_text_file(std::string(metrics::QueryTraceMetricRawData::FILES[0]).c_str());
  // Make sure the file is open
  if (!query_text_file.is_open())
    throw PILOT_EXCEPTION(fmt::format("Could not open file {}", metrics::QueryTraceMetricRawData::FILES[0]),
                          common::ErrorCode::ERRCODE_IO_ERROR);

  // Helper vars
  std::string line;
  if (!query_text_file.good()) throw PILOT_EXCEPTION("File stream is not good", common::ErrorCode::ERRCODE_IO_ERROR);

  // ignore header
  std::getline(query_text_file, line);

  bool parse_succ;
  uint64_t db_oid;
  execution::query_id_t query_id;
  size_t pos, colnum;
  std::string type_string;
  std::vector<std::string> val_vec(num_cols, "");

  // Read data, line by line
  while (std::getline(query_text_file, line)) {
    std::vector<type::TypeId> param_types;
    colnum = 0;
    parse_succ = true;
    val_vec.assign(num_cols, "");

    while ((pos = line.find(',')) != std::string::npos && colnum < num_cols) {
      // deal with the query_text col separately
      if (colnum == query_text_col) {
        pos = line.find("\",");
        if (pos == std::string::npos || pos < 2) {
          // no quotation mark found or no query_text found
          parse_succ = false;
          break;
        }
        val_vec[colnum] = line.substr(1, pos - 1);
        // skip the right quotation mark
        pos++;
      } else if (pos > 0) {
        val_vec[colnum] = line.substr(0, pos);
        if (val_vec[colnum].empty()) {
          // empty field
          parse_succ = false;
          break;
        }
      }
      line.erase(0, pos + 2);
      colnum++;
    }
    if (!parse_succ) continue;

    db_oid = static_cast<uint64_t>(std::stoi(val_vec[0]));
    query_id = static_cast<execution::query_id_t>(std::stoi(val_vec[1]));
    type_string = val_vec[4];
    query_id_to_text_[query_id] = val_vec[query_text_col];
    query_text_to_id_[val_vec[query_text_col]] = query_id;

    // extract each type in the type_string
    while ((pos = type_string.find(';')) != std::string::npos) {
      param_types.push_back(type::TypeUtil::TypeIdFromString(type_string.substr(0, pos)));
      type_string.erase(0, pos + 1);
    }

    query_id_to_dboid_[query_id] = db_oid;
    query_id_to_param_types_[query_id] = std::move(param_types);
  }
  // Close file
  query_text_file.close();
}

void WorkloadForecast::LoadQueryTrace() {
  std::string feat_cols = std::string{metrics::QueryTraceMetricRawData::FEATURE_COLUMNS[1]};
  uint8_t num_cols = std::count(feat_cols.begin(), feat_cols.end(), ',') + 1;

  // Create an input filestream
  std::ifstream trace_file(std::string(metrics::QueryTraceMetricRawData::FILES[1]).c_str());
  // Make sure the file is open
  if (!trace_file.is_open())
    throw PILOT_EXCEPTION(fmt::format("Could not open file {}", metrics::QueryTraceMetricRawData::FILES[1]),
                          common::ErrorCode::ERRCODE_IO_ERROR);

  // Helper vars
  std::string line, param_string;
  if (!trace_file.good()) throw PILOT_EXCEPTION("File stream is not good", common::ErrorCode::ERRCODE_IO_ERROR);

  // ignore header
  std::getline(trace_file, line);

  bool parse_succ;
  execution::query_id_t query_id;
  size_t pos, colnum;
  std::vector<std::string> val_vec(num_cols, "");

  // Read data, line by line
  while (std::getline(trace_file, line)) {
    colnum = 0;
    parse_succ = true;
    val_vec.assign(num_cols, "");
    // parse each field separated by , delimiter and store them in val_vec
    while ((pos = line.find(',')) != std::string::npos && colnum < num_cols) {
      if (pos > 0) {
        val_vec[colnum] = line.substr(0, pos);
      }
      if (val_vec[colnum].empty()) {
        // field not found
        parse_succ = false;
        break;
      }
      line.erase(0, pos + 2);
      colnum++;
    }

    if (!parse_succ) continue;

    query_id = static_cast<execution::query_id_t>(std::stoi(val_vec[0]));
    param_string = val_vec[2];

    // extract each parameter in the param_string
    std::vector<parser::ConstantValueExpression> param_vec;
    while ((pos = param_string.find(';')) != std::string::npos) {
      auto cve = parser::ConstantValueExpression::FromString(param_string.substr(0, pos),
                                                             query_id_to_param_types_[query_id][param_vec.size()]);
      param_vec.push_back(cve);
      param_string.erase(0, pos + 1);
    }

    if (query_id_to_params_[query_id].size() < num_sample_) {
      query_id_to_params_[query_id].push_back(param_vec);
    }
    query_timestamp_to_id_.insert(std::make_pair(std::stoull(val_vec[1]), query_id));
  }
  // Close file
  trace_file.close();
}

}  // namespace noisepage::selfdriving
