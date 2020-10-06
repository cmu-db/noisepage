#include "optimizer/statistics/column_stats.h"

#include "common/json.h"

namespace terrier::optimizer {

nlohmann::json ColumnStats::ToJson() const {
  nlohmann::json j;
  j["database_id"] = database_id_;
  j["table_id"] = table_id_;
  j["column_id"] = column_id_;
  j["num_rows"] = num_rows_;
  j["cardinality"] = cardinality_;
  j["frac_null"] = frac_null_;
  j["most_common_vals"] = most_common_vals_;
  j["most_common_freqs"] = most_common_freqs_;
  j["histogram_bounds"] = histogram_bounds_;
  j["is_basetable"] = is_base_table_;
  return j;
}

void ColumnStats::FromJson(const nlohmann::json &j) {
  database_id_ = j.at("database_id").get<catalog::db_oid_t>();
  table_id_ = j.at("table_id").get<catalog::table_oid_t>();
  column_id_ = j.at("column_id").get<catalog::col_oid_t>();
  num_rows_ = j.at("num_rows").get<size_t>();
  cardinality_ = j.at("cardinality").get<double>();
  frac_null_ = j.at("frac_null").get<double>();
  most_common_vals_ = j.at("most_common_vals").get<std::vector<double>>();
  most_common_freqs_ = j.at("most_common_freqs").get<std::vector<double>>();
  histogram_bounds_ = j.at("histogram_bounds").get<std::vector<double>>();
  is_base_table_ = j.at("is_basetable").get<bool>();
}

DEFINE_JSON_BODY_DECLARATIONS(ColumnStats);

}  // namespace terrier::optimizer
