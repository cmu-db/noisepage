#pragma once

#include <sstream>

#include "catalog/catalog_defs.h"
#include "common/macros.h"

namespace terrier::optimizer {

class ColumnStats {
 public:
  ColumnStats(catalog::namespace_oid_t namespace_id, catalog::db_oid_t database_id, catalog::table_oid_t table_id,
              catalog::col_oid_t column_id, size_t num_rows,
              double cardinality, double frac_null,
              std::vector<double> most_common_vals,
              std::vector<double> most_common_freqs,
              std::vector<double> histogram_bounds,
              bool is_base_table)
      : namespace_id_(namespace_id),
        database_id_(database_id),
        table_id_(table_id),
        column_id_(column_id),
        num_rows_(num_rows),
        cardinality_(cardinality),
        frac_null_(frac_null),
        most_common_vals_(std::move(most_common_vals)),
        most_common_freqs_(std::move(most_common_freqs)),
        histogram_bounds_(std::move(histogram_bounds)),
        is_base_table_{is_base_table} {}

  inline catalog::col_oid_t GetColumnID() { return column_id_; }

  inline size_t& GetNumRows() { return this->num_rows_; }

  inline double& GetCardinality() { return this->cardinality_; }

  ColumnStats() = default;

  virtual ~ColumnStats() = default;

  nlohmann::json ToJson() const {
    nlohmann::json j;
    j["namespace_id"] = namespace_id_;
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

  void FromJson(const nlohmann::json &j) {
    namespace_id_ = j.at("namespace_id").get<catalog::namespace_oid_t>();
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

  private:
    catalog::namespace_oid_t namespace_id_;
    catalog::db_oid_t database_id_;
    catalog::table_oid_t table_id_;
    catalog::col_oid_t column_id_;

    size_t num_rows_;
    double cardinality_;
    double frac_null_;
    std::vector<double> most_common_vals_;
    std::vector<double> most_common_freqs_;
    std::vector<double> histogram_bounds_;

    bool is_base_table_;
};
  DEFINE_JSON_DECLARATIONS(ColumnStats)
}
