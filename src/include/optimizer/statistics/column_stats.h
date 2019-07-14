#pragma once

#include <sstream>

#include "catalog/catalog_defs.h"
#include "common/macros.h"

namespace terrier::optimizer {

class ColumnStats {
 public:
  ColumnStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id, catalog::col_oid_t column_id,
              const std::string &column_name, bool has_index, size_t num_rows,
              double cardinality, double frac_null,
              std::vector<double> most_common_vals,
              std::vector<double> most_common_freqs,
              std::vector<double> histogram_bounds)
      : database_id_(database_id),
        table_id_(table_id),
        column_id_(column_id),
        column_name_(column_name),
        has_index_(has_index),
        num_rows_(num_rows),
        cardinality_(cardinality),
        frac_null_(frac_null),
        most_common_vals_(std::move(most_common_vals)),
        most_common_freqs_(std::move(most_common_freqs)),
        histogram_bounds_(std::move(histogram_bounds)),
        is_basetable_{true} {}

  void UpdateJoinStats(size_t table_num_rows, size_t sample_size,
                       size_t sample_card) {
    num_rows_ = table_num_rows;

    // FIX ME: for now using samples's cardinality * samples size / number of
    // rows to ensure the same selectivity among samples and the whole table
    auto estimated_card =
        (size_t)(sample_card * num_rows_ / (double)sample_size);
    cardinality_ = cardinality_ < estimated_card ? cardinality_ : estimated_card;
  }

  nlohmann::json ToJson() const {
    nlohmann::json j;
    j["database_id"] = database_id_;
    j["table_id"] = table_id_;
    j["column_id"] = column_id_;
    j["column_name"] = column_name_;
    j["has_index"] = has_index_;
    j["num_rows"] = num_rows_;
    j["cardinality"] = cardinality_;
    j["frac_null"] = frac_null_;
    j["most_common_vals"] = most_common_vals_;
    j["most_common_freqs"] = most_common_freqs_;
    j["histogram_bounds"] = histogram_bounds_;
    j["is_basetable"] = is_basetable_;
    return
  }

  void FromJson(const nlohmann::json &j) {
    database_id_ = j.at("database_id").get<catalog::db_oid_t>();
    table_id_ = j.at("table_id").get<catalog::table_oid_t>();
    column_id_ = j.at("column_id").get<catalog::col_oid_t>();
    column_name_ = j.at("column_name").get<std::string>();
    has_index_ = j.at("has_index").get<bool>();
    num_rows_ = j.at("num_rows").get<size_t>();
    cardinality_ = j.at("cardinality").get<double>();
    frac_null_ = j.at("frac_null").get<double>();
    most_common_vals_ = j.at("most_common_vals").get<std::vector<double>>();
    most_common_freqs_ = j.at("most_common_freqs").get<std::vector<double>>();
    histogram_bounds_ = j.at("histogram_bounds").get<std::vector<double>>();
    is_basetable_ = j.at("is_basetable").get<bool>();
  }


  private:
    catalog::db_oid_t database_id_;
    catalog::table_oid_t table_id_;
    catalog::col_oid_t column_id_;
    std::string column_name_;
    bool has_index_;

    size_t num_rows_;
    double cardinality_;
    double frac_null_;
    std::vector<double> most_common_vals_;
    std::vector<double> most_common_freqs_;
    std::vector<double> histogram_bounds_;

    bool is_basetable_;
};

}
