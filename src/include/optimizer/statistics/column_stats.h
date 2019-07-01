#pragma once

#include <sstream>

#include "common/macros.h"

namespace terrier {
namespace optimizer {

class ColumnStats {
 public:
  ColumnStats(oid_t database_id, oid_t table_id, oid_t column_id,
              const std::string column_name, bool has_index, size_t num_rows,
              double cardinality, double frac_null,
              std::vector<double> most_common_vals,
              std::vector<double> most_common_freqs,
              std::vector<double> histogram_bounds)
      : database_id(database_id),
        table_id(table_id),
        column_id(column_id),
        column_name(column_name),
        has_index(has_index),
        num_rows(num_rows),
        cardinality(cardinality),
        frac_null(frac_null),
        most_common_vals(most_common_vals),
        most_common_freqs(most_common_freqs),
        histogram_bounds(histogram_bounds),
        is_basetable{true} {}

  void UpdateJoinStats(size_t table_num_rows, size_t sample_size,
                       size_t sample_card) {
    num_rows = table_num_rows;

    // FIX ME: for now using samples's cardinality * samples size / number of
    // rows to ensure the same selectivity among samples and the whole table
    size_t estimated_card =
        (size_t)(sample_card * num_rows / (double)sample_size);
    cardinality = cardinality < estimated_card ? cardinality : estimated_card;
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
    j["most_common_freqs"] = most_common_freqs;
    j["histogram_bounds"] = histogram_bounds_;
    j["is_basetable"] = is_basetable_;
    return
  }

  private:
    oid_t database_id_;
    oid_t table_id_;
    oid_t column_id_;
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

}  // namespace optimizer
}  // namespace terrier
