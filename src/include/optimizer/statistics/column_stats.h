#pragma once

#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/json_header.h"
#include "common/macros.h"

namespace noisepage::optimizer {
/**
 * Represents the statistics of a given column. Stores relevant oids,
 * important trends (most common values/their frequencies in the column), and other
 * useful information.
 */
class ColumnStats {
 public:
  /**
   * Constructor
   * @param database_id - database oid of column
   * @param table_id - table oid of column
   * @param column_id - column oid of column
   * @param num_rows - number of rows in column
   * @param cardinality - cardinality of column
   * @param frac_null - fraction of null values out of total values in column
   * @param most_common_vals - list of most common values in the column
   * @param most_common_freqs - list of the frequencies of the most common values in the column
   * @param histogram_bounds - the bounds of the histogram of the column e.g. (1.0 - 4.0)
   * @param is_base_table - indicates whether the column is from a base table
   */
  ColumnStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id, catalog::col_oid_t column_id,
              size_t num_rows, double cardinality, double frac_null, std::vector<double> most_common_vals,
              std::vector<double> most_common_freqs, std::vector<double> histogram_bounds, bool is_base_table)
      : database_id_(database_id),
        table_id_(table_id),
        column_id_(column_id),
        num_rows_(num_rows),
        cardinality_(cardinality),
        frac_null_(frac_null),
        most_common_vals_(std::move(most_common_vals)),
        most_common_freqs_(std::move(most_common_freqs)),
        histogram_bounds_(std::move(histogram_bounds)),
        is_base_table_{is_base_table} {}

  /**
   * Default constructor for deserialization
   */
  ColumnStats() = default;

  /**
   * Gets the column oid of the column
   * @return the column oid
   */
  catalog::col_oid_t GetColumnID() const { return column_id_; }

  /**
   * Gets the number of rows in the column
   * @return the number of rows
   */
  size_t &GetNumRows() { return this->num_rows_; }

  /**
   * Sets the number of rows int he column
   * @param num_rows number of rows
   */
  void SetNumRows(size_t num_rows) { num_rows_ = num_rows; }

  /**
   * Gets the cardinality of the column
   * @return the cardinality
   */
  double &GetCardinality() { return this->cardinality_; }

  /**
   * Gets the histogram bounds
   * @return histogram bounds
   */
  const std::vector<double> &GetHistogramBounds() const { return histogram_bounds_; }

  /**
   * Gets the Common Vals
   * @return column vals
   */
  const std::vector<double> &GetCommonVals() const { return most_common_vals_; }

  /**
   * Gets the Common Freqs
   * @return common freqs
   */
  const std::vector<double> &GetCommonFreqs() const { return most_common_freqs_; }

  /**
   * Serializes a column stats object
   * @return column stats object serialized to json
   */
  nlohmann::json ToJson() const;

  /**
   * Deserializes a column stats object
   * @param j - serialized column stats object
   */
  void FromJson(const nlohmann::json &j);

 private:
  /**
   * database oid
   */
  catalog::db_oid_t database_id_;

  /**
   * table oid
   */
  catalog::table_oid_t table_id_;

  /**
   * column oid
   */
  catalog::col_oid_t column_id_;

  /**
   * number of rows in column
   */
  size_t num_rows_;

  /**
   * cardinality of column
   */
  double cardinality_;

  /**
   * fraction of null values/total values in column
   */
  double frac_null_;

  /**
   * list of most common values in column
   */
  std::vector<double> most_common_vals_;

  /**
   * list of frequencies for most common values in column
   */
  std::vector<double> most_common_freqs_;

  /**
   * bounds for the histogram of the column
   */
  std::vector<double> histogram_bounds_;

  /**
   * tells whether column is from a base table
   */
  bool is_base_table_;
};
DEFINE_JSON_HEADER_DECLARATIONS(ColumnStats);
}  // namespace noisepage::optimizer
