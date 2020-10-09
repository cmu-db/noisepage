#pragma once

#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/json_header.h"
#include "common/macros.h"
#include "execution/sql/value.h"
#include "optimizer/statistics/top_k_elements.h"

namespace terrier::optimizer {

/**
 * Represents the statistics of a given column. Stores relevant oids,
 * important trends (most common values/their frequencies in the column), and other
 * useful information. The template denotes the type of the column for which
 * statistics are being stored.
 */
template <typename T>
class NewColumnStats {
  using CppType = decltype(T::val_);

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
  NewColumnStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id, catalog::col_oid_t column_id,
                 size_t num_rows, double cardinality, double frac_null, size_t k_value, uint64_t top_k_width,
                 std::vector<CppType> histogram_bounds, bool is_base_table)
      : database_id_(database_id),
        table_id_(table_id),
        column_id_(column_id),
        num_rows_(num_rows),
        cardinality_(cardinality),
        frac_null_(frac_null),
        histogram_bounds_(std::move(histogram_bounds)),
        is_base_table_{is_base_table} {
    top_k_ptr = new TopKElements<CppType>(k_value, top_k_width);
  }

  /**
   * Default constructor for deserialization
   */
  NewColumnStats() = default;

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
  const std::vector<CppType> &GetHistogramBounds() const { return histogram_bounds_; }

  /**
   * Gets the Top-K object with information on top k values and their frequencies.
   * @return top k object
   */
  common::ManagedPointer<TopKElements<CppType>> GetTopK() { return top_k_ptr; }

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
   * Top-K elements based on frequency.
   */
  common::ManagedPointer<TopKElements<CppType>> top_k_ptr;

  /**
   * bounds for the histogram of the column
   */
  std::vector<CppType> histogram_bounds_;

  /**
   * tells whether column is from a base table
   */
  bool is_base_table_;
};
}  // namespace terrier::optimizer
