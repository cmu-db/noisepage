#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/json_header.h"
#include "common/macros.h"
#include "execution/sql/value.h"
#include "optimizer/statistics/histogram.h"
#include "optimizer/statistics/top_k_elements.h"

namespace noisepage::optimizer {

/**
 * Represents the statistics of a given column. Stores relevant oids,
 * top K elements which uses the count min sketch algorithm to estimate the cardinality
 * for filters, and other useful information. The template denotes the type of the column
 * for which statistics are being stored.
 */
template <typename T>
class NewColumnStats {
  // Type used to represent the SQL data type in C++.
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
   * @param k_value - Number of elements to keep track of in top K
   * @param top_k_width - width of the count min sketch used in top K elements
   * @param histogram_max_bins - The maximum number of bins in the histogram.
   * @param is_base_table - indicates whether the column is from a base table
   */
  NewColumnStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id, catalog::col_oid_t column_id,
                 size_t num_rows, double cardinality, double frac_null, size_t k_value, uint64_t top_k_width,
                 uint8_t histogram_max_bins, bool is_base_table)
      : database_id_(database_id),
        table_id_(table_id),
        column_id_(column_id),
        num_rows_(num_rows),
        cardinality_(cardinality),
        frac_null_(frac_null) {
    top_k_ptr_ = std::make_unique<TopKElements<CppType>>(k_value, top_k_width);
    histogram_ptr_ = std::make_unique<Histogram<CppType>>(histogram_max_bins);
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
   * Gets the fraction of null values in the table.
   * @return Fraction of nulls
   */
  double &GetFracNull() { return this->frac_null_; }

  /**
   * Gets the cardinality of the column
   * @return the cardinality
   */
  double &GetCardinality() { return this->cardinality_; }

  /**
   * Gets the pointer to the histogram for the column.
   * @return Pointer to histogram.
   */
  common::ManagedPointer<Histogram<CppType>> GetHistogram() const { return common::ManagedPointer(histogram_ptr_); }

  /**
   * Gets the Top-K pointer with information on top k values and their frequencies.
   * @return pointer to top k object
   */
  common::ManagedPointer<TopKElements<CppType>> GetTopK() { return common::ManagedPointer(top_k_ptr_); }

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
  std::unique_ptr<TopKElements<CppType>> top_k_ptr_;

  /**
   * Histogram for the column values.
   */
  std::unique_ptr<Histogram<CppType>> histogram_ptr_;
};
}  // namespace noisepage::optimizer
