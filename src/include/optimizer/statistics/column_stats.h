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
 * Base class for Column Statistics
 */
class ColumnStatsBase {
 public:
  /**
   * Destructor
   */
  virtual ~ColumnStatsBase() = default;
  /**
   * Gets the column oid of the column
   * @return the column oid
   */
  virtual catalog::col_oid_t GetColumnID() const = 0;
  /**
   * Gets the number of rows in the column
   * @return the number of rows
   */
  virtual size_t GetNumRows() = 0;
  /**
   * Sets the number of rows int he column
   * @param num_rows number of rows
   */
  virtual void SetNumRows(size_t num_rows) = 0;
  /**
   * Gets the number of rows that are not null
   * @return number of non null rows
   */
  virtual size_t GetNonNullRows() = 0;
  /**
   * Gets the number of rows that are null
   * @return number of null rows
   */
  virtual size_t GetNullRows() = 0;
  /**
   * Gets the fraction of null values in the column.
   * @return Fraction of nulls
   */
  virtual double GetFracNull() = 0;
  /**
   * Gets the number of distinct values in the column
   * @return distinct values
   */
  virtual size_t GetDistinctValues() = 0;
  /**
   * Gets the type id of the column
   * @return type id of the column
   */
  virtual type::TypeId GetTypeId() = 0;
  /**
   * Returns whether or not this stat is stale
   * @return true if stale false otherwise
   */
  virtual bool IsStale() = 0;
  /**
   * Marks this column stat as stale
   */
  virtual void MarkStale() = 0;
};

/**
 * Represents the statistics of a given column. Stores relevant oids,
 * top K elements which uses the count min sketch algorithm to estimate the cardinality
 * for filters, and other useful information. The template denotes the type of the column
 * for which statistics are being stored.
 */
template <typename T>
class ColumnStats : public ColumnStatsBase {
  // Type used to represent the SQL data type in C++.
  using CppType = decltype(T::val_);

 public:
  /**
   * Constructor
   * @param database_id - database oid of column
   * @param table_id - table oid of column
   * @param column_id - column oid of column
   * @param num_rows - number of rows in column
   * @param non_null_rows - number of non null rows in column
   * @param distinct_values - number of distinct values in column
   * @param top_k - TopKElements for this column
   * @param histogram - Histogram for this column
   * @param type_id - type of column
   */
  ColumnStats(catalog::db_oid_t database_id, catalog::table_oid_t table_id, catalog::col_oid_t column_id,
              size_t num_rows, size_t non_null_rows, size_t distinct_values,
              std::unique_ptr<TopKElements<CppType>> top_k, std::unique_ptr<Histogram<CppType>> histogram,
              type::TypeId type_id)
      : database_id_(database_id),
        table_id_(table_id),
        column_id_(column_id),
        num_rows_(num_rows),
        non_null_rows_(non_null_rows),
        distinct_values_(distinct_values),
        top_k_(std::move(top_k)),
        histogram_(std::move(histogram)),
        type_id_(type_id),
        stale_(false) {
    frac_null_ = num_rows == 0 ? 0 : static_cast<double>(num_rows - non_null_rows) / static_cast<double>(num_rows);
  }

  /**
   * Default constructor for deserialization
   */
  ColumnStats() = default;

  /**
   * Copy constructor
   * @param other ColumnStats to copy
   */
  ColumnStats(const ColumnStats &other)
      : database_id_(other.database_id_),
        table_id_(other.table_id_),
        column_id_(other.column_id_),
        num_rows_(other.num_rows_),
        non_null_rows_(other.non_null_rows_),
        frac_null_(other.frac_null_),
        distinct_values_(other.distinct_values_),
        type_id_(other.type_id_),
        stale_(other.stale_) {
    top_k_ = std::make_unique<TopKElements<CppType>>(*other.top_k_);
    histogram_ = std::make_unique<Histogram<CppType>>(*other.histogram_);
  }

  /**
   * Move constructor
   * @param other ColumnStats to move
   */
  ColumnStats(ColumnStats &&other) noexcept = default;

  /**
   * Default destructor
   */
  ~ColumnStats() override = default;

  /**
   * Copy assignment operator
   * @param other ColumnStats to copy
   * @return this after copying
   */
  ColumnStats &operator=(const ColumnStats &other) {
    database_id_ = other.database_id_;
    table_id_ = other.table_id_;
    column_id_ = other.column_id_;
    num_rows_ = other.num_rows_;
    non_null_rows_ = other.non_null_rows_;
    frac_null_ = other.frac_null_;
    distinct_values_ = other.distinct_values_;
    top_k_ = std::make_unique<TopKElements<CppType>>(*other.top_k_);
    histogram_ = std::make_unique<Histogram<CppType>>(*other.histogram_);
    type_id_ = other.type_id_;
    stale_ = other.stale_;
    return *this;
  }

  /**
   * Move assignment operator
   * @param other ColumnStats to copy
   * @return this after moving
   */
  ColumnStats &operator=(ColumnStats &&other) noexcept = default;

  /**
   * Gets the column oid of the column
   * @return the column oid
   */
  catalog::col_oid_t GetColumnID() const override { return column_id_; }

  /**
   * Sets the number of rows int he column
   * @param num_rows number of rows
   */
  void SetNumRows(size_t num_rows) override { num_rows_ = num_rows; }

  /**
   * Gets the number of rows in the column
   * @return the number of rows
   */
  size_t GetNumRows() override { return num_rows_; }

  /**
   * Gets the number of rows that are not null
   * @return number of non null rows
   */
  size_t GetNonNullRows() override { return non_null_rows_; }

  /**
   * Gets the number of rows that are null
   * @return number of null rows
   */
  size_t GetNullRows() override { return num_rows_ - non_null_rows_; }

  /**
   * Gets the fraction of null values in the column.
   * @return Fraction of nulls
   */
  double GetFracNull() override { return frac_null_; }

  /**
   * Gets the number of distinct values in the column
   * @return distinct values
   */
  size_t GetDistinctValues() override { return distinct_values_; }

  /**
   * Gets the pointer to the histogram for the column.
   * @return Pointer to histogram.
   */
  common::ManagedPointer<Histogram<CppType>> GetHistogram() const { return common::ManagedPointer(histogram_); }

  /**
   * Gets the Top-K pointer with information on top k values and their frequencies.
   * @return pointer to top k object
   */
  common::ManagedPointer<TopKElements<CppType>> GetTopK() { return common::ManagedPointer(top_k_); }

  /**
   * Gets the type id of the column
   * @return type id of the column
   */
  type::TypeId GetTypeId() override { return type_id_; }

  /**
   * Returns whether or not this stat is stale
   * @return true if stale false otherwise
   */
  bool IsStale() override { return stale_; }

  /**
   * Marks this column stat as stale
   */
  void MarkStale() override { stale_ = true; }

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
   * number of non null rows
   */
  size_t non_null_rows_;

  /**
   * fraction of null values/total values in column
   */
  double frac_null_;

  /**
   * number of distinct values in column
   */
  size_t distinct_values_;

  /**
   * Top-K elements based on frequency.
   */
  std::unique_ptr<TopKElements<CppType>> top_k_;

  /**
   * Histogram for the column values.
   */
  std::unique_ptr<Histogram<CppType>> histogram_;

  /**
   * Type Id of underlying column.
   */
  type::TypeId type_id_;

  /**
   * Whether these statistics are stale, i.e. pg_statistic has been updated for this column with newer statistics
   */
  bool stale_;
};
}  // namespace noisepage::optimizer
