#pragma once

#include <utility>
#include <vector>

#include "type/type_id.h"

namespace terrier::runner {

/**
 * Configuration for mini-runner generated data.
 * Stores all the parameters for generating tables.
 */
class MiniRunnersDataConfig {
 public:
  /** Distribution of table column types */
  std::vector<std::vector<type::TypeId>> table_type_dists_ = {
      {type::TypeId::INTEGER, type::TypeId::DECIMAL, type::TypeId::BIGINT},
      {type::TypeId::INTEGER, type::TypeId::VARCHAR}};

  /**
   * Distribution of table columns
   *
   * Describes a set of table column distributions to be used when creating
   * data for the mini-runners. The explanation for this is best illustrated
   * with an example.
   *
   * Consider table_col_dists_ = {{{1, 2, 3},...},...}.
   * Now note that y = table_col_dists_[i=0][j=0] = {1, 2, 3}
   *
   * This means that a table [t] created from [y] is comprised of three column
   * types (integer, decimal, and bigint) based on table_type_dists_[i=0].
   * Furthermore, the number of columns in table [t] can be obtained by summing
   * up all the numbers in [y] which is 6 based on the fact that [t] has
   * 1 INTEGER, 2 DECIMALS, and 3 BIGINTS (y[k] is the number of columns of
   * type table_type_dists_[i=0][k]).
   */
  std::vector<std::vector<std::vector<uint32_t>>> table_col_dists_ = {
      {{0, 15, 0}, {3, 12, 0}, {7, 8, 0}, {11, 4, 0}, {15, 0, 0}, {0, 0, 15}},
      {{0, 5}, {1, 4}, {2, 3}, {3, 2}, {4, 1}}};

  /**
   * Distribution of row numbers of tables to create.
   *
   * Note that for each row number, we create multiple tables, varying the
   * cardinality in powers of 2. For instance, when creating a table of
   * 100 tuples, we create tables of 100 tuples with cardinality 1, 2, 4,
   * 8, 16, 32, 64, and 100.
   */
  std::vector<uint32_t> table_row_nums_ = {1,    3,    5,     7,     10,    50,     100,    200,    500,    1000,
                                           2000, 5000, 10000, 20000, 50000, 100000, 200000, 300000, 500000, 1000000};

  /**
   * Types of pure tables to create for index builds.
   * A "pure" table is a table where all columns are of the same type.
   *
   * The vector stores a pair denoting <number of columns, type>
   */
  std::vector<std::pair<uint32_t, type::TypeId>> index_table_types_ = {
      {15, type::TypeId::INTEGER}, {15, type::TypeId::BIGINT}, {5, type::TypeId::VARCHAR}};

  /**
   * Parameter controls number of columns extracted from base tables.
   */
  std::vector<uint32_t> sweep_col_nums_ = {1, 3, 5, 7, 9, 11, 13, 15};

  /**
   * Parameter controls distribution of mixed (integer, decimal/bigint) for scans
   */
  std::vector<std::pair<uint32_t, uint32_t>> sweep_scan_mixed_dist_ = {{3, 12}, {7, 8}, {11, 4}};

  /**
   * Parameter controls distribution of mixed (integer, varchar) for scans
   */
  std::vector<std::pair<uint32_t, uint32_t>> sweep_scan_mixed_varchar_dist_ = {{2, 3}, {3, 2}, {4, 1}};

  /**
   * Parameter controls number of keys to be used in mini-runner index lookups.
   */
  std::vector<uint32_t> sweep_index_col_nums_ = {1, 2, 4, 8, 15};

  /**
   * Parameter controls size of index scan lookups.
   */
  std::vector<uint32_t> sweep_index_lookup_sizes_ = {1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100};

  /*
   * Parameter controls # threads to sweep for building index.
   * 0 is a special argument to indicate serial build.
   */
  std::vector<uint32_t> sweep_index_create_threads_ = {0, 1, 2, 4, 8, 16};

  /**
   * Parameter controls number of insert tuples
   */
  std::vector<uint32_t> sweep_insert_row_nums_ = {1, 10, 100, 200, 500, 1000, 2000, 5000, 10000};

  /**
   * Parameter controls distribution of mixed (integer, decimal) tuples.
   */
  std::vector<std::pair<uint32_t, uint32_t>> sweep_insert_mixed_dist_ = {{1, 14}, {3, 12}, {5, 10}, {7, 8},
                                                                         {9, 6},  {11, 4}, {13, 2}};
};

};  // namespace terrier::runner
