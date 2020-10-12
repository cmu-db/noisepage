#include <vector>

#include "type/type_id.h"

namespace terrier::runner {

/**
 * Configuration for mini-runner data configuration.
 * Stores all the parameters for generating tables.
 */
class MiniRunnersDataConfig {
  /** Distribution of table types */
  std::vector<std::vector<type::TypeId>> table_type_dists_ = {
      {type::TypeId::INTEGER, type::TypeId::DECIMAL, type::TypeId::BIGINT},
      {type::TypeId::INTEGER, type::TypeId::VARCHAR}};

  /**
   * Distribution of table colums
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
                                           2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000};
};  // namespace terrier::runner
};  // namespace terrier::runner
