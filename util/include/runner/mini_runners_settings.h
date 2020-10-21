#pragma once

#include <cstdint>

namespace terrier::runner {

/**
 * Class that holds mini-runner settings
 */
class MiniRunnersSettings {
 public:
  /**
   * Port
   */
  uint16_t port_ = 15721;

  /**
   * Number of warmup iterations
   */
  int64_t warmup_iterations_num_ = 5;

  /**
   * Number of rerun iterations
   */
  int64_t rerun_iterations_ = 10;

  /**
   * Limit on the max number of rows / tuples processed
   */
  int64_t data_rows_limit_ = 1000000;

  /**
   * Limit on num_rows for which queries need warming up
   */
  int64_t warmup_rows_limit_ = 1000;

  /**
   * warmup_rows_limit controls which queries need warming up.
   * skip_large_rows_runs is used for controlling whether or not
   * to run queries with large rows (> warmup_rows_limit)
   */
  bool skip_large_rows_runs_ = false;

  /**
   * Update/Delete Index Scan Limit
   */
  int64_t updel_limit_ = 1000;

  /**
   * CREATE INDEX small build limit
   */
  int64_t create_index_small_limit_ = 10000;

  /**
   * Number of cardinalities to vary for CREATE INDEX large builds.
   */
  int64_t create_index_large_cardinality_num_ = 3;

  /**
   * Whether to run a targeted filter
   */
  bool target_runner_specified_ = false;

  /**
   * Whether to generate test data
   */
  bool generate_test_data_ = false;

  /**
   * Initialize all the above settings from the arguments
   * @param argc Number of arguments
   * @param argv Arguments
   */
  void InitializeFromArguments(int argc, char **argv);
};

};  // namespace terrier::runner
