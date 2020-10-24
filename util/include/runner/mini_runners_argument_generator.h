#pragma once

#include <vector>

#include "runner/mini_runners_config.h"
#include "runner/mini_runners_settings.h"

namespace noisepage::runner {

/**
 * Argument generator for mini-runners
 */
class MiniRunnersArgumentGenerator {
 public:
  /** Using declaration for output arguments */
  using OutputArgs = std::vector<std::vector<int64_t>>;

  /** Typedef for each of the Gen*Arguments functions below */
  using GenArgFn = void (*)(OutputArgs *b, const MiniRunnersSettings &settings, const MiniRunnersDataConfig &config);

  /**
   * Generates arguments for modeling arithemtics
   *
   * Benchmark arguments are as follows:
   * Arg 0: ExecutionOperatingUnitType of arithmetic
   * Arg 1: Number of operations to measure
   *
   * @param b Vector to store output argument vectors
   * @param settings Settings of the mini-runners
   * @param config MiniRunners data parameters
   */
  static void GenArithArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                const MiniRunnersDataConfig &config);

  /**
   * Generates arguments for modeling OUTPUT
   *
   * Benchmark arguments are as follows:
   * Arg 0: Number of integers to scan
   * Arg 1: Number of decimals to scan
   * Arg 2: Number of rows to output
   *
   * @param b Vector to store output argument vectors
   * @param settings Settings of the mini-runners
   * @param config MiniRunners data parameters
   */
  static void GenOutputArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                 const MiniRunnersDataConfig &config);

  /**
   * Generates arguments for modeling SEQ_SCAN
   *
   * Benchmark arguments are as follows:
   * Arg 0: Number of integers to scan
   * Arg 1: Number of decimals or varchars to scan
   * Arg 2: # integers in the underlying table
   * Arg 3: # decimals or varchars in the underlying table
   * Arg 4: Number of rows in the underlying table
   * Arg 5: Cardinality of underlying table
   * Arg 6: Whether arg 1 & 3 are for decimals or varchars
   *
   * @param b Vector to store output argument vectors
   * @param settings Settings of the mini-runners
   * @param config MiniRunners data parameters
   */
  static void GenScanArguments(OutputArgs *b, const MiniRunnersSettings &settings, const MiniRunnersDataConfig &config);

  /**
   * Generates arguments for modeling SEQ_SCAN under a mixed table distribution
   *
   * Benchmark arguments are as follows:
   * Arg 0: Number of integers to scan
   * Arg 1: Number of decimals or varchars to scan
   * Arg 2: # integers in the underlying table
   * Arg 3: # decimals or varchars in the underlying table
   * Arg 4: Number of rows in the underlying table
   * Arg 5: Cardinality of underlying table
   * Arg 6: Whether arg 1 & 3 are for decimals or varchars
   *
   * @param b Vector to store output argument vectors
   * @param settings Settings of the mini-runners
   * @param config MiniRunners data parameters
   */
  static void GenScanMixedArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                    const MiniRunnersDataConfig &config);

  /**
   * Generates arguments for modeling IDX_SCAN
   *
   * Benchmark arguments are as follows:
   * Arg 0: Type of the index key columns
   * Arg 1: Number of columns in the key
   * Arg 2: Size of the index
   * Arg 3: Size of lookup
   * Arg 4: Special argument used to indicate building an index.
   *        A value of 0 means to drop the index. A value of -1 is
   *        a dummy/sentinel value. A value of 1 means to create the
   *        index. This argument is only used when lookup_size = 0
   *
   * @param b Vector to store output argument vectors
   * @param settings Settings of the mini-runners
   * @param config MiniRunners data parameters
   */
  static void GenIdxScanArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                  const MiniRunnersDataConfig &config);

  /**
   * Generates arguments for modeling IDX_JOIN
   *
   * Benchmark arguments are as follows:
   * Arg 0: Number of columns in the key
   * Arg 1: Size of the outer table in rows
   * Arg 2: Size of the inner table in rows
   *
   * @param b Vector to store output argument vectors
   * @param settings Settings of the mini-runners
   * @param config MiniRunners data parameters
   */
  static void GenIdxJoinArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                  const MiniRunnersDataConfig &config);

  /**
   * Generates arguments for modeling SORT_BUILD / SORT_ITERATE
   *
   * Benchmark arguments are as follows:
   * Arg 0: Number of integers to scan
   * Arg 1: Number of decimals to scan
   * Arg 2: # integers in the underlying table
   * Arg 3: # decimals in the underlying table
   * Arg 4: Number of rows in the underlying table
   * Arg 5: Cardinality of underlying table
   *
   * @param b Vector to store output argument vectors
   * @param settings Settings of the mini-runners
   * @param config MiniRunners data parameters
   */
  static void GenSortArguments(OutputArgs *b, const MiniRunnersSettings &settings, const MiniRunnersDataConfig &config);

  /**
   * Generates arguments for modeling HASHJOIN_BUILD / HASHJOIN_ITERATE on self-table joins
   *
   * Benchmark arguments are as follows:
   * Arg 0: Number of integers to scan
   * Arg 1: Number of bigints to scan
   * Arg 2: # integers in the underlying table
   * Arg 3: # bigints in the underlying table
   * Arg 4: Number of rows in the underlying table
   * Arg 5: Cardinality of underlying table
   *
   * @param b Vector to store output argument vectors
   * @param settings Settings of the mini-runners
   * @param config MiniRunners data parameters
   */
  static void GenJoinSelfArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                   const MiniRunnersDataConfig &config);

  /**
   * Generates arguments for modeling HASHJOIN_BUILD / HASHJOIN_ITERATE
   *
   * Benchmark arguments are as follows:
   * Arg 0: Number of integers to scan
   * Arg 1: Number of bigints to scan
   * Arg 2: # integers in the underlying table
   * Arg 3: # bigints in the underlying table
   * Arg 4: Number of rows in the build-side table
   * Arg 5: Cardinality of build-side table
   * Arg 6: Number of rows in the probe-side table
   * Arg 7: Cardinality of probe-side table
   *
   * @param b Vector to store output argument vectors
   * @param settings Settings of the mini-runners
   * @param config MiniRunners data parameters
   */
  static void GenJoinNonSelfArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                      const MiniRunnersDataConfig &config);

  /**
   * Generates arguments for modeling AGGREGATE_BUILD / AGGREGATE_ITERATE with GROUP BY clauses
   *
   * Benchmark arguments are as follows:
   * Arg 0: Number of integers to scan
   * Arg 1: Number of bigints to scan
   * Arg 2: # integers in the underlying table
   * Arg 3: # bigints in the underlying table
   * Arg 4: Number of rows in the underlying table
   * Arg 5: Cardinality of underlying table
   *
   * @param b Vector to store output argument vectors
   * @param settings Settings of the mini-runners
   * @param config MiniRunners data parameters
   */
  static void GenAggregateArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                    const MiniRunnersDataConfig &config);

  /**
   * Generates arguments for modeling AGGREGATE_BUILD / AGGREGATE_ITERATE without GROUP BYs
   *
   * Benchmark arguments are as follows:
   * Arg 0: Number of integers to scan
   * Arg 1: Number of bigints to scan
   * Arg 2: # integers in the underlying table
   * Arg 3: # bigints in the underlying table
   * Arg 4: Number of rows in the underlying table
   * Arg 5: Cardinality of underlying table
   *
   * @param b Vector to store output argument vectors
   * @param settings Settings of the mini-runners
   * @param config MiniRunners data parameters
   */
  static void GenAggregateKeylessArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                           const MiniRunnersDataConfig &config);

  /**
   * Generates arguments for modeling INSERT
   *
   * Benchmark arguments are as follows:
   * Arg 0: Type of the tuple row to insert
   * Arg 1: Number of columns in tuple to insert
   * Arg 2: Number of rows to insert
   *
   * @param b Vector to store output argument vectors
   * @param settings Settings of the mini-runners
   * @param config MiniRunners data parameters
   */
  static void GenInsertArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                 const MiniRunnersDataConfig &config);

  /**
   * Generates arguments for modeling INSERT
   *
   * Benchmark arguments are as follows:
   * Arg 0: Type of the tuple row to insert
   * Arg 1: Number of columns in tuple to insert
   * Arg 2: Number of rows to insert
   *
   * @param b Vector to store output argument vectors
   * @param settings Settings of the mini-runners
   * @param config MiniRunners data parameters
   */
  static void GenInsertMixedArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                      const MiniRunnersDataConfig &config);

  /**
   * Generates arguments for modeling UPDATE/DELETE with index-scans
   *
   * Benchmark arguments are as follows:
   * Arg 0: Number of integers in the index key
   * Arg 1: Number of bigints in the index key
   * Arg 2: Number of integers in the underlying table
   * Arg 3: Number of bigints in the underlying table
   * Arg 4: Number of rows in the underlying table
   * Arg 5: Lookup size
   * Arg 6: Special argument used to indicate building an index.
   *        A value of 0 means to drop the index. A value of -1 is
   *        a dummy/sentinel value. A value of 1 means to create the
   *        index. This argument is only used when lookup_size = 0
   *
   * @param b Vector to store output argument vectors
   * @param settings Settings of the mini-runners
   * @param config MiniRunners data parameters
   */
  static void GenUpdateDeleteIndexArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                            const MiniRunnersDataConfig &config);

  /**
   * Generates arguments for modeling CREATE INDEX
   *
   * Benchmark arguments are as follows:
   * Arg 0: Number of integers in the index key
   * Arg 1: Number of mixed type (bigint/varchar) in the index key
   * Arg 2: Number of integers in the underlying table
   * Arg 3: Number of mixed type columns in the underlying table
   * Arg 4: Number of rows in the underlying table
   * Arg 5: Cardinality of the underlying table
   * Arg 6: Whether mixed type is a varchar or not
   * Arg 7: # threads to use (0 indicates pure-serial, 1 is 1-thread parallel build)
   *
   * @param b Vector to store output argument vectors
   * @param settings Settings of the mini-runners
   * @param config MiniRunners data parameters
   */
  static void GenCreateIndexArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                      const MiniRunnersDataConfig &config);

  /**
   * Generates arguments for modeling CREATE INDEX over mixed tables
   *
   * Benchmark arguments are as follows:
   * Arg 0: Number of integers in the index key
   * Arg 1: Number of mixed type (bigint/varchar) in the index key
   * Arg 2: Number of integers in the underlying table
   * Arg 3: Number of mixed type columns in the underlying table
   * Arg 4: Number of rows in the underlying table
   * Arg 5: Cardinality of the underlying table
   * Arg 6: Whether mixed type is a varchar or not
   * Arg 7: # threads to use (0 indicates pure-serial, 1 is 1-thread parallel build)
   *
   * @param b Vector to store output argument vectors
   * @param settings Settings of the mini-runners
   * @param config MiniRunners data parameters
   */
  static void GenCreateIndexMixedArguments(OutputArgs *b, const MiniRunnersSettings &settings,
                                           const MiniRunnersDataConfig &config);

 private:
  /**
   * Generates arguments for mixed table distributions
   *
   * Arguments generated are as follows:
   * Arg 0: # integers to use
   * Arg 1: # mixed types to use
   * Arg 2: # integers in underlying table
   * Arg 3: # mixed types in underlying table
   * Arg 4: Number of rows of underlying table
   * Arg 5: Cardinality of underlying table
   * Arg 6: Whether mixed type is a varchar
   *
   * @param args Vector to insert argument vectors
   * @param settings Settings of the mini-runners
   * @param config MiniRunners data parameters
   * @param row_nums Row numbers ot generate column distributions for
   * @param varchar_mix Whether mixed type is a varchar
   */
  static void GenerateMixedArguments(std::vector<std::vector<int64_t>> *args, const MiniRunnersSettings &settings,
                                     const MiniRunnersDataConfig &config, const std::vector<uint32_t> &row_nums,
                                     uint32_t varchar_mix);
};

};  // namespace noisepage::runner
