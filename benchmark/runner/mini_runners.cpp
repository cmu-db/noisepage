#include <gflags/gflags.h>

#include <cstdio>
#include <functional>
#include <pqxx/pqxx>  // NOLINT
#include <random>
#include <utility>

#include "benchmark/benchmark.h"
#include "benchmark_util/benchmark_config.h"
#include "binder/bind_node_visitor.h"
#include "brain/brain_defs.h"
#include "brain/operating_unit.h"
#include "common/macros.h"
#include "common/scoped_timer.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/executable_query.h"
#include "execution/exec/execution_settings.h"
#include "execution/execution_util.h"
#include "execution/sql/ddl_executors.h"
#include "execution/table_generator/table_generator.h"
#include "execution/util/cpu_info.h"
#include "execution/vm/module.h"
#include "loggers/loggers_util.h"
#include "main/db_main.h"
#include "optimizer/cost_model/forced_cost_model.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/optimizer.h"
#include "optimizer/properties.h"
#include "optimizer/query_to_operator_transformer.h"
#include "planner/plannodes/index_join_plan_node.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "storage/sql_table.h"
#include "traffic_cop/traffic_cop_util.h"

namespace terrier::runner {

/**
 * Should start small-row only scans
 */
bool rerun_start = false;

/**
 * Number of rerun iterations
 */
int64_t rerun_iterations = 10;

/**
 * Update/Delete Index Scan Limit
 */
int64_t updel_limit = 1000;

/**
 * Port
 */
uint16_t port = 15721;

/**
 * Static db_main instance
 * This is done so all tests reuse the same DB Main instance
 */
DBMain *db_main = nullptr;

/**
 * Database OID
 * This is done so all tests can use the same database OID
 */
catalog::db_oid_t db_oid{0};

/**
 * Number of warmup iterations
 */
int64_t warmup_iterations_num{5};

/**
 * warmup_rows_limit controls which queries need warming up.
 * skip_large_rows_runs is used for controlling whether or not
 * to run queries with large rows (> warmup_rows_limit)
 */
bool skip_large_rows_runs = false;

/**
 * Limit on num_rows for which queries need warming up
 */
int64_t warmup_rows_limit{1000};

/**
 * CREATE INDEX small build limit
 */
int64_t create_index_small_limit{10000};

/**
 * Number of cardinalities to vary for CREATE INDEX large builds.
 */
int64_t create_index_large_cardinality_num{3};

/**
 * Empty global param vector
 */
std::vector<std::vector<parser::ConstantValueExpression>> empty_params = {};

void InvokeGC() {
  // Perform GC to do any cleanup
  auto gc = terrier::runner::db_main->GetStorageLayer()->GetGarbageCollector();
  gc->PerformGarbageCollection();
  gc->PerformGarbageCollection();
}

/**
 * Arg <0, 1, 2, 3, 4, 5>
 * 0 - # integers to scan
 * 1 - # decimals to scan
 * 2 - integers of table
 * 3 - decimals of table
 * 4 - Number of rows
 * 5 - Cardinality
 */
#define GENERATE_MIXED_ARGUMENTS(args, noop)                                                        \
  {                                                                                                 \
    /* Vector of table distributions <INTEGER, DECIMALS> */                                         \
    std::vector<std::pair<uint32_t, uint32_t>> mixed_dist = {{3, 12}, {7, 8}, {11, 4}};             \
    /* Always generate full table scans for all row_num and cardinalities. */                       \
    for (auto col_dist : mixed_dist) {                                                              \
      std::pair<uint32_t, uint32_t> start = {col_dist.first - 2, 2};                                \
      while (true) {                                                                                \
        for (auto row : row_nums) {                                                                 \
          int64_t car = 1;                                                                          \
          while (car < row) {                                                                       \
            args.push_back({start.first, start.second, col_dist.first, col_dist.second, row, car}); \
            car *= 2;                                                                               \
          }                                                                                         \
          args.push_back({start.first, start.second, col_dist.first, col_dist.second, row, row});   \
        }                                                                                           \
        if (start.second < col_dist.second) {                                                       \
          start.second += 2;                                                                        \
        } else if (start.first < col_dist.first) {                                                  \
          if (noop) args.push_back({0, 0, 0, 0, 0, 0});                                             \
          start.first += 2;                                                                         \
          start.second = 2;                                                                         \
        } else {                                                                                    \
          break;                                                                                    \
        }                                                                                           \
      }                                                                                             \
    }                                                                                               \
  }

/**
 * Taken from Facebook's folly library.
 * DoNotOptimizeAway helps to ensure that a certain variable
 * is not optimized away by the compiler.
 */
template <typename T>
struct DoNotOptimizeAwayNeedsIndirect {
  using Decayed = typename std::decay<T>::type;

  // First two constraints ensure it can be an "r" operand.
  // std::is_pointer check is because callers seem to expect that
  // doNotOptimizeAway(&x) is equivalent to doNotOptimizeAway(x).
  constexpr static bool VALUE = !std::is_trivially_copyable<Decayed>::value ||
                                sizeof(Decayed) > sizeof(int64_t) || std::is_pointer<Decayed>::value;
};

template <typename T>
auto DoNotOptimizeAway(const T &datum) -> typename std::enable_if<!DoNotOptimizeAwayNeedsIndirect<T>::VALUE>::type {
  // The "r" constraint forces the compiler to make datum available
  // in a register to the asm block, which means that it must have
  // computed/loaded it.  We use this path for things that are <=
  // sizeof(long) (they have to fit), trivial (otherwise the compiler
  // doesn't want to put them in a register), and not a pointer (because
  // DoNotOptimizeAway(&foo) would otherwise be a foot gun that didn't
  // necessarily compute foo).
  //
  // An earlier version of this method had a more permissive input operand
  // constraint, but that caused unnecessary variation between clang and
  // gcc benchmarks.
  asm volatile("" ::"r"(datum));
}

/**
 * To prevent the iterator op i from being optimized out on
 * clang/gcc compilers, gcc requires noinline and noclone
 * to prevent any inlining. noclone is used to prevent gcc
 * from doing interprocedural constant propagation.
 *
 * DoNotOptimizeAway is placed inside the for () loop to
 * ensure that the compiler does not blindly optimize away
 * the for loop.
 *
 * TIGHT_LOOP_OPERATION(uint32_t, PLUS, +)
 * Defines a function called uint32_t_PLUS that adds integers.
 */
#ifdef __clang__
#define TIGHT_LOOP_OPERATION(type, name, op)                       \
  __attribute__((noinline)) type __##type##_##name(size_t count) { \
    type iterator = 1;                                             \
    for (size_t i = 1; i <= count; i++) {                          \
      iterator = iterator op i;                                    \
      DoNotOptimizeAway(iterator);                                 \
    }                                                              \
                                                                   \
    return iterator;                                               \
  }

#elif __GNUC__
#define TIGHT_LOOP_OPERATION(type, name, op)                                \
  __attribute__((noinline, noclone)) type __##type##_##name(size_t count) { \
    type iterator = 1;                                                      \
    for (size_t i = 1; i <= count; i++) {                                   \
      iterator = iterator op i;                                             \
      DoNotOptimizeAway(iterator);                                          \
    }                                                                       \
                                                                            \
    return iterator;                                                        \
  }

#endif

/**
 * Arg: <0, 1>
 * 0 - ExecutionOperatingType
 * 1 - Iteration count
 */
static void GenArithArguments(benchmark::internal::Benchmark *b) {
  auto operators = {brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS,
                    brain::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY,
                    brain::ExecutionOperatingUnitType::OP_INTEGER_DIVIDE,
                    brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
                    brain::ExecutionOperatingUnitType::OP_DECIMAL_PLUS_OR_MINUS,
                    brain::ExecutionOperatingUnitType::OP_DECIMAL_MULTIPLY,
                    brain::ExecutionOperatingUnitType::OP_DECIMAL_DIVIDE,
                    brain::ExecutionOperatingUnitType::OP_DECIMAL_COMPARE};

  std::vector<size_t> counts;
  for (size_t i = 10000; i < 100000; i += 10000) counts.push_back(i);
  for (size_t i = 100000; i < 1000000; i += 100000) counts.push_back(i);

  for (auto op : operators) {
    for (auto count : counts) {
      b->Args({static_cast<int64_t>(op), static_cast<int64_t>(count)});
    }
  }
}

/**
 * Arg <0, 1, 2>
 * 0 - # integers to scan
 * 1 - # decimals to scan
 * 2 - row
 */
static void GenOutputArguments(benchmark::internal::Benchmark *b) {
  auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
  auto types = {type::TypeId::INTEGER, type::TypeId::DECIMAL};
  std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    500,    1000,
                                   2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000};

  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : row_nums) {
        if (type == type::TypeId::INTEGER)
          b->Args({col, 0, row});
        else if (type == type::TypeId::DECIMAL)
          b->Args({0, col, row});
      }
    }
  }

  // Generate special Output feature [1 0 0 1 1]
  b->Args({0, 0, 1});
}

/**
 * Arg <0, 1, 2, 3, 4, 5>
 * 0 - # integers to scan
 * 1 - # decimals to scan
 * 2 - # integers in table
 * 3 - # decimals in table
 * 4 - row
 * 5 - cardinality
 */
static void GenScanArguments(benchmark::internal::Benchmark *b) {
  auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
  auto types = {type::TypeId::INTEGER, type::TypeId::DECIMAL};
  std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    200,    500,    1000,
                                   2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000};
  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : row_nums) {
        int64_t car = 1;
        while (car < row) {
          if (type == type::TypeId::INTEGER)
            b->Args({col, 0, 15, 0, row, car});
          else if (type == type::TypeId::DECIMAL)
            b->Args({0, col, 0, 15, row, car});
          car *= 2;
        }

        if (type == type::TypeId::INTEGER)
          b->Args({col, 0, 15, 0, row, row});
        else if (type == type::TypeId::DECIMAL)
          b->Args({0, col, 0, 15, row, row});
      }
    }
  }
}

static void GenScanMixedArguments(benchmark::internal::Benchmark *b) {
  std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    200,    500,    1000,
                                   2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000};
  std::vector<std::vector<int64_t>> args;
  GENERATE_MIXED_ARGUMENTS(args, false);
  for (const auto &arg : args) {
    b->Args(arg);
  }
}

/**
 * Arg <0, 1, 2, 3, 4, 5>
 * 0 - # integers to scan
 * 1 - # decimals to scan
 * 2 - # integers in table
 * 3 - # decimals in table
 * 4 - row
 * 5 - cardinality
 */
static void GenSortArguments(benchmark::internal::Benchmark *b) {
  auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
  auto types = {type::TypeId::INTEGER};
  std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    200,    500,    1000,
                                   2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000};
  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : row_nums) {
        int64_t car = 1;
        while (car < row) {
          if (type == type::TypeId::INTEGER)
            b->Args({col, 0, 15, 0, row, car});
          else if (type == type::TypeId::DECIMAL)
            b->Args({0, col, 0, 15, row, car});
          car *= 2;
        }

        if (type == type::TypeId::INTEGER)
          b->Args({col, 0, 15, 0, row, row});
        else if (type == type::TypeId::DECIMAL)
          b->Args({0, col, 0, 15, row, row});
      }
    }
  }
}

/**
 * Arg <0, 1, 2, 3, 4, 5>
 * 0 - # integers to scan
 * 1 - # decimals to scan
 * 2 - # integers in table
 * 3 - # decimals in table
 * 4 - row
 * 5 - cardinality
 */
static void GenAggregateArguments(benchmark::internal::Benchmark *b) {
  auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
  auto types = {type::TypeId::INTEGER};
  std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    200,    500,    1000,
                                   2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000};
  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : row_nums) {
        int64_t car = 1;
        while (car < row) {
          if (type == type::TypeId::INTEGER)
            b->Args({col, 0, 15, 0, row, car});
          else if (type == type::TypeId::BIGINT)
            b->Args({0, col, 0, 15, row, car});
          car *= 2;
        }

        if (type == type::TypeId::INTEGER)
          b->Args({col, 0, 15, 0, row, row});
        else if (type == type::TypeId::BIGINT)
          b->Args({0, col, 0, 15, row, row});
      }
    }
  }
}

/**
 * Arg <0, 1, 2, 3, 4, 5>
 * 0 - # integers to scan
 * 1 - # integers in table
 * 3 - row
 * 4 - cardinality
 */
static void GenAggregateKeylessArguments(benchmark::internal::Benchmark *b) {
  auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
  std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    200,    500,    1000,
                                   2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000};
  for (auto col : num_cols) {
    for (auto row : row_nums) {
      int64_t car = 1;
      while (car < row) {
        b->Args({col, 15, row, car});
        car *= 2;
      }

      b->Args({col, 15, row, row});
    }
  }
}

/**
 * Arg <0, 1, 2, 3, 4, 5>
 * 0 - # integers to scan
 * 1 - # decimals to scan
 * 2 - integers of table
 * 3 - decimals of table
 * 4 - Number of rows
 * 5 - Cardinality
 */
static void GenJoinSelfArguments(benchmark::internal::Benchmark *b) {
  auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
  auto types = {type::TypeId::INTEGER};
  std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    200,    500,    1000,
                                   2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000};
  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : row_nums) {
        int64_t car = 1;
        std::vector<int64_t> cars;
        while (car < row) {
          if (row * row / car <= 10000000) {
            if (type == type::TypeId::INTEGER)
              b->Args({col, 0, 15, 0, row, car});
            else if (type == type::TypeId::BIGINT)
              b->Args({0, col, 0, 15, row, car});
          }

          car *= 2;
        }

        if (type == type::TypeId::INTEGER)
          b->Args({col, 0, 15, 0, row, row});
        else if (type == type::TypeId::BIGINT)
          b->Args({0, col, 0, 15, row, row});
      }
    }
  }
}

/**
 * Arg <0, 1, 2, 3, 4, 5, 6, 7, 8>
 * 0 - # integers to scan
 * 1 - # decimals ot scan
 * 2 - # integers in table
 * 3 - # decimals in table
 * 4 - Build # rows
 * 5 - Build Cardinality
 * 6 - Probe # rows
 * 7 - Probe Cardinality
 * 8 - Matched cardinality
 */
static void GenJoinNonSelfArguments(benchmark::internal::Benchmark *b) {
  auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
  auto types = {type::TypeId::INTEGER};
  std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    200,    500,    1000,
                                   2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000};
  for (auto type : types) {
    for (auto col : num_cols) {
      for (size_t i = 0; i < row_nums.size(); i++) {
        auto build_rows = row_nums[i];
        auto build_car = row_nums[i];
        for (size_t j = i + 1; j < row_nums.size(); j++) {
          auto probe_rows = row_nums[j];
          auto probe_car = row_nums[j];

          auto matched_car = row_nums[i];
          if (type == type::TypeId::INTEGER)
            b->Args({col, 0, 15, 0, build_rows, build_car, probe_rows, probe_car, matched_car});
          else if (type == type::TypeId::BIGINT)
            b->Args({0, col, 0, 15, build_rows, build_car, probe_rows, probe_car, matched_car});
        }
      }
    }
  }
}

/**
 * Arg: <0, 1, 2, 3, 4>
 * 0 - Type
 * 1 - Key Size
 * 2 - Index Size
 * 3 - Lookup size
 * 4 - Special argument used to indicate building an index.
 *     A value of 0 means to drop the index. A value of -1 is
 *     a dummy/sentinel value. A value of 1 means to create the
 *     index. This argument is only used when lookup_size = 0
 */
static void GenIdxScanArguments(benchmark::internal::Benchmark *b) {
  auto types = {type::TypeId::INTEGER, type::TypeId::BIGINT};
  auto key_sizes = {1, 2, 4, 8, 15};
  auto idx_sizes = {1, 10, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 300000, 500000, 1000000};
  std::vector<int64_t> lookup_sizes = {1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
  for (auto type : types) {
    for (auto key_size : key_sizes) {
      for (auto idx_size : idx_sizes) {
        b->Args({static_cast<int64_t>(type), key_size, idx_size, 0, 1});

        for (auto lookup_size : lookup_sizes) {
          if (lookup_size <= idx_size) {
            b->Args({static_cast<int64_t>(type), key_size, idx_size, lookup_size, -1});
          }
        }

        b->Args({static_cast<int64_t>(type), key_size, idx_size, 0, 0});
      }
    }
  }
}

/**
 * Arg: <0, 1, 2>
 * 0 - Col
 * 1 - Outer Table
 * 2 - Inner Table
 */
static void GenIdxJoinArguments(benchmark::internal::Benchmark *b) {
  auto key_sizes = {1, 2, 4, 8, 15};
  std::vector<int64_t> idx_sizes = {1,     10,    100,   200,    500,    1000,   2000,   5000,
                                    10000, 20000, 50000, 100000, 300000, 500000, 1000000};
  for (auto key_size : key_sizes) {
    for (size_t j = 0; j < idx_sizes.size(); j++) {
      // Build the inner index
      b->Args({key_size, 0, idx_sizes[j], 1});

      for (size_t i = 0; i < idx_sizes.size(); i++) {
        b->Args({key_size, idx_sizes[i], idx_sizes[j], -1});
      }

      // Drop the inner index
      b->Args({key_size, 0, idx_sizes[j], 0});
    }
  }
}

static void GenIdxScanParameters(type::TypeId type_param, int64_t num_rows, int64_t lookup_size, int64_t num_iters,
                                 std::vector<std::vector<parser::ConstantValueExpression>> *real_params) {
  std::mt19937 generator{};
  std::vector<std::pair<uint32_t, uint32_t>> bounds;
  for (int i = 0; i < num_iters; i++) {
    // Pick a range [0, 10). The span is num_rows - lookup_size / 10 which controls
    // the span of numbers within a given range.
    int num_regions = 10;
    int64_t span = (num_rows - lookup_size) / num_regions;
    auto range =
        std::uniform_int_distribution(static_cast<uint32_t>(0), static_cast<uint32_t>(num_regions - 1))(generator);
    auto low_key = std::uniform_int_distribution(static_cast<uint32_t>(0),
                                                 static_cast<uint32_t>((span >= 1) ? (span - 1) : 0))(generator);
    low_key += range * span;

    std::vector<parser::ConstantValueExpression> param;
    if (lookup_size == 1) {
      param.emplace_back(type_param, execution::sql::Integer(low_key));
      bounds.emplace_back(low_key, low_key);
    } else {
      auto high_key = low_key + lookup_size - 1;
      param.emplace_back(type_param, execution::sql::Integer(low_key));
      param.emplace_back(type_param, execution::sql::Integer(high_key));
      bounds.emplace_back(low_key, high_key);
    }

    real_params->emplace_back(std::move(param));
  }
}

/**
 * Arg <0, 1, 2>
 * 0 - Type
 * 0 - Number of columns
 * 1 - Number of rows
 */
static void GenInsertArguments(benchmark::internal::Benchmark *b) {
  auto types = {type::TypeId::INTEGER, type::TypeId::DECIMAL};
  auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
  auto num_rows = {1, 10, 100, 200, 500, 1000, 2000, 5000, 10000};
  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : num_rows) {
        if (type == type::TypeId::INTEGER)
          b->Args({col, 0, col, row});
        else if (type == type::TypeId::DECIMAL)
          b->Args({0, col, col, row});
      }
    }
  }
}

static void GenInsertMixedArguments(benchmark::internal::Benchmark *b) {
  std::vector<std::pair<uint32_t, uint32_t>> mixed_dist = {{1, 14}, {3, 12}, {5, 10}, {7, 8}, {9, 6}, {11, 4}, {13, 2}};

  auto num_rows = {1, 10, 100, 200, 500, 1000, 2000, 5000, 10000};
  for (auto mixed : mixed_dist) {
    for (auto row : num_rows) {
      b->Args({mixed.first, mixed.second, mixed.first + mixed.second, row});
    }
  }
}

static void GenUpdateDeleteIndexArguments(benchmark::internal::Benchmark *b) {
  std::vector<uint32_t> idx_key = {1, 2, 4, 8, 15};
  std::vector<uint32_t> row_nums = {1,     10,    100,   500,    1000,   2000,   5000,
                                    10000, 20000, 50000, 100000, 300000, 500000, 1000000};
  std::vector<type::TypeId> types = {type::TypeId::INTEGER, type::TypeId::BIGINT};

  for (auto type : types) {
    for (auto idx_key_size : idx_key) {
      for (auto row_num : row_nums) {
        if (row_num > updel_limit) continue;

        // Special argument used to indicate a build index
        // We need to do this to prevent update/delete from unintentionally
        // updating multiple indexes. This way, there will only be 1 index
        // on the table at a given time.
        if (type == type::TypeId::INTEGER)
          b->Args({idx_key_size, 0, 15, 0, row_num, 0, 1});
        else if (type == type::TypeId::BIGINT)
          b->Args({0, idx_key_size, 0, 15, row_num, 0, 1});

        int64_t lookup_size = 1;
        std::vector<int64_t> lookups;
        while (lookup_size <= row_num) {
          lookups.push_back(lookup_size);
          lookup_size *= 2;
        }

        for (auto lookup : lookups) {
          if (type == type::TypeId::INTEGER)
            b->Args({idx_key_size, 0, 15, 0, row_num, lookup, -1});
          else if (type == type::TypeId::BIGINT)
            b->Args({0, idx_key_size, 0, 15, row_num, lookup, -1});
        }

        // Special argument used to indicate a drop index
        if (type == type::TypeId::INTEGER)
          b->Args({idx_key_size, 0, 15, 0, row_num, 0, 0});
        else if (type == type::TypeId::BIGINT)
          b->Args({0, idx_key_size, 0, 15, row_num, 0, 0});
      }
    }
  }
}

/**
 * Arg <0, 1, 2, 3, 4, 5>
 * 0 - # integers to scan
 * 1 - # bigints to scan
 * 2 - # integers in table
 * 3 - # bigints in table
 * 4 - row
 * 5 - cardinality
 */
static void GenCreateIndexArguments(benchmark::internal::Benchmark *b) {
  auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
  auto types = {type::TypeId::INTEGER, type::TypeId::BIGINT};
  std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    200,    500,    1000,
                                   2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000};
  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : row_nums) {
        int64_t car = 1;
        if (row > create_index_small_limit) {
          // For these, we get a memory explosion if the cardinality is too low.
          while (car < row) {
            car *= 2;
          }
          car = car / (pow(2, create_index_large_cardinality_num));
        }

        while (car < row) {
          if (type == type::TypeId::INTEGER)
            b->Args({col, 0, 15, 0, row, car});
          else if (type == type::TypeId::BIGINT)
            b->Args({0, col, 0, 15, row, car});
          car *= 2;
        }

        if (type == type::TypeId::INTEGER)
          b->Args({col, 0, 15, 0, row, row});
        else if (type == type::TypeId::BIGINT)
          b->Args({0, col, 0, 15, row, row});
      }
    }
  }
}

class MiniRunners : public benchmark::Fixture {
 public:
  static execution::query_id_t query_id;
  static execution::vm::ExecutionMode mode;
  const uint64_t optimizer_timeout_ = 1000000;

  static std::unique_ptr<planner::AbstractPlanNode> PassthroughPlanChecker(
      UNUSED_ATTRIBUTE common::ManagedPointer<transaction::TransactionContext> txn,
      std::unique_ptr<planner::AbstractPlanNode> plan) {
    return plan;
  }

  void ExecuteSeqScan(benchmark::State *state);
  void ExecuteInsert(benchmark::State *state);
  void ExecuteUpdate(benchmark::State *state);
  void ExecuteDelete(benchmark::State *state);

  std::string ConstructIndexScanPredicate(int64_t key_num, int64_t num_rows, int64_t lookup_size,
                                          bool parameter = false) {
    std::mt19937 generator{};
    auto low_key = std::uniform_int_distribution(static_cast<uint32_t>(0),
                                                 static_cast<uint32_t>(num_rows - lookup_size))(generator);
    auto high_key = low_key + lookup_size - 1;

    std::stringstream predicatess;
    for (auto j = 1; j <= key_num; j++) {
      if (lookup_size == 1) {
        predicatess << "col" << j << " = ";
        if (parameter)
          predicatess << "$1";
        else
          predicatess << low_key;
      } else {
        predicatess << "col" << j << " >= ";
        if (parameter)
          predicatess << "$1";
        else
          predicatess << low_key;

        predicatess << " AND col" << j << " <= ";
        if (parameter)
          predicatess << "$2";
        else
          predicatess << high_key;
      }

      if (j != key_num) predicatess << " AND ";
    }
    return predicatess.str();
  }

  std::string ConstructColumns(const std::string &prefix, type::TypeId left_type, type::TypeId right_type,
                               int64_t num_left, int64_t num_right) {
    std::stringstream cols;
    for (auto i = 1; i <= num_left; i++) {
      cols << prefix << (type::TypeUtil::TypeIdToString(left_type)) << i;
      if (i != num_left || num_right != 0) cols << ", ";
    }

    for (auto i = 1; i <= num_right; i++) {
      cols << prefix << (type::TypeUtil::TypeIdToString(right_type)) << i;
      if (i != num_right) cols << ", ";
    }
    return cols.str();
  }

  std::string ConstructPredicate(const std::string &left_alias, const std::string &right_alias, type::TypeId left_type,
                                 type::TypeId right_type, int64_t num_left, int64_t num_right) {
    std::stringstream pred;
    for (auto i = 1; i <= num_left; i++) {
      auto type_name = type::TypeUtil::TypeIdToString(left_type);
      pred << left_alias << "." << type_name << i << " = " << right_alias << "." << type_name << i;
      if (i != num_left || num_right != 0) pred << " AND ";
    }

    for (auto i = 1; i <= num_right; i++) {
      auto type_name = type::TypeUtil::TypeIdToString(right_type);
      pred << left_alias << "." << type_name << i << " = " << right_alias << "." << type_name << i;
      if (i != num_right) pred << " AND ";
    }
    return pred.str();
  }

  std::string ConstructTableName(type::TypeId left_type, type::TypeId right_type, int64_t num_left, int64_t num_right,
                                 size_t row, size_t car) {
    std::vector<type::TypeId> types = {left_type, right_type};
    std::vector<uint32_t> col_counts = {static_cast<uint32_t>(num_left), static_cast<uint32_t>(num_right)};
    auto tbl_name = execution::sql::TableGenerator::GenerateMixedTableName(types, col_counts, row, car);
    if (num_left == 0)
      tbl_name = execution::sql::TableGenerator::GenerateTableName(right_type, num_right, row, car);
    else if (num_right == 0)
      tbl_name = execution::sql::TableGenerator::GenerateTableName(left_type, num_left, row, car);
    return tbl_name;
  }

  std::unique_ptr<planner::AbstractPlanNode> IndexScanChecker(
      size_t num_keys, common::ManagedPointer<transaction::TransactionContext> txn,
      std::unique_ptr<planner::AbstractPlanNode> plan) {
    if (plan->GetPlanNodeType() != planner::PlanNodeType::INDEXSCAN) throw "Expected IndexScan";

    auto *idx_scan = reinterpret_cast<planner::IndexScanPlanNode *>(plan.get());
    if (idx_scan->GetLoIndexColumns().size() != num_keys) throw "Number keys mismatch";

    return plan;
  }

  std::unique_ptr<planner::AbstractPlanNode> IndexNLJoinChecker(
      std::string build_tbl, size_t num_cols, common::ManagedPointer<transaction::TransactionContext> txn,
      std::unique_ptr<planner::AbstractPlanNode> plan) {
    if (plan->GetPlanNodeType() != planner::PlanNodeType::INDEXNLJOIN) throw "Expected IndexNLJoin";
    if (plan->GetChildrenSize() != 1) throw "Expected 1 child";
    if (plan->GetChild(0)->GetPlanNodeType() != planner::PlanNodeType::SEQSCAN) throw "Expected child IdxScan";

    auto *nlplan = reinterpret_cast<const planner::IndexJoinPlanNode *>(plan.get());
    if (nlplan->GetLoIndexColumns().size() != num_cols) throw "Number keys mismatch";

    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
    auto build_oid = accessor->GetTableOid(std::move(build_tbl));
    if (nlplan->GetTableOid() != build_oid) throw "inner index does not match";

    return plan;
  }

  std::unique_ptr<planner::AbstractPlanNode> ChildIndexScanChecker(
      common::ManagedPointer<transaction::TransactionContext> txn, std::unique_ptr<planner::AbstractPlanNode> plan) {
    if (plan->GetChild(0)->GetPlanNodeType() != planner::PlanNodeType::INDEXSCAN) throw "Expected IndexScan";

    return plan;
  }

  std::unique_ptr<planner::AbstractPlanNode> JoinNonSelfChecker(
      std::string build_tbl, common::ManagedPointer<transaction::TransactionContext> txn,
      std::unique_ptr<planner::AbstractPlanNode> plan) {
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
    auto build_oid = accessor->GetTableOid(std::move(build_tbl));

    if (plan->GetPlanNodeType() != planner::PlanNodeType::HASHJOIN) throw "Expected HashJoin";
    if (plan->GetChild(0)->GetPlanNodeType() != planner::PlanNodeType::SEQSCAN) throw "Expected Left SeqScan";
    if (plan->GetChild(1)->GetPlanNodeType() != planner::PlanNodeType::SEQSCAN) throw "Expected Right SeqScan";

    // Assumes the build side is the left (since that is the HashJoinLeftTranslator)
    auto *l_scan = reinterpret_cast<const planner::SeqScanPlanNode *>(plan->GetChild(0));
    if (l_scan->GetTableOid() != build_oid) {
      // Don't modify join_predicate/left/right hash keys because derivedindex DerivedValueExpression
      // Don't modify output_schema since just output 1 side tuple anyways
      plan->SwapChildren();
    }

    return plan;
  }

  TIGHT_LOOP_OPERATION(uint32_t, PLUS, +);
  TIGHT_LOOP_OPERATION(uint32_t, MULTIPLY, *);
  TIGHT_LOOP_OPERATION(uint32_t, DIVIDE, /);
  TIGHT_LOOP_OPERATION(uint32_t, GEQ, >=);
  TIGHT_LOOP_OPERATION(double, PLUS, +);
  TIGHT_LOOP_OPERATION(double, MULTIPLY, *);
  TIGHT_LOOP_OPERATION(double, DIVIDE, /);
  TIGHT_LOOP_OPERATION(double, GEQ, >=);

  void SetUp(const benchmark::State &state) final {
    catalog_ = db_main->GetCatalogLayer()->GetCatalog();
    txn_manager_ = db_main->GetTransactionLayer()->GetTransactionManager();
    metrics_manager_ = db_main->GetMetricsManager();
  }

  static execution::exec::ExecutionSettings GetExecutionSettings() {
    execution::exec::ExecutionSettings settings;
    settings.is_parallel_execution_enabled_ = false;
    return settings;
  }

  std::pair<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::OutputSchema>>
  OptimizeSqlStatement(
      const std::string &query, std::unique_ptr<optimizer::AbstractCostModel> cost_model,
      std::unique_ptr<brain::PipelineOperatingUnits> pipeline_units,
      const std::function<std::unique_ptr<planner::AbstractPlanNode>(
          common::ManagedPointer<transaction::TransactionContext>, std::unique_ptr<planner::AbstractPlanNode>)>
          &checker = std::function<std::unique_ptr<planner::AbstractPlanNode>(
              common::ManagedPointer<transaction::TransactionContext>, std::unique_ptr<planner::AbstractPlanNode>)>(
              PassthroughPlanChecker),
      common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params = nullptr,
      common::ManagedPointer<std::vector<type::TypeId>> param_types = nullptr) {
    auto txn = txn_manager_->BeginTransaction();
    auto stmt_list = parser::PostgresParser::BuildParseTree(query);

    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
    auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid);
    binder.BindNameToNode(common::ManagedPointer(stmt_list), params, param_types);

    auto out_plan = trafficcop::TrafficCopUtil::Optimize(
        common::ManagedPointer(txn), common::ManagedPointer(accessor), common::ManagedPointer(stmt_list), db_oid,
        db_main->GetStatsStorage(), std::move(cost_model), optimizer_timeout_);

    out_plan = checker(common::ManagedPointer(txn), std::move(out_plan));
    if (out_plan->GetPlanNodeType() == planner::PlanNodeType::CREATE_INDEX) {
      execution::sql::DDLExecutors::CreateIndexExecutor(
          common::ManagedPointer<planner::CreateIndexPlanNode>(
              reinterpret_cast<planner::CreateIndexPlanNode *>(out_plan.get())),
          common::ManagedPointer<catalog::CatalogAccessor>(accessor));
    } else if (out_plan->GetPlanNodeType() == planner::PlanNodeType::DROP_INDEX) {
      execution::sql::DDLExecutors::DropIndexExecutor(
          common::ManagedPointer<planner::DropIndexPlanNode>(
              reinterpret_cast<planner::DropIndexPlanNode *>(out_plan.get())),
          common::ManagedPointer<catalog::CatalogAccessor>(accessor));
      txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      return std::make_pair(nullptr, nullptr);
    }

    auto exec_settings = GetExecutionSettings();
    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
        db_oid, common::ManagedPointer(txn), execution::exec::NoOpResultConsumer(), out_plan->GetOutputSchema().Get(),
        common::ManagedPointer(accessor), exec_settings);

    execution::compiler::ExecutableQuery::query_identifier.store(MiniRunners::query_id++);
    auto exec_query = execution::compiler::CompilationContext::Compile(*out_plan, exec_settings, accessor.get(),
                                                                       execution::compiler::CompilationMode::OneShot);
    exec_query->SetPipelineOperatingUnits(std::move(pipeline_units));

    auto ret_val = std::make_pair(std::move(exec_query), out_plan->GetOutputSchema()->Copy());
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    return ret_val;
  }

  void HandleBuildDropIndex(bool is_build, int64_t num_rows, int64_t num_key, type::TypeId type) {
    auto block_store = db_main->GetStorageLayer()->GetBlockStore();
    auto catalog = db_main->GetCatalogLayer()->GetCatalog();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();

    auto txn = txn_manager->BeginTransaction();
    auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
    auto exec_settings = MiniRunners::GetExecutionSettings();
    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
        db_oid, common::ManagedPointer(txn), nullptr, nullptr, common::ManagedPointer(accessor), exec_settings);

    execution::sql::TableGenerator table_generator(exec_ctx.get(), block_store, accessor->GetDefaultNamespace());
    if (is_build) {
      table_generator.BuildMiniRunnerIndex(type, num_rows, num_key);
    } else {
      bool result = table_generator.DropMiniRunnerIndex(type, num_rows, num_key);
      if (!result) {
        throw "Drop Index has failed";
      }
    }

    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    InvokeGC();
  }

  void BenchmarkExecQuery(int64_t num_iters, execution::compiler::ExecutableQuery *exec_query,
                          planner::OutputSchema *out_schema, bool commit,
                          std::vector<std::vector<parser::ConstantValueExpression>> *params = &empty_params) {
    transaction::TransactionContext *txn = nullptr;
    std::unique_ptr<catalog::CatalogAccessor> accessor = nullptr;
    std::vector<std::vector<parser::ConstantValueExpression>> param_ref = *params;
    for (auto i = 0; i < num_iters; i++) {
      if (i == num_iters - 1) {
        metrics_manager_->RegisterThread();
      }

      txn = txn_manager_->BeginTransaction();
      accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

      auto exec_settings = GetExecutionSettings();
      auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
          db_oid, common::ManagedPointer(txn), execution::exec::NoOpResultConsumer(), out_schema,
          common::ManagedPointer(accessor), exec_settings);

      // Attach params to ExecutionContext
      if (static_cast<size_t>(i) < param_ref.size()) {
        exec_ctx->SetParams(common::ManagedPointer<const std::vector<parser::ConstantValueExpression>>(&param_ref[i]));
      }

      exec_query->Run(common::ManagedPointer(exec_ctx), mode);

      if (commit)
        txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      else
        txn_manager_->Abort(txn);

      if (i == num_iters - 1) {
        metrics_manager_->Aggregate();
        metrics_manager_->UnregisterThread();
      }
    }
  }

  void BenchmarkArithmetic(brain::ExecutionOperatingUnitType type, size_t num_elem) {
    auto qid = MiniRunners::query_id++;
    auto txn = txn_manager_->BeginTransaction();
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
    auto exec_settings = GetExecutionSettings();
    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
        db_oid, common::ManagedPointer(txn), nullptr, nullptr, common::ManagedPointer(accessor), exec_settings);
    exec_ctx->SetExecutionMode(static_cast<uint8_t>(mode));

    brain::PipelineOperatingUnits units;
    brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
    exec_ctx->SetPipelineOperatingUnits(common::ManagedPointer(&units));
    pipe0_vec.emplace_back(type, num_elem, 4, 1, num_elem, 1, 0);
    units.RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

    switch (type) {
      case brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS: {
        exec_ctx->StartResourceTracker(metrics::MetricsComponent::EXECUTION_PIPELINE);
        uint32_t ret = __uint32_t_PLUS(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY: {
        exec_ctx->StartResourceTracker(metrics::MetricsComponent::EXECUTION_PIPELINE);
        uint32_t ret = __uint32_t_MULTIPLY(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_INTEGER_DIVIDE: {
        exec_ctx->StartResourceTracker(metrics::MetricsComponent::EXECUTION_PIPELINE);
        uint32_t ret = __uint32_t_DIVIDE(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE: {
        exec_ctx->StartResourceTracker(metrics::MetricsComponent::EXECUTION_PIPELINE);
        uint32_t ret = __uint32_t_GEQ(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_DECIMAL_PLUS_OR_MINUS: {
        exec_ctx->StartResourceTracker(metrics::MetricsComponent::EXECUTION_PIPELINE);
        double ret = __double_PLUS(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_DECIMAL_MULTIPLY: {
        exec_ctx->StartResourceTracker(metrics::MetricsComponent::EXECUTION_PIPELINE);
        double ret = __double_MULTIPLY(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_DECIMAL_DIVIDE: {
        exec_ctx->StartResourceTracker(metrics::MetricsComponent::EXECUTION_PIPELINE);
        double ret = __double_DIVIDE(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_DECIMAL_COMPARE: {
        exec_ctx->StartResourceTracker(metrics::MetricsComponent::EXECUTION_PIPELINE);
        double ret = __double_GEQ(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1));
        DoNotOptimizeAway(ret);
        break;
      }
      default:
        UNREACHABLE("Unsupported ExecutionOperatingUnitType");
        break;
    }

    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

 protected:
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<metrics::MetricsManager> metrics_manager_;
};

execution::query_id_t MiniRunners::query_id = execution::query_id_t(0);
execution::vm::ExecutionMode MiniRunners::mode = execution::vm::ExecutionMode::Interpret;

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ0_ArithmeticRunners)(benchmark::State &state) {
  if (rerun_start) {
    return;
  }

  metrics_manager_->RegisterThread();

  // state.range(0) is the OperatingUnitType
  // state.range(1) is the size
  BenchmarkArithmetic(static_cast<brain::ExecutionOperatingUnitType>(state.range(0)),
                      static_cast<size_t>(state.range(1)));

  metrics_manager_->Aggregate();
  metrics_manager_->UnregisterThread();

  state.SetItemsProcessed(state.range(1));
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ0_ArithmeticRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenArithArguments);

void NetworkQueriesOutputRunners(pqxx::work *txn) {
  std::ostream null{nullptr};
  auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
  auto types = {type::TypeId::INTEGER, type::TypeId::DECIMAL};
  std::vector<int64_t> row_nums = {1, 3, 5, 7, 10, 50, 100, 500, 1000, 2000, 5000, 10000};

  bool metrics_enabled = true;
  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : row_nums) {
        // Scale # iterations accordingly
        // Want to warmup the first query
        int iters = 1;
        if (row == 1 && col == 1 && type == type::TypeId::INTEGER) {
          iters += warmup_iterations_num;
        }

        for (int i = 0; i < iters; i++) {
          if (i != iters - 1 && metrics_enabled) {
            db_main->GetMetricsManager()->DisableMetric(metrics::MetricsComponent::EXECUTION_PIPELINE);
            metrics_enabled = false;
          } else if (i == iters - 1 && !metrics_enabled) {
            db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::EXECUTION_PIPELINE, 0);
            metrics_enabled = true;
          }

          std::stringstream query_ss;
          std::string type_s = (type == type::TypeId::INTEGER) ? "int" : "real";

          query_ss << "SELECT nprunnersemit" << type_s << "(" << row << "," << col << ",";
          if (type == type::TypeId::INTEGER)
            query_ss << col << ",0)";
          else
            query_ss << "0," << col << ")";

          if (col > 1) {
            query_ss << ",";
            for (int j = 1; j < col; j++) {
              query_ss << "nprunnersdummy" << type_s << "()";
              if (j != col - 1) {
                query_ss << ",";
              }
            }
          }

          // Execute query
          pqxx::result r{txn->exec(query_ss.str())};

          // Get all the results
          for (const auto &result_row : r) {
            for (auto j = 0; j < col; j++) {
              null << result_row[j];
            }
          }
        }
      }
    }
  }
}

void NetworkQueriesCreateIndexRunners(pqxx::work *txn) {
  std::ostream null{nullptr};
  std::vector<uint32_t> num_cols = {1, 2, 4, 8, 15};
  std::vector<type::TypeId> types = {type::TypeId::INTEGER, type::TypeId::BIGINT};
  std::vector<uint32_t> row_nums = {1,     10,    100,   200,    500,    1000,   2000,   5000,
                                    10000, 20000, 50000, 100000, 300000, 500000, 1000000};

  bool metrics_enabled = true;
  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : row_nums) {
        // Scale # iterations accordingly
        // Want to warmup the first query
        int iters = 1;
        if (row == 1 && col == 1 && type == type::TypeId::INTEGER) {
          iters += warmup_iterations_num;
        }

        for (int i = 0; i < iters; i++) {
          if (i != iters - 1 && metrics_enabled) {
            db_main->GetMetricsManager()->DisableMetric(metrics::MetricsComponent::EXECUTION_PIPELINE);
            metrics_enabled = false;
          } else if (i == iters - 1 && !metrics_enabled) {
            db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::EXECUTION_PIPELINE, 0);
            metrics_enabled = true;
          }

          std::string create_query;
          {
            std::stringstream query_ss;
            auto table_name = execution::sql::TableGenerator::GenerateTableIndexName(type, row);
            query_ss << "CREATE INDEX minirunners__" << row << " ON " << table_name << "(";
            for (size_t j = 1; j <= col; j++) {
              query_ss << "col" << j;
              if (j != col) {
                query_ss << ",";
              } else {
                query_ss << ")";
              }
            }

            create_query = query_ss.str();
          }
          txn->exec(create_query);

          std::string delete_query;
          {
            std::stringstream query_ss;
            query_ss << "DROP INDEX minirunners__" << row;
            delete_query = query_ss.str();
          }
          txn->exec(delete_query);
        }
      }
    }
  }
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ0_OutputRunners)(benchmark::State &state) {
  auto num_integers = state.range(0);
  auto num_decimals = state.range(1);
  auto row_num = state.range(2);
  auto num_col = num_integers + num_decimals;

  std::stringstream output;
  output << "struct OutputStruct {\n";
  for (auto i = 0; i < num_integers; i++) output << "col" << i << " : Integer\n";
  for (auto i = num_integers; i < num_col; i++) output << "col" << i << " : Real\n";
  output << "}\n";

  output << "struct QueryState {\nexecCtx: *ExecutionContext\n}\n";
  output << "struct P1_State {\n}\n";
  output << "fun Query0_Init(queryState: *QueryState) -> nil {\nreturn}\n";
  output << "fun Query0_Pipeline1_InitPipelineState(queryState: *QueryState, pipelineState: *P1_State) -> nil "
            "{\nreturn\n}\n";
  output << "fun Query0_Pipeline1_TearDownPipelineState(queryState: *QueryState, pipelineState: *P1_State) -> nil "
            "{\nreturn\n}\n";

  // pipeline
  output << "fun Query0_Pipeline1_SerialWork(queryState: *QueryState, pipelineState: *P1_State) -> nil {\n";
  if (num_col > 0) {
    output << "\tvar out: *OutputStruct\n";
    output << "\tfor(var it = 0; it < " << row_num << "; it = it + 1) {\n";
    output << "\t\tout = @ptrCast(*OutputStruct, @resultBufferAllocRow(queryState.execCtx))\n";
    output << "\t}\n";
  }
  output << "}\n";

  output << "fun Query0_Pipeline1_Init(queryState: *QueryState) -> nil {\n";
  output << "\tvar threadStateContainer = @execCtxGetTLS(queryState.execCtx)\n";
  output << "\t@tlsReset(threadStateContainer, @sizeOf(P1_State), Query0_Pipeline1_InitPipelineState, "
            "Query0_Pipeline1_TearDownPipelineState, queryState)\n";
  output << "\treturn\n";
  output << "}\n";

  output << "fun Query0_Pipeline1_Run(queryState: *QueryState) -> nil {\n";
  output << "\t@execCtxStartResourceTracker(queryState.execCtx, 4)\n";
  output
      << "\tvar pipelineState = @ptrCast(*P1_State, @tlsGetCurrentThreadState(@execCtxGetTLS(queryState.execCtx)))\n";
  output << "\tQuery0_Pipeline1_SerialWork(queryState, pipelineState)\n";
  output << "\t@resultBufferFinalize(queryState.execCtx)\n";
  output << "\t@execCtxEndPipelineTracker(queryState.execCtx, 0, 1)\n";
  output << "\treturn\n";
  output << "}\n";

  output << "fun Query0_Pipeline1_TearDown(queryState: *QueryState) -> nil {\n";
  output << "\t@tlsClear(@execCtxGetTLS(queryState.execCtx))\n";
  output << "\treturn\n";
  output << "}\n";

  output << "fun Query0_TearDown(queryState: *QueryState) -> nil {\nreturn\n}\n";

  output << "fun main(queryState: *QueryState) -> nil {\n";
  output << "\tQuery0_Pipeline1_Init(queryState)\n";
  output << "\tQuery0_Pipeline1_Run(queryState)\n";
  output << "\tQuery0_Pipeline1_TearDown(queryState)\n";
  output << "\treturn\n";
  output << "}\n";

  std::vector<planner::OutputSchema::Column> cols;
  for (auto i = 0; i < num_integers; i++) {
    std::stringstream col;
    col << "col" << i;
    cols.emplace_back(col.str(), type::TypeId::INTEGER, nullptr);
  }

  for (auto i = 0; i < num_decimals; i++) {
    std::stringstream col;
    col << "col" << i;
    cols.emplace_back(col.str(), type::TypeId::DECIMAL, nullptr);
  }

  auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
  auto decimal_size = type::TypeUtil::GetTypeSize(type::TypeId::DECIMAL);
  auto tuple_size = int_size * num_integers + decimal_size * num_decimals;

  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
  auto schema = std::make_unique<planner::OutputSchema>(std::move(cols));

  auto exec_settings = GetExecutionSettings();
  execution::compiler::ExecutableQuery::query_identifier.store(MiniRunners::query_id++);
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
      db_oid, common::ManagedPointer(txn), execution::exec::NoOpResultConsumer(), schema.get(),
      common::ManagedPointer(accessor), exec_settings);

  auto exec_query =
      execution::compiler::ExecutableQuery(output.str(), common::ManagedPointer(exec_ctx), false, 16, exec_settings);

  auto units = std::make_unique<brain::PipelineOperatingUnits>();
  brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, row_num, tuple_size, num_col, 0, 1, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));
  exec_query.SetPipelineOperatingUnits(std::move(units));

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  BenchmarkExecQuery(warmup_iterations_num + 1, &exec_query, schema.get(), true);
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ0_OutputRunners)
    ->Unit(benchmark::kMillisecond)
    ->Apply(GenOutputArguments)
    ->Iterations(1);

void MiniRunners::ExecuteSeqScan(benchmark::State *state) {
  auto num_integers = state->range(0);
  auto num_decimals = state->range(1);
  auto tbl_ints = state->range(2);
  auto tbl_decimals = state->range(3);
  auto row = state->range(4);
  auto car = state->range(5);

  int num_iters = 1;
  if (row <= warmup_rows_limit) {
    num_iters += warmup_iterations_num;
  } else if (rerun_start || skip_large_rows_runs) {
    return;
  }

  auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
  auto decimal_size = type::TypeUtil::GetTypeSize(type::TypeId::DECIMAL);
  auto tuple_size = int_size * num_integers + decimal_size * num_decimals;
  auto num_col = num_integers + num_decimals;

  auto units = std::make_unique<brain::PipelineOperatingUnits>();
  brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, row, tuple_size, num_col, car, 1, 0);
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, row, tuple_size, num_col, 0, 1, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  std::string query_final;
  {
    std::stringstream query;
    auto cols = ConstructColumns("", type::TypeId::INTEGER, type::TypeId::DECIMAL, num_integers, num_decimals);
    auto tbl_name = ConstructTableName(type::TypeId::INTEGER, type::TypeId::DECIMAL, tbl_ints, tbl_decimals, row, car);
    query << "SELECT " << (cols) << " FROM " << tbl_name;
    query_final = query.str();
  }

  auto equery = OptimizeSqlStatement(query_final, std::make_unique<optimizer::TrivialCostModel>(), std::move(units));
  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), true);

  state->SetItemsProcessed(row);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ1_0_SeqScanRunners)(benchmark::State &state) { ExecuteSeqScan(&state); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ1_1_SeqScanRunners)(benchmark::State &state) { ExecuteSeqScan(&state); }

BENCHMARK_REGISTER_F(MiniRunners, SEQ1_0_SeqScanRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenScanArguments);

BENCHMARK_REGISTER_F(MiniRunners, SEQ1_1_SeqScanRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenScanMixedArguments);

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ2_0_IndexScanRunners)(benchmark::State &state) {
  auto type = static_cast<type::TypeId>(state.range(0));
  auto key_num = state.range(1);
  auto num_rows = state.range(2);
  auto lookup_size = state.range(3);
  auto is_build = state.range(4);

  if (lookup_size == 0) {
    if (is_build < 0) {
      throw "Invalid is_build argument for IndexScan";
    }

    HandleBuildDropIndex(is_build != 0, num_rows, key_num, type);
    return;
  }

  int num_iters = 1;
  if (lookup_size <= warmup_rows_limit) {
    num_iters += warmup_iterations_num;
  } else if (rerun_start || skip_large_rows_runs) {
    return;
  }

  std::vector<std::vector<parser::ConstantValueExpression>> real_params;
  GenIdxScanParameters(type, num_rows, lookup_size, num_iters, &real_params);

  auto type_size = type::TypeUtil::GetTypeSize(type);
  auto tuple_size = type_size * key_num;

  auto units = std::make_unique<brain::PipelineOperatingUnits>();
  brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, lookup_size, tuple_size, key_num, 0, 1, 0);
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::IDX_SCAN, num_rows, tuple_size, key_num, lookup_size, 1, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  std::string cols;
  {
    std::stringstream colss;
    for (auto j = 1; j <= key_num; j++) {
      colss << "col" << j;
      if (j != key_num) colss << ", ";
    }
    cols = colss.str();
  }

  std::string predicate = ConstructIndexScanPredicate(key_num, num_rows, lookup_size, true);

  std::vector<parser::ConstantValueExpression> params;
  std::vector<type::TypeId> param_types;
  params.emplace_back(type, execution::sql::Integer(0));
  param_types.push_back(type);
  if (lookup_size > 1) {
    params.emplace_back(type, execution::sql::Integer(0));
    param_types.push_back(type);
  }

  std::stringstream query;
  auto table_name = execution::sql::TableGenerator::GenerateTableIndexName(type, num_rows);
  query << "SELECT " << cols << " FROM  " << table_name << " WHERE " << predicate;
  auto f = std::bind(&MiniRunners::IndexScanChecker, this, key_num, std::placeholders::_1, std::placeholders::_2);
  auto equery = OptimizeSqlStatement(query.str(), std::make_unique<optimizer::TrivialCostModel>(), std::move(units), f,
                                     common::ManagedPointer<std::vector<parser::ConstantValueExpression>>(&params),
                                     common::ManagedPointer<std::vector<type::TypeId>>(&param_types));
  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), true, &real_params);

  state.SetItemsProcessed(state.range(2));
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ2_0_IndexScanRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenIdxScanArguments);

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ2_1_IndexJoinRunners)(benchmark::State &state) {
  auto type = type::TypeId::INTEGER;
  auto key_num = state.range(0);
  auto outer = state.range(1);
  auto inner = state.range(2);
  auto is_build = state.range(3);

  if (outer == 0) {
    if (is_build < 0) {
      throw "Invalid is_build argument for IndexJoin";
    }

    HandleBuildDropIndex(is_build != 0, inner, key_num, type);
    return;
  }

  // No warmup
  int num_iters = 1;
  auto type_size = type::TypeUtil::GetTypeSize(type);
  auto tuple_size = type_size * key_num;

  auto units = std::make_unique<brain::PipelineOperatingUnits>();
  brain::ExecutionOperatingUnitFeatureVector pipe0_vec;

  // We only ever emit min(outer, inner) # of tuples
  // Even though there are no matches, it still might be a good idea to see what the relation is
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, std::min(inner, outer), tuple_size, key_num, 0, 1,
                         0);

  // Outer table scan happens
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, outer, tuple_size, key_num, outer, 1, 0);

  // For each in outer, match 1 tuple in inner
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::IDX_SCAN, inner, tuple_size, key_num, 1, 1, outer);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  std::string cols;
  {
    std::stringstream colss;
    for (auto j = 1; j <= key_num; j++) {
      colss << "a.col" << j;
      if (j != key_num) colss << ", ";
    }
    cols = colss.str();
  }

  std::string predicate;
  {
    std::stringstream preds;
    for (auto j = 1; j <= key_num; j++) {
      preds << "a.col" << j << " = b.col" << j;
      if (j != key_num) preds << " AND ";
    }
    predicate = preds.str();
  }

  auto outer_tbl = execution::sql::TableGenerator::GenerateTableIndexName(type, outer);
  auto inner_tbl = execution::sql::TableGenerator::GenerateTableIndexName(type, inner);

  std::stringstream query;
  query << "SELECT " << cols << " FROM " << outer_tbl << " AS a, " << inner_tbl << " AS b WHERE " << predicate;
  auto f = std::bind(&MiniRunners::IndexNLJoinChecker, this, inner_tbl, key_num, std::placeholders::_1,
                     std::placeholders::_2);
  auto equery = OptimizeSqlStatement(query.str(), std::make_unique<optimizer::TrivialCostModel>(), std::move(units), f);
  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), true);

  state.SetItemsProcessed(state.range(2));
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ2_1_IndexJoinRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenIdxJoinArguments);

void MiniRunners::ExecuteInsert(benchmark::State *state) {
  auto num_ints = state->range(0);
  auto num_decimals = state->range(1);
  auto num_cols = state->range(2);
  auto num_rows = state->range(3);

  // TODO(wz2): Re-enable compiled inserts once runtime is sensible
  if (terrier::runner::MiniRunners::mode == execution::vm::ExecutionMode::Compiled) return;

  if (rerun_start || (num_rows > warmup_rows_limit && skip_large_rows_runs)) return;

  // Create temporary table schema
  std::vector<catalog::Schema::Column> cols;
  std::vector<std::pair<type::TypeId, int64_t>> info = {{type::TypeId::INTEGER, num_ints},
                                                        {type::TypeId::DECIMAL, num_decimals}};
  int col_no = 1;
  for (auto &i : info) {
    for (auto j = 1; j <= i.second; j++) {
      std::stringstream col_name;
      col_name << "col" << col_no++;
      if (i.first == type::TypeId::INTEGER) {
        cols.emplace_back(col_name.str(), i.first, false,
                          terrier::parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(0)));
      } else {
        cols.emplace_back(col_name.str(), i.first, false,
                          terrier::parser::ConstantValueExpression(type::TypeId::DECIMAL, execution::sql::Real(0.f)));
      }
    }
  }
  catalog::Schema tmp_schema(cols);

  // Create table
  catalog::table_oid_t tbl_oid;
  {
    auto txn = txn_manager_->BeginTransaction();
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
    tbl_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "tmp_table", tmp_schema);
    auto &schema = accessor->GetSchema(tbl_oid);
    auto *tmp_table = new storage::SqlTable(db_main->GetStorageLayer()->GetBlockStore(), schema);
    accessor->SetTablePointer(tbl_oid, tmp_table);
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

  std::string tuple_row;
  {
    std::stringstream tuple;
    tuple << "(";
    for (uint32_t i = 1; i <= num_cols; i++) {
      tuple << i;
      if (i != num_cols) {
        tuple << ",";
      } else {
        tuple << ")";
      }
    }
    tuple_row = tuple.str();
  }

  // Hack to preallocate some memory
  std::string reserves;
  std::string query;
  query.reserve(tuple_row.length() * num_rows + num_rows + 100);

  query += "INSERT INTO tmp_table VALUES ";
  for (uint32_t idx = 0; idx < num_rows; idx++) {
    query += tuple_row;
    if (idx != num_rows - 1) {
      query += ", ";
    }
  }

  auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
  auto decimal_size = type::TypeUtil::GetTypeSize(type::TypeId::DECIMAL);
  auto tuple_size = int_size * num_ints + decimal_size * num_decimals;

  auto units = std::make_unique<brain::PipelineOperatingUnits>();
  brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::INSERT, num_rows, tuple_size, num_cols, num_rows, 1, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  auto equery = OptimizeSqlStatement(query, std::make_unique<optimizer::TrivialCostModel>(), std::move(units));
  BenchmarkExecQuery(1, equery.first.get(), equery.second.get(), true);

  // Drop the table
  {
    auto txn = txn_manager_->BeginTransaction();
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
    accessor->DropTable(tbl_oid);
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

  InvokeGC();
  state->SetItemsProcessed(num_rows);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ6_0_InsertRunners)(benchmark::State &state) { ExecuteInsert(&state); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ6_1_InsertRunners)(benchmark::State &state) { ExecuteInsert(&state); }

BENCHMARK_REGISTER_F(MiniRunners, SEQ6_0_InsertRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenInsertArguments);

BENCHMARK_REGISTER_F(MiniRunners, SEQ6_1_InsertRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenInsertMixedArguments);

void MiniRunners::ExecuteUpdate(benchmark::State *state) {
  auto num_integers = state->range(0);
  auto num_decimals = state->range(1);
  auto tbl_ints = state->range(2);
  auto row = state->range(4);
  auto car = state->range(5);
  auto is_build = state->range(6);

  if (row == 0) {
    state->SetItemsProcessed(row);
    InvokeGC();
    return;
  }

  // A lookup size of 0 indicates a special query
  auto type = tbl_ints != 0 ? (type::TypeId::INTEGER) : (type::TypeId::BIGINT);
  if (car == 0) {
    if (is_build < 0) {
      throw "Invalid is_build argument for ExecuteUpdate";
    }

    HandleBuildDropIndex(is_build != 0, row, num_integers + num_decimals, type);
    return;
  }

  int num_iters = 1;
  if (car <= warmup_rows_limit) {
    num_iters += warmup_iterations_num;
  } else if (rerun_start || skip_large_rows_runs) {
    return;
  }

  // UPDATE [] SET [col] = random integer()
  // This does not force a read from the underlying tuple more than getting the slot.
  // Arguably, this approach has the least amount of "SEQ_SCAN" overhead and measures:
  // - Iterating over entire table for the slot
  // - Cost of "merging" updates with the undo/redos
  std::stringstream query;
  std::string tbl = execution::sql::TableGenerator::GenerateTableIndexName(type, row);
  query << "UPDATE " << tbl << " SET ";

  auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
  auto decimal_size = type::TypeUtil::GetTypeSize(type::TypeId::BIGINT);
  auto tuple_size = int_size * num_integers + decimal_size * num_decimals;
  auto num_col = num_integers + num_decimals;
  std::vector<catalog::Schema::Column> cols;
  std::mt19937 generator{};
  std::uniform_int_distribution<int> distribution(0, INT_MAX);
  for (auto j = 1; j <= num_integers; j++) {
    // We need to do this to prevent the lookup from having to move
    query << "col" << j << " = "
          << "col" << j << " + 0";
    if (j != num_integers || num_decimals != 0) query << ", ";
  }

  for (auto j = 1; j <= num_decimals; j++) {
    query << "col" << j << " = "
          << "col" << j << " + 0";
    if (j != num_decimals) query << ", ";
  }

  std::vector<std::vector<parser::ConstantValueExpression>> real_params;
  std::pair<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::OutputSchema>> equery;
  auto cost = std::make_unique<optimizer::TrivialCostModel>();

  auto units = std::make_unique<brain::PipelineOperatingUnits>();
  brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::UPDATE, car, tuple_size, num_col, car, 1, 0);
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::IDX_SCAN, row, tuple_size, num_col, car, 1, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  std::vector<parser::ConstantValueExpression> params;
  std::vector<type::TypeId> param_types;
  params.emplace_back(type, execution::sql::Integer(0));
  param_types.push_back(type);
  if (car > 1) {
    params.emplace_back(type, execution::sql::Integer(0));
    param_types.push_back(type);
  }

  GenIdxScanParameters(type, row, car, num_iters, &real_params);
  std::string predicate = ConstructIndexScanPredicate(num_col, row, car, true);
  query << " WHERE " << predicate;

  auto f = std::bind(&MiniRunners::ChildIndexScanChecker, this, std::placeholders::_1, std::placeholders::_2);
  equery = OptimizeSqlStatement(query.str(), std::move(cost), std::move(units), f,
                                common::ManagedPointer<std::vector<parser::ConstantValueExpression>>(&params),
                                common::ManagedPointer<std::vector<type::TypeId>>(&param_types));

  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), false, &real_params);
  state->SetItemsProcessed(row);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ7_2_UpdateRunners)(benchmark::State &state) { ExecuteUpdate(&state); }

BENCHMARK_REGISTER_F(MiniRunners, SEQ7_2_UpdateRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenUpdateDeleteIndexArguments);

void MiniRunners::ExecuteDelete(benchmark::State *state) {
  auto num_integers = state->range(0);
  auto num_decimals = state->range(1);
  auto tbl_ints = state->range(2);
  auto tbl_decimals = state->range(3);
  auto row = state->range(4);
  auto car = state->range(5);
  auto is_build = state->range(6);

  if (row == 0) {
    state->SetItemsProcessed(row);
    InvokeGC();
    return;
  }

  // A lookup size of 0 indicates a special query
  auto type = tbl_ints != 0 ? (type::TypeId::INTEGER) : (type::TypeId::BIGINT);
  if (car == 0) {
    if (is_build < 0) {
      throw "Invalid is_build argument for ExecuteDelete";
    }

    HandleBuildDropIndex(is_build != 0, row, num_integers + num_decimals, type);
    return;
  }

  int num_iters = 1;
  if (car <= warmup_rows_limit) {
    num_iters += warmup_iterations_num;
  } else if (rerun_start || skip_large_rows_runs) {
    return;
  }

  auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
  auto decimal_size = type::TypeUtil::GetTypeSize(type::TypeId::BIGINT);
  auto tuple_size = int_size * num_integers + decimal_size * num_decimals;
  auto num_col = num_integers + num_decimals;

  std::stringstream query;
  std::vector<std::vector<parser::ConstantValueExpression>> real_params;
  std::pair<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::OutputSchema>> equery;
  auto cost = std::make_unique<optimizer::TrivialCostModel>();

  auto tbl_col = tbl_ints + tbl_decimals;
  auto tbl_size = tbl_ints * int_size + tbl_decimals * decimal_size;

  auto units = std::make_unique<brain::PipelineOperatingUnits>();
  brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::DELETE, car, tbl_size, tbl_col, car, 1, 0);
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::IDX_SCAN, row, tuple_size, num_col, car, 1, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  std::vector<parser::ConstantValueExpression> params;
  std::vector<type::TypeId> param_types;
  params.emplace_back(type, execution::sql::Integer(0));
  param_types.push_back(type);
  if (car > 1) {
    params.emplace_back(type, execution::sql::Integer(0));
    param_types.push_back(type);
  }

  GenIdxScanParameters(type, row, car, num_iters, &real_params);
  std::string predicate = ConstructIndexScanPredicate(num_col, row, car, true);
  query << "DELETE FROM " << execution::sql::TableGenerator::GenerateTableIndexName(type, row) << " WHERE "
        << predicate;

  auto f = std::bind(&MiniRunners::ChildIndexScanChecker, this, std::placeholders::_1, std::placeholders::_2);
  equery = OptimizeSqlStatement(query.str(), std::move(cost), std::move(units), f,
                                common::ManagedPointer<std::vector<parser::ConstantValueExpression>>(&params),
                                common::ManagedPointer<std::vector<type::TypeId>>(&param_types));

  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), false, &real_params);
  state->SetItemsProcessed(row);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ8_2_DeleteRunners)(benchmark::State &state) { ExecuteDelete(&state); }

BENCHMARK_REGISTER_F(MiniRunners, SEQ8_2_DeleteRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenUpdateDeleteIndexArguments);

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ3_SortRunners)(benchmark::State &state) {
  auto num_integers = state.range(0);
  auto num_decimals = state.range(1);
  auto tbl_ints = state.range(2);
  auto tbl_decimals = state.range(3);
  auto row = state.range(4);
  auto car = state.range(5);

  int num_iters = 1;
  if (row <= warmup_rows_limit) {
    num_iters += warmup_iterations_num;
  } else if (rerun_start || skip_large_rows_runs) {
    return;
  }

  auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
  auto decimal_size = type::TypeUtil::GetTypeSize(type::TypeId::DECIMAL);
  auto tuple_size = int_size * num_integers + decimal_size * num_decimals;
  auto num_col = num_integers + num_decimals;

  auto units = std::make_unique<brain::PipelineOperatingUnits>();
  brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
  brain::ExecutionOperatingUnitFeatureVector pipe1_vec;
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, row, tuple_size, num_col, car, 1, 0);
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SORT_BUILD, row, tuple_size, num_col, car, 1, 0);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::SORT_ITERATE, row, tuple_size, num_col, car, 1, 0);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, row, tuple_size, num_col, 0, 1, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(2), std::move(pipe0_vec));
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

  std::stringstream query;
  auto cols = ConstructColumns("", type::TypeId::INTEGER, type::TypeId::DECIMAL, num_integers, num_decimals);
  auto tbl_name = ConstructTableName(type::TypeId::INTEGER, type::TypeId::DECIMAL, tbl_ints, tbl_decimals, row, car);
  query << "SELECT " << (cols) << " FROM " << tbl_name << " ORDER BY " << (cols);
  auto equery = OptimizeSqlStatement(query.str(), std::make_unique<optimizer::TrivialCostModel>(), std::move(units));
  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), true);
  state.SetItemsProcessed(row);
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ3_SortRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenSortArguments);

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ4_HashJoinSelfRunners)(benchmark::State &state) {
  auto num_integers = state.range(0);
  auto num_bigints = state.range(1);
  auto tbl_ints = state.range(2);
  auto tbl_bigints = state.range(3);
  auto row = state.range(4);
  auto car = state.range(5);

  if (rerun_start) {
    return;
  }

  // Size of the scan tuple
  // Size of hash key size, probe key size
  // Size of output since only output 1 side
  auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
  auto bigint_size = type::TypeUtil::GetTypeSize(type::TypeId::BIGINT);
  auto tuple_size = int_size * num_integers + bigint_size * num_bigints;
  auto num_col = num_integers + num_bigints;

  auto hj_output = row * row / car;
  auto units = std::make_unique<brain::PipelineOperatingUnits>();
  brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
  brain::ExecutionOperatingUnitFeatureVector pipe1_vec;
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, row, tuple_size, num_col, car, 1, 0);
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::HASHJOIN_BUILD, row, tuple_size, num_col, car, 1, 0);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, row, tuple_size, num_col, car, 1, 0);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::HASHJOIN_PROBE, row, tuple_size, num_col, hj_output, 1, 0);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, hj_output, tuple_size, num_col, 0, 1, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(2), std::move(pipe0_vec));
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

  std::stringstream query;
  auto tbl_name = ConstructTableName(type::TypeId::INTEGER, type::TypeId::BIGINT, tbl_ints, tbl_bigints, row, car);
  query << "SELECT " << ConstructColumns("b.", type::TypeId::INTEGER, type::TypeId::BIGINT, num_integers, num_bigints);
  query << " FROM " << tbl_name << ", " << tbl_name << " as b WHERE ";
  query << ConstructPredicate(tbl_name, "b", type::TypeId::INTEGER, type::TypeId::BIGINT, num_integers, num_bigints);
  auto equery = OptimizeSqlStatement(query.str(), std::make_unique<optimizer::ForcedCostModel>(true), std::move(units));
  BenchmarkExecQuery(1, equery.first.get(), equery.second.get(), true);
  state.SetItemsProcessed(row);
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ4_HashJoinSelfRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenJoinSelfArguments);

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ4_HashJoinNonSelfRunners)(benchmark::State &state) {
  auto num_integers = state.range(0);
  auto num_bigints = state.range(1);
  auto tbl_ints = state.range(2);
  auto tbl_bigints = state.range(3);
  auto build_row = state.range(4);
  auto build_car = state.range(5);
  auto probe_row = state.range(6);
  auto probe_car = state.range(7);
  auto matched_car = state.range(8);

  if (rerun_start) {
    return;
  }

  auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
  auto bigint_size = type::TypeUtil::GetTypeSize(type::TypeId::BIGINT);
  auto tuple_size = int_size * num_integers + bigint_size * num_bigints;
  auto num_col = num_integers + num_bigints;

  auto units = std::make_unique<brain::PipelineOperatingUnits>();
  brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
  brain::ExecutionOperatingUnitFeatureVector pipe1_vec;
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, build_row, tuple_size, num_col, build_car, 1, 0);
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::HASHJOIN_BUILD, build_row, tuple_size, num_col, build_car,
                         1, 0);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, probe_row, tuple_size, num_col, probe_car, 1, 0);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::HASHJOIN_PROBE, probe_row, tuple_size, num_col, matched_car,
                         1, 0);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, matched_car, tuple_size, num_col, 0, 1, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(2), std::move(pipe0_vec));
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

  auto build_tbl =
      ConstructTableName(type::TypeId::INTEGER, type::TypeId::BIGINT, tbl_ints, tbl_bigints, build_row, build_car);
  auto probe_tbl =
      ConstructTableName(type::TypeId::INTEGER, type::TypeId::BIGINT, tbl_ints, tbl_bigints, probe_row, probe_car);

  std::stringstream query;
  query << "SELECT " << ConstructColumns("b.", type::TypeId::INTEGER, type::TypeId::BIGINT, num_integers, num_bigints);
  query << " FROM " << build_tbl << ", " << probe_tbl << " as b WHERE ";
  query << ConstructPredicate(build_tbl, "b", type::TypeId::INTEGER, type::TypeId::BIGINT, num_integers, num_bigints);

  auto f = std::bind(&MiniRunners::JoinNonSelfChecker, this, build_tbl, std::placeholders::_1, std::placeholders::_2);
  auto cost = std::make_unique<optimizer::ForcedCostModel>(true);
  auto equery = OptimizeSqlStatement(query.str(), std::move(cost), std::move(units), f);
  BenchmarkExecQuery(1, equery.first.get(), equery.second.get(), true);
  state.SetItemsProcessed(matched_car);
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ4_HashJoinNonSelfRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenJoinNonSelfArguments);

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ5_0_AggregateRunners)(benchmark::State &state) {
  auto num_integers = state.range(0);
  auto num_bigints = state.range(1);
  auto tbl_ints = state.range(2);
  auto tbl_bigints = state.range(3);
  auto row = state.range(4);
  auto car = state.range(5);

  int num_iters = 1;
  if (row <= warmup_rows_limit && car <= warmup_rows_limit) {
    num_iters += warmup_iterations_num;
  } else if (rerun_start || skip_large_rows_runs) {
    return;
  }

  auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
  auto bigint_size = type::TypeUtil::GetTypeSize(type::TypeId::BIGINT);
  auto tuple_size = int_size * num_integers + bigint_size * num_bigints;
  auto num_col = num_integers + num_bigints;
  auto out_cols = num_col + 1;     // pulling the count(*) out
  auto out_size = tuple_size + 4;  // count(*) is an integer

  auto units = std::make_unique<brain::PipelineOperatingUnits>();
  brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
  brain::ExecutionOperatingUnitFeatureVector pipe1_vec;
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, row, tuple_size, num_col, car, 1, 0);
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::AGGREGATE_BUILD, row, tuple_size, num_col, car, 1, 0);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::AGGREGATE_ITERATE, car, out_size, out_cols, car, 1, 0);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, car, out_size, out_cols, 0, 1, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(2), std::move(pipe0_vec));
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

  std::stringstream query;
  auto cols = ConstructColumns("", type::TypeId::INTEGER, type::TypeId::BIGINT, num_integers, num_bigints);
  auto tbl_name = ConstructTableName(type::TypeId::INTEGER, type::TypeId::BIGINT, tbl_ints, tbl_bigints, row, car);
  query << "SELECT COUNT(*), " << cols << " FROM " << tbl_name << " GROUP BY " << cols;
  auto equery = OptimizeSqlStatement(query.str(), std::make_unique<optimizer::TrivialCostModel>(), std::move(units));
  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), true);

  state.SetItemsProcessed(row);
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ5_0_AggregateRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenAggregateArguments);

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ5_1_AggregateRunners)(benchmark::State &state) {
  auto num_integers = state.range(0);
  auto tbl_ints = state.range(1);
  auto row = state.range(2);
  auto car = state.range(3);

  int num_iters = 1;
  if (row <= warmup_rows_limit && car <= warmup_rows_limit) {
    num_iters += warmup_iterations_num;
  } else if (rerun_start || skip_large_rows_runs) {
    return;
  }

  auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
  auto tuple_size = int_size * num_integers;
  auto num_col = num_integers;
  auto out_cols = num_col;
  auto out_size = tuple_size;

  auto units = std::make_unique<brain::PipelineOperatingUnits>();
  brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
  brain::ExecutionOperatingUnitFeatureVector pipe1_vec;
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, row, tuple_size, num_col, car, 1, 0);
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::AGGREGATE_BUILD, row, 0, num_col, 1, 1, 0);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::AGGREGATE_ITERATE, 1, out_size, out_cols, 1, 1, 0);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, 1, out_size, out_cols, 0, 1, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(2), std::move(pipe0_vec));
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

  std::stringstream query;
  auto cols = ConstructColumns("", type::TypeId::INTEGER, type::TypeId::BIGINT, num_integers, 0);
  auto tbl_name = ConstructTableName(type::TypeId::INTEGER, type::TypeId::BIGINT, tbl_ints, 0, row, car);

  {
    query << "SELECT ";
    for (int i = 1; i <= num_integers; i++) {
      query << "SUM(" << (type::TypeUtil::TypeIdToString(type::TypeId::INTEGER)) << i << ")";
      if (i != num_integers) query << ", ";
    }

    query << " FROM " << tbl_name;
  }

  auto equery = OptimizeSqlStatement(query.str(), std::make_unique<optimizer::TrivialCostModel>(), std::move(units));
  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), true);

  state.SetItemsProcessed(row);
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ5_1_AggregateRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenAggregateKeylessArguments);

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ9_0_CreateIndexRunners)(benchmark::State &state) {
  auto num_integers = state.range(0);
  auto num_bigints = state.range(1);
  auto tbl_ints = state.range(2);
  auto tbl_bigints = state.range(3);
  auto row = state.range(4);
  auto car = state.range(5);

  if (rerun_start || (row > warmup_rows_limit && skip_large_rows_runs)) {
    return;
  }

  auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
  auto bigint_size = type::TypeUtil::GetTypeSize(type::TypeId::BIGINT);
  auto tuple_size = int_size * num_integers + bigint_size * num_bigints;
  auto num_col = num_integers + num_bigints;

  auto cols = ConstructColumns("", type::TypeId::INTEGER, type::TypeId::BIGINT, num_integers, num_bigints);
  auto tbl_name = ConstructTableName(type::TypeId::INTEGER, type::TypeId::BIGINT, tbl_ints, tbl_bigints, row, car);

  auto units = std::make_unique<brain::PipelineOperatingUnits>();
  brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::CREATE_INDEX, row, tuple_size, num_col, car, 1, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  std::stringstream query;
  query << "CREATE INDEX idx ON " << tbl_name << " (" << cols << ")";
  auto equery = OptimizeSqlStatement(query.str(), std::make_unique<optimizer::TrivialCostModel>(), std::move(units));
  BenchmarkExecQuery(1, equery.first.get(), equery.second.get(), true, &empty_params);

  {
    auto units = std::make_unique<brain::PipelineOperatingUnits>();
    OptimizeSqlStatement("DROP INDEX idx", std::make_unique<optimizer::TrivialCostModel>(), std::move(units));
  }

  InvokeGC();
  state.SetItemsProcessed(row);
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ9_0_CreateIndexRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenCreateIndexArguments);

void InitializeRunnersState() {
  auto db_main_builder = DBMain::Builder()
                             .SetUseGC(true)
                             .SetUseCatalog(true)
                             .SetUseStatsStorage(true)
                             .SetUseMetrics(true)
                             .SetBlockStoreSize(1000000000)
                             .SetBlockStoreReuse(1000000000)
                             .SetRecordBufferSegmentSize(1000000000)
                             .SetRecordBufferSegmentReuse(1000000000)
                             .SetUseExecution(true)
                             .SetUseTrafficCop(true)
                             .SetUseNetwork(true)
                             .SetNetworkPort(terrier::runner::port);

  db_main = db_main_builder.Build().release();

  auto block_store = db_main->GetStorageLayer()->GetBlockStore();
  auto catalog = db_main->GetCatalogLayer()->GetCatalog();
  auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
  db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::EXECUTION_PIPELINE, 0);

  // Create the database
  auto txn = txn_manager->BeginTransaction();
  db_oid = catalog->CreateDatabase(common::ManagedPointer(txn), "test_db", true);

  // Load the database
  auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
  auto exec_settings = MiniRunners::GetExecutionSettings();
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
      db_oid, common::ManagedPointer(txn), nullptr, nullptr, common::ManagedPointer(accessor), exec_settings);

  execution::sql::TableGenerator table_gen(exec_ctx.get(), block_store, accessor->GetDefaultNamespace());
  table_gen.GenerateTestTables(true);
  table_gen.GenerateMiniRunnerIndexTables();

  txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  InvokeGC();

  auto network_layer = terrier::runner::db_main->GetNetworkLayer();
  auto server = network_layer->GetServer();
  server->RunServer();
}

void EndRunnersState() {
  terrier::execution::ExecutionUtil::ShutdownTPL();
  db_main->GetMetricsManager()->Aggregate();
  db_main->GetMetricsManager()->ToCSV();
  // free db main here so we don't need to use the loggers anymore
  delete db_main;
}

}  // namespace terrier::runner

/**
 * Bool for server ready
 */
bool network_queries_ready = false;

/**
 * Bool for network queries finished
 */
bool network_queries_finished = false;

/**
 * Mutex for access to above two booleans
 */
std::mutex network_queries_mutex;

/**
 * CVar for signalling
 */
std::condition_variable network_queries_cv;

using NetworkWorkFunction = std::function<void(pqxx::work *)>;

void RunNetworkQueries(NetworkWorkFunction work) {
  // GC does not run in a background thread!
  {
    std::unique_lock<std::mutex> lk(network_queries_mutex);
    network_queries_cv.wait(lk, [] { return network_queries_ready; });
  }

  std::string conn;
  {
    std::stringstream conn_ss;
    conn_ss << "postgresql://127.0.0.1:" << (terrier::runner::port) << "/test_db";
    conn = conn_ss.str();
  }

  // Execute network queries
  try {
    pqxx::connection c{conn};
    pqxx::work txn{c};

    work(&txn);

    txn.commit();
  } catch (std::exception &e) {
  }

  {
    std::unique_lock<std::mutex> lk(network_queries_mutex);
    network_queries_finished = true;
    network_queries_cv.notify_one();
  }
}

void RunNetworkSequence(NetworkWorkFunction work) {
  terrier::runner::db_main->GetMetricsManager()->Aggregate();
  terrier::runner::db_main->GetMetricsManager()->ToCSV();
  terrier::runner::InvokeGC();

  auto thread = std::thread([=] { RunNetworkQueries(work); });

  {
    std::unique_lock<std::mutex> lk(network_queries_mutex);
    network_queries_ready = true;
    network_queries_cv.notify_one();
    network_queries_cv.wait(lk, [] { return network_queries_finished; });
  }

  terrier::runner::db_main->GetMetricsManager()->Aggregate();
  terrier::runner::db_main->GetMetricsManager()->ToCSV();
  terrier::runner::InvokeGC();

  thread.join();
}

void Shutdown() {
  terrier::runner::db_main->ForceShutdown();
  delete terrier::runner::db_main;
}

void RunBenchmarkSequence(int rerun_counter) {
  // As discussed, certain runners utilize multiple features.
  // In order for the modeller to work correctly, we first need to model
  // the dependent features and then subtract estimations/exact counters
  // from the composite to get an approximation for the target feature.
  std::vector<std::vector<std::string>> filters = {{"SEQ0"},
                                                   {"SEQ1_0", "SEQ1_1"},
                                                   {"SEQ2_0", "SEQ2_1"},
                                                   {"SEQ3"},
                                                   {"SEQ4"},
                                                   {"SEQ5_0", "SEQ5_1"},
                                                   {"SEQ6_0", "SEQ6_1"},
                                                   {"SEQ7_2"},
                                                   {"SEQ8_2"},
                                                   {"SEQ9_0"}};
  std::vector<std::string> titles = {"OUTPUT", "SCANS",  "IDX_SCANS", "SORTS",  "HJ",
                                     "AGGS",   "INSERT", "UPDATE",    "DELETE", "CREATE_INDEX"};

  char buffer[64];
  const char *argv[2];
  argv[0] = "mini_runners";
  argv[1] = buffer;

  auto vm_modes = {terrier::execution::vm::ExecutionMode::Interpret, terrier::execution::vm::ExecutionMode::Compiled};
  for (size_t i = 0; i < filters.size(); i++) {
    for (auto &filter : filters[i]) {
      for (auto mode : vm_modes) {
        terrier::runner::MiniRunners::mode = mode;

        int argc = 2;
        snprintf(buffer, sizeof(buffer), "--benchmark_filter=%s", filter.c_str());
        benchmark::Initialize(&argc, const_cast<char **>(argv));
        benchmark::RunSpecifiedBenchmarks();
        std::this_thread::sleep_for(std::chrono::seconds(2));
        terrier::runner::InvokeGC();
      }
    }

    std::this_thread::sleep_for(std::chrono::seconds(2));
    terrier::runner::db_main->GetMetricsManager()->Aggregate();
    terrier::runner::db_main->GetMetricsManager()->ToCSV();

    if (!terrier::runner::rerun_start) {
      snprintf(buffer, sizeof(buffer), "execution_%s.csv", titles[i].c_str());
    } else {
      snprintf(buffer, sizeof(buffer), "execution_%s_%d.csv", titles[i].c_str(), rerun_counter);
    }

    std::rename("pipeline.csv", buffer);
  }
}

void RunMiniRunners() {
  terrier::runner::rerun_start = false;
  for (int i = 0; i <= terrier::runner::rerun_iterations; i++) {
    terrier::runner::rerun_start = (i != 0);
    RunBenchmarkSequence(i);
  }

  for (int i = 0; i <= terrier::runner::rerun_iterations; i++) {
    RunNetworkSequence(terrier::runner::NetworkQueriesOutputRunners);
  }

  std::rename("pipeline.csv", "execution_NETWORK.csv");

  // Do post-processing
  std::vector<std::string> titles = {"OUTPUT", "SCANS",  "IDX_SCANS", "SORTS",  "HJ",
                                     "AGGS",   "INSERT", "UPDATE",    "DELETE", "CREATE_INDEX"};
  std::vector<std::string> adjusts = {"0", "1_0", "1_1", "2", "3", "4", "5_0", "5_1", "5_2", "6"};
  for (size_t t = 0; t < titles.size(); t++) {
    auto &title = titles[t];
    char target[64];
    snprintf(target, sizeof(target), "execution_%s.csv", title.c_str());

    for (int i = 1; i <= terrier::runner::rerun_iterations; i++) {
      char source[64];
      snprintf(source, sizeof(target), "execution_%s_%d.csv", title.c_str(), i);

      std::string csv_line;
      std::ofstream of(target, std::ios_base::binary | std::ios_base::app);
      std::ifstream is(source, std::ios_base::binary);
      std::getline(is, csv_line);

      // Concat rest of data file
      of.seekp(0, std::ios_base::end);
      of << is.rdbuf();

      // Delete the _%d file
      std::remove(source);
    }

    char adjust[64];
    snprintf(adjust, sizeof(adjust), "execution_SEQ%s.csv", adjusts[t].c_str());
    std::rename(target, adjust);
  }

  {
    std::ifstream ifile("execution_NETWORK.csv");
    std::ofstream ofile("execution_SEQ0.csv", std::ios::app);

    std::string dummy;
    std::getline(ifile, dummy);
    ofile << ifile.rdbuf();
  }
  std::remove("execution_NETWORK.csv");
}

struct Arg {
  const char *match_;
  bool found_;
  const char *value_;
  int int_value_;
};

int main(int argc, char **argv) {
  Arg port_info{"--port=", false};
  Arg filter_info{"--benchmark_filter=", false, "*"};
  Arg skip_large_rows_runs_info{"--skip_large_rows_runs=", false};
  Arg warm_num_info{"--warm_num=", false};
  Arg rerun_info{"--rerun=", false};
  Arg updel_info{"--updel_limit=", false};
  Arg warm_limit_info{"--warm_limit=", false};
  Arg compiled_info{"--compiled=", false};
  Arg gen_test_data{"--gen_test=", false};
  Arg create_index_small_data{"--create_index_small_limit=", false};
  Arg create_index_car_data{"--create_index_large_car_num=", false};
  Arg *args[] = {&port_info,
                 &filter_info,
                 &skip_large_rows_runs_info,
                 &warm_num_info,
                 &rerun_info,
                 &updel_info,
                 &warm_limit_info,
                 &compiled_info,
                 &gen_test_data,
                 &create_index_small_data,
                 &create_index_car_data};

  for (int i = 0; i < argc; i++) {
    for (auto *arg : args) {
      if (strstr(argv[i], arg->match_) != nullptr) {
        arg->found_ = true;
        arg->value_ = strstr(argv[i], "=") + 1;
        arg->int_value_ = atoi(arg->value_);
      }
    }
  }

  if (port_info.found_) terrier::runner::port = port_info.int_value_;
  if (skip_large_rows_runs_info.found_) terrier::runner::skip_large_rows_runs = true;
  if (warm_num_info.found_) terrier::runner::warmup_iterations_num = warm_num_info.int_value_;
  if (rerun_info.found_) terrier::runner::rerun_iterations = rerun_info.int_value_;
  if (updel_info.found_) terrier::runner::updel_limit = updel_info.int_value_;
  if (warm_limit_info.found_) terrier::runner::warmup_rows_limit = warm_limit_info.int_value_;
  if (create_index_small_data.found_) terrier::runner::create_index_small_limit = create_index_small_data.int_value_;
  if (create_index_car_data.found_)
    terrier::runner::create_index_large_cardinality_num = create_index_car_data.int_value_;

  terrier::LoggersUtil::Initialize();
  SETTINGS_LOG_INFO("Starting mini-runners with this parameter set:");
  SETTINGS_LOG_INFO("Port ({}): {}", port_info.match_, terrier::runner::port);
  SETTINGS_LOG_INFO("Skip Large Rows ({}): {}", skip_large_rows_runs_info.match_,
                    terrier::runner::skip_large_rows_runs);
  SETTINGS_LOG_INFO("Warmup Iterations ({}): {}", warm_num_info.match_, terrier::runner::warmup_iterations_num);
  SETTINGS_LOG_INFO("Rerun Iterations ({}): {}", rerun_info.match_, terrier::runner::rerun_iterations);
  SETTINGS_LOG_INFO("Update/Delete Index Limit ({}): {}", updel_info.match_, terrier::runner::updel_limit);
  SETTINGS_LOG_INFO("Create Index Small Build Limit ({}): {}", create_index_small_data.match_,
                    terrier::runner::create_index_small_limit);
  SETTINGS_LOG_INFO("Create Index Large Cardinality Number Vary ({}): {}", create_index_car_data.match_,
                    terrier::runner::create_index_large_cardinality_num);
  SETTINGS_LOG_INFO("Warmup Rows Limit ({}): {}", warm_limit_info.match_, terrier::runner::warmup_rows_limit);
  SETTINGS_LOG_INFO("Filter ({}): {}", filter_info.match_, filter_info.value_);
  SETTINGS_LOG_INFO("Compiled ({}): {}", compiled_info.match_, compiled_info.found_);
  SETTINGS_LOG_INFO("Generate Test Data ({}): {}", gen_test_data.match_, gen_test_data.found_);

  // Benchmark Config Environment Variables
  // Check whether we are being passed environment variables to override configuration parameter
  // for this benchmark run.
  const char *env_num_threads = std::getenv(terrier::ENV_NUM_THREADS);
  if (env_num_threads != nullptr) terrier::BenchmarkConfig::num_threads = atoi(env_num_threads);

  const char *env_logfile_path = std::getenv(terrier::ENV_LOGFILE_PATH);
  if (env_logfile_path != nullptr) terrier::BenchmarkConfig::logfile_path = std::string_view(env_logfile_path);

  terrier::runner::InitializeRunnersState();
  if (gen_test_data.found_) {
    RunNetworkSequence(terrier::runner::NetworkQueriesCreateIndexRunners);
    std::rename("pipeline.csv", "execution_TEST_DATA.csv");
  } else {
    if (filter_info.found_) {
      if (compiled_info.found_) {
        terrier::runner::MiniRunners::mode = terrier::execution::vm::ExecutionMode::Compiled;
      }

      // Pass straight through to gbenchmark
      benchmark::Initialize(&argc, argv);
      benchmark::RunSpecifiedBenchmarks();
      terrier::runner::EndRunnersState();
    } else {
      RunMiniRunners();
      Shutdown();
    }
  }

  terrier::LoggersUtil::ShutDown();

  return 0;
}
