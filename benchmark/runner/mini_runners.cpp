#include <common/macros.h>
#include <gflags/gflags.h>
#include <pqxx/pqxx>

#include <cstdio>
#include <functional>
#include <random>
#include <utility>

#include "benchmark/benchmark.h"
#include "benchmark_util/benchmark_config.h"
#include "binder/bind_node_visitor.h"
#include "brain/brain_defs.h"
#include "brain/operating_unit.h"
#include "common/scoped_timer.h"
#include "execution/executable_query.h"
#include "execution/execution_util.h"
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
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "traffic_cop/traffic_cop_util.h"

namespace terrier::runner {

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
 * Limit on num_rows for which queries need warming up
 */
int64_t warmup_rows_limit{1000};

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
  std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    500,    1000,
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
  std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    500,    1000,
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
  std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    500,    1000,
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
  std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    500,    1000,
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
 * 1 - # decimals to scan
 * 2 - integers of table
 * 3 - decimals of table
 * 4 - Number of rows
 * 5 - Cardinality
 */
static void GenJoinSelfArguments(benchmark::internal::Benchmark *b) {
  auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
  auto types = {type::TypeId::INTEGER};
  std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    500,    1000,
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
  std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    500,    1000,
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
 * Arg: <0, 1, 2>
 * 0 - Key Sizes
 * 1 - Idx Size
 * 2 - Lookup size
 */
static void GenIdxScanArguments(benchmark::internal::Benchmark *b) {
  auto types = {type::TypeId::INTEGER};
  auto key_sizes = {1, 2, 4, 8, 15};
  auto idx_sizes = {1, 10, 100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 500000, 1000000};
  std::vector<int64_t> lookup_sizes = {1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
  for (auto type : types) {
    for (auto key_size : key_sizes) {
      for (auto idx_size : idx_sizes) {
        for (auto lookup_size : lookup_sizes) {
          if (lookup_size <= idx_size) {
            b->Args({static_cast<int64_t>(type), key_size, idx_size, lookup_size});
          }
        }
      }
    }
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

/**
 * Arg <0, 1, 2, 3, 4, 5, 6>
 * 0 - Number of ints
 * 1 - Number of decimals
 * 2 - Table number of ints
 * 3 - Table number of decimals
 * 4 - Row
 * 5 - Cardinality
 * 6 - Not Index
 */
static void GenUpdateDeleteArguments(benchmark::internal::Benchmark *b) {
  auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
  auto types = {type::TypeId::INTEGER, type::TypeId::DECIMAL};
  std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    500,    1000,
                                   2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000};
  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : row_nums) {
        int64_t car = 1;
        std::vector<int64_t> cars;
        while (car < row) {
          cars.push_back(car);
          car *= 2;
        }
        cars.push_back(row);

        for (auto car : cars) {
          if (type == type::TypeId::INTEGER)
            b->Args({col, 0, 15, 0, row, car, 0});
          else if (type == type::TypeId::DECIMAL)
            b->Args({0, col, 0, 15, row, car, 0});
        }
      }
    }

    // No-op to perform garbage collection
    b->Args({0, 0, 0, 0, 0, 0, 0});
  }
}

static void GenUpdateDeleteMixedArguments(benchmark::internal::Benchmark *b) {
  std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    500,    1000,
                                   2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000};
  std::vector<std::vector<int64_t>> args;
  GENERATE_MIXED_ARGUMENTS(args, true);
  for (const auto &arg : args) {
    std::vector<int64_t> arg_mut = arg;
    arg_mut.push_back(0);
    b->Args(arg_mut);
  }
}

static void GenUpdateDeleteIndexArguments(benchmark::internal::Benchmark *b) {
  std::vector<uint32_t> idx_key = {1, 2, 4, 8, 15};
  std::vector<uint32_t> row_nums = {1, 10, 100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 500000, 1000000};
  for (auto idx_key_size : idx_key) {
    for (auto row_num : row_nums) {
      int64_t lookup_size = 1;
      std::vector<int64_t> lookups;
      while (lookup_size < row_num && lookup_size < 1024) {
        lookups.push_back(lookup_size);
        lookup_size *= 2;
      }

      for (auto lookup : lookups) {
        b->Args({idx_key_size, 0, 15, 0, row_num, lookup, 1});
      }
    }
  }
}

class MiniRunners : public benchmark::Fixture {
 public:
  static execution::query_id_t query_id;
  static execution::vm::ExecutionMode mode;
  const uint64_t optimizer_timeout_ = 1000000;

  static std::unique_ptr<planner::AbstractPlanNode> PassthroughPlanCorrect(
      UNUSED_ATTRIBUTE common::ManagedPointer<transaction::TransactionContext> txn,
      std::unique_ptr<planner::AbstractPlanNode> plan) {
    return plan;
  }

  void ExecuteSeqScan(benchmark::State *state);
  void ExecuteInsert(benchmark::State *state);
  void ExecuteUpdate(benchmark::State *state);
  void ExecuteDelete(benchmark::State *state);

  std::string ConstructIndexScanPredicate(int64_t key_num, int64_t num_rows, int64_t lookup_size) {
    std::mt19937 generator{};
    auto low_key = std::uniform_int_distribution(static_cast<uint32_t>(0),
                                                 static_cast<uint32_t>(num_rows - lookup_size))(generator);
    auto high_key = low_key + lookup_size - 1;

    std::stringstream predicatess;
    for (auto j = 1; j <= key_num; j++) {
      if (lookup_size == 1) {
        predicatess << "col" << j << " = " << low_key;
      } else {
        predicatess << "col" << j << " >= " << low_key << " AND "
                    << "col" << j << " <= " << high_key;
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

  std::unique_ptr<planner::AbstractPlanNode> IndexScanCorrector(
      size_t num_keys, common::ManagedPointer<transaction::TransactionContext> txn,
      std::unique_ptr<planner::AbstractPlanNode> plan) {
    if (plan->GetPlanNodeType() != planner::PlanNodeType::INDEXSCAN) throw "Expected IndexScan";

    auto *idx_scan = reinterpret_cast<planner::IndexScanPlanNode *>(plan.get());
    if (idx_scan->GetLoIndexColumns().size() != num_keys) throw "Number keys mismatch";

    return plan;
  }

  std::unique_ptr<planner::AbstractPlanNode> ChildIndexScanCorrector(
      common::ManagedPointer<transaction::TransactionContext> txn, std::unique_ptr<planner::AbstractPlanNode> plan) {
    if (plan->GetChild(0)->GetPlanNodeType() != planner::PlanNodeType::INDEXSCAN) throw "Expected IndexScan";

    return plan;
  }

  std::unique_ptr<planner::AbstractPlanNode> JoinNonSelfCorrector(
      std::string build_tbl, common::ManagedPointer<transaction::TransactionContext> txn,
      std::unique_ptr<planner::AbstractPlanNode> plan) {
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid);
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

  std::pair<std::unique_ptr<execution::ExecutableQuery>, std::unique_ptr<planner::OutputSchema>> OptimizeSqlStatement(
      const std::string &query, std::unique_ptr<optimizer::AbstractCostModel> cost_model,
      std::unique_ptr<brain::PipelineOperatingUnits> pipeline_units,
      const std::function<std::unique_ptr<planner::AbstractPlanNode>(
          common::ManagedPointer<transaction::TransactionContext>, std::unique_ptr<planner::AbstractPlanNode>)>
          &corrector = std::function<std::unique_ptr<planner::AbstractPlanNode>(
              common::ManagedPointer<transaction::TransactionContext>, std::unique_ptr<planner::AbstractPlanNode>)>(
              PassthroughPlanCorrect)) {
    auto txn = txn_manager_->BeginTransaction();
    auto stmt_list = parser::PostgresParser::BuildParseTree(query);

    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid);
    auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid);
    binder.BindNameToNode(common::ManagedPointer(stmt_list), nullptr, nullptr);

    auto out_plan = trafficcop::TrafficCopUtil::Optimize(
        common::ManagedPointer(txn), common::ManagedPointer(accessor), common::ManagedPointer(stmt_list), db_oid,
        db_main->GetStatsStorage(), std::move(cost_model), optimizer_timeout_);

    out_plan = corrector(common::ManagedPointer(txn), std::move(out_plan));

    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
        db_oid, common::ManagedPointer(txn), execution::exec::NoOpResultConsumer(), out_plan->GetOutputSchema().Get(),
        common::ManagedPointer(accessor));

    execution::ExecutableQuery::query_identifier.store(MiniRunners::query_id++);
    auto exec_query = std::make_unique<execution::ExecutableQuery>(common::ManagedPointer(out_plan),
                                                                   common::ManagedPointer(exec_ctx));
    exec_query->SetPipelineOperatingUnits(std::move(pipeline_units));

    auto ret_val = std::make_pair(std::move(exec_query), out_plan->GetOutputSchema()->Copy());
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    return ret_val;
  }

  void BenchmarkExecQuery(int64_t num_iters, execution::ExecutableQuery *exec_query, planner::OutputSchema *out_schema,
                          bool commit) {
    for (auto i = 0; i < num_iters; i++) {
      if (i == num_iters - 1) {
        metrics_manager_->RegisterThread();
      }

      auto txn = txn_manager_->BeginTransaction();
      auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid);

      auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(db_oid, common::ManagedPointer(txn),
                                                                          execution::exec::NoOpResultConsumer(),
                                                                          out_schema, common::ManagedPointer(accessor));
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
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid);
    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(db_oid, common::ManagedPointer(txn), nullptr,
                                                                        nullptr, common::ManagedPointer(accessor));
    exec_ctx->SetExecutionMode(static_cast<uint8_t>(mode));

    brain::PipelineOperatingUnits units;
    brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
    exec_ctx->SetPipelineOperatingUnits(common::ManagedPointer(&units));
    pipe0_vec.emplace_back(type, num_elem, 4, 1, num_elem, 1);
    units.RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

    switch (type) {
      case brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS: {
        exec_ctx->StartResourceTracker(metrics::MetricsComponent::EXECUTION_PIPELINE);
        uint32_t ret = __uint32_t_PLUS(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(0));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY: {
        exec_ctx->StartResourceTracker(metrics::MetricsComponent::EXECUTION_PIPELINE);
        uint32_t ret = __uint32_t_MULTIPLY(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(0));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_INTEGER_DIVIDE: {
        exec_ctx->StartResourceTracker(metrics::MetricsComponent::EXECUTION_PIPELINE);
        uint32_t ret = __uint32_t_DIVIDE(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(0));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE: {
        exec_ctx->StartResourceTracker(metrics::MetricsComponent::EXECUTION_PIPELINE);
        uint32_t ret = __uint32_t_GEQ(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(0));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_DECIMAL_PLUS_OR_MINUS: {
        exec_ctx->StartResourceTracker(metrics::MetricsComponent::EXECUTION_PIPELINE);
        double ret = __double_PLUS(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(0));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_DECIMAL_MULTIPLY: {
        exec_ctx->StartResourceTracker(metrics::MetricsComponent::EXECUTION_PIPELINE);
        double ret = __double_MULTIPLY(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(0));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_DECIMAL_DIVIDE: {
        exec_ctx->StartResourceTracker(metrics::MetricsComponent::EXECUTION_PIPELINE);
        double ret = __double_DIVIDE(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(0));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_DECIMAL_COMPARE: {
        exec_ctx->StartResourceTracker(metrics::MetricsComponent::EXECUTION_PIPELINE);
        double ret = __double_GEQ(num_elem);
        exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(0));
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

void NetworkQueries_OutputRunners(pqxx::work *txn) {
  std::ostream null{0};
  auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
  auto types = {type::TypeId::INTEGER, type::TypeId::DECIMAL};
  std::vector<int64_t> row_nums = {1, 3, 5, 7, 10, 50, 100, 500, 1000, 2000, 5000, 10000};

  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : row_nums) {
        std::stringstream query_ss;
        std::string type_s = (type == type::TypeId::INTEGER) ? "int" : "real";

        query_ss << "SELECT nprunnersemit" << type_s << "(" << row << "," << col << ",";
        if (type == type::TypeId::INTEGER)
          query_ss << col << ",0)";
        else
          query_ss << "0," << col << ")";

        if (col > 1) {
          query_ss << ",";
          for (int i = 1; i < col; i++) {
            query_ss << "nprunnersdummy" << type_s << "()";
            if (i != col - 1) {
              query_ss << ",";
            }
          }
        }

        // Execute query
        pqxx::result r{txn->exec(query_ss.str())};

        // Get all the results
        for (auto row : r) {
          for (auto i = 0; i < col; i++) {
            null << row[i];
          }
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

  metrics_manager_->RegisterThread();

  std::stringstream output;
  output << "struct Output {\n";
  for (auto i = 0; i < num_integers; i++) output << "col" << i << " : Integer\n";
  for (auto i = num_integers; i < num_col; i++) output << "col" << i << " : Real\n";
  output << "}\n";

  output << "struct State {\ncount : int64\n}\n";
  output << "fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {\n}\n";
  output << "fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {\n}\n";

  // pipeline
  output << "fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {\n";
  output << "\t@execCtxStartResourceTracker(execCtx, 4)\n";
  if (num_col > 0) {
    output << "\tvar out: *Output\n";
    output << "\tfor(var it = 0; it < " << row_num << "; it = it + 1) {\n";
    output << "\t\tout = @ptrCast(*Output, @outputAlloc(execCtx))\n";
    output << "\t}\n";
    output << "\t@outputFinalize(execCtx)\n";
  }
  output << "\t@execCtxEndPipelineTracker(execCtx, 0, 0)\n";
  output << "}\n";

  // main
  output << "fun main (execCtx: *ExecutionContext) -> int64 {\n";
  output << "\tvar state: State\n";
  output << "\tsetUpState(execCtx, &state)\n";
  output << "\tpipeline1(execCtx, &state)\n";
  output << "\tteardownState(execCtx, &state)\n";
  output << "\treturn 0\n";
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
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid);
  auto schema = std::make_unique<planner::OutputSchema>(std::move(cols));

  execution::ExecutableQuery::query_identifier.store(MiniRunners::query_id++);
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(db_oid, common::ManagedPointer(txn),
                                                                      execution::exec::NoOpResultConsumer(),
                                                                      schema.get(), common::ManagedPointer(accessor));

  auto exec_query = execution::ExecutableQuery(output.str(), common::ManagedPointer(exec_ctx), false);

  auto units = std::make_unique<brain::PipelineOperatingUnits>();
  brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, row_num, tuple_size, num_col, 0, 1);
  units->RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));
  exec_query.SetPipelineOperatingUnits(std::move(units));
  exec_query.Run(common::ManagedPointer(exec_ctx), mode);

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  metrics_manager_->Aggregate();
  metrics_manager_->UnregisterThread();
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
  }

  auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
  auto decimal_size = type::TypeUtil::GetTypeSize(type::TypeId::DECIMAL);
  auto tuple_size = int_size * num_integers + decimal_size * num_decimals;
  auto num_col = num_integers + num_decimals;

  auto units = std::make_unique<brain::PipelineOperatingUnits>();
  brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, row, tuple_size, num_col, car, 1);
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, row, tuple_size, num_col, 0, 1);
  units->RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

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
BENCHMARK_DEFINE_F(MiniRunners, SEQ1_2_IndexScanRunners)(benchmark::State &state) {
  auto type = static_cast<type::TypeId>(state.range(0));
  auto key_num = state.range(1);
  auto num_rows = state.range(2);
  auto lookup_size = state.range(3);

  int num_iters = 1;
  if (lookup_size <= warmup_rows_limit) {
    num_iters += warmup_iterations_num;
  }

  auto type_size = type::TypeUtil::GetTypeSize(type);
  auto tuple_size = type_size * key_num;

  auto units = std::make_unique<brain::PipelineOperatingUnits>();
  brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, lookup_size, tuple_size, key_num, 0, 1);
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::IDX_SCAN, num_rows, tuple_size, key_num, lookup_size, 1);
  units->RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

  std::string cols;
  {
    std::stringstream colss;
    for (auto j = 1; j <= key_num; j++) {
      colss << "col" << j;
      if (j != key_num) colss << ", ";
    }
    cols = colss.str();
  }

  std::string predicate = ConstructIndexScanPredicate(key_num, num_rows, lookup_size);

  std::stringstream query;
  auto table_name = execution::sql::TableGenerator::GenerateTableIndexName(type, num_rows);
  query << "SELECT " << cols << " FROM  " << table_name << " WHERE " << predicate;
  auto f = std::bind(&MiniRunners::IndexScanCorrector, this, key_num, std::placeholders::_1, std::placeholders::_2);
  auto equery = OptimizeSqlStatement(query.str(), std::make_unique<optimizer::TrivialCostModel>(), std::move(units), f);
  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), true);

  state.SetItemsProcessed(state.range(2));
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ1_2_IndexScanRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenIdxScanArguments);

void MiniRunners::ExecuteInsert(benchmark::State *state) {
  auto num_ints = state->range(0);
  auto num_decimals = state->range(1);
  auto num_cols = state->range(2);
  auto num_rows = state->range(3);

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
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid);
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
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::INSERT, num_rows, tuple_size, num_cols, num_rows, 1);
  units->RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

  auto equery = OptimizeSqlStatement(query, std::make_unique<optimizer::TrivialCostModel>(), std::move(units));
  BenchmarkExecQuery(1, equery.first.get(), equery.second.get(), true);

  // Drop the table
  {
    auto txn = txn_manager_->BeginTransaction();
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid);
    accessor->DropTable(tbl_oid);
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

  InvokeGC();
  state->SetItemsProcessed(num_rows);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ5_0_InsertRunners)(benchmark::State &state) { ExecuteInsert(&state); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ5_1_InsertRunners)(benchmark::State &state) { ExecuteInsert(&state); }

BENCHMARK_REGISTER_F(MiniRunners, SEQ5_0_InsertRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenInsertArguments);

BENCHMARK_REGISTER_F(MiniRunners, SEQ5_1_InsertRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenInsertMixedArguments);

void MiniRunners::ExecuteUpdate(benchmark::State *state) {
  auto num_integers = state->range(0);
  auto num_decimals = state->range(1);
  auto tbl_ints = state->range(2);
  auto tbl_decimals = state->range(3);
  auto row = state->range(4);
  auto car = state->range(5);
  auto is_idx = state->range(6);

  if (row == 0) {
    state->SetItemsProcessed(row);
    InvokeGC();
    return;
  }

  int num_iters = 1;
  if (((is_idx != 0) ? car : row) <= warmup_rows_limit) {
    num_iters += warmup_iterations_num;
  }

  std::string tbl;
  if (is_idx != 0) {
    tbl = execution::sql::TableGenerator::GenerateTableIndexName(type::TypeId::INTEGER, row);
  } else {
    tbl = ConstructTableName(type::TypeId::INTEGER, type::TypeId::DECIMAL, tbl_ints, tbl_decimals, row, car);
  }

  // UPDATE [] SET [col] = random integer()
  // This does not force a read from the underlying tuple more than getting the slot.
  // Arguably, this approach has the least amount of "SEQ_SCAN" overhead and measures:
  // - Iterating over entire table for the slot
  // - Cost of "merging" updates with the undo/redos
  std::stringstream query;
  query << "UPDATE " << tbl << " SET ";

  auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
  auto decimal_size = type::TypeUtil::GetTypeSize(type::TypeId::DECIMAL);
  auto tuple_size = int_size * num_integers + decimal_size * num_decimals;
  auto num_col = num_integers + num_decimals;
  std::vector<catalog::Schema::Column> cols;
  std::mt19937 generator{};
  std::uniform_int_distribution<int> distribution(0, INT_MAX);
  for (auto j = 1; j <= num_integers; j++) {
    if (is_idx != 0) {
      query << "col" << j << " = " << distribution(generator);
    } else {
      query << type::TypeUtil::TypeIdToString(type::TypeId::INTEGER) << j << " = " << distribution(generator);
    }

    if (j != num_integers || num_decimals != 0) query << ", ";
  }

  for (auto j = 1; j <= num_decimals; j++) {
    query << type::TypeUtil::TypeIdToString(type::TypeId::DECIMAL) << j << " = " << distribution(generator);
    if (j != num_decimals) query << ", ";
  }

  std::pair<std::unique_ptr<execution::ExecutableQuery>, std::unique_ptr<planner::OutputSchema>> equery;
  auto cost = std::make_unique<optimizer::TrivialCostModel>();
  if (is_idx == 0) {
    auto units = std::make_unique<brain::PipelineOperatingUnits>();
    brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::UPDATE, row, tuple_size, num_col, car, 1);
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, row, 4, 1, car, 1);
    units->RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

    equery = OptimizeSqlStatement(query.str(), std::move(cost), std::move(units));
  } else {
    auto units = std::make_unique<brain::PipelineOperatingUnits>();
    brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::UPDATE, car, tuple_size, num_col, car, 1);
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::IDX_SCAN, row, tuple_size, num_col, car, 1);
    units->RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

    std::string predicate = ConstructIndexScanPredicate(num_col, row, car);
    query << " WHERE " << predicate;

    auto f = std::bind(&MiniRunners::ChildIndexScanCorrector, this, std::placeholders::_1, std::placeholders::_2);
    equery = OptimizeSqlStatement(query.str(), std::move(cost), std::move(units), f);
  }

  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), false);
  state->SetItemsProcessed(row);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ5_0_UpdateRunners)(benchmark::State &state) { ExecuteUpdate(&state); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ5_1_UpdateRunners)(benchmark::State &state) { ExecuteUpdate(&state); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ5_2_UpdateRunners)(benchmark::State &state) { ExecuteUpdate(&state); }

BENCHMARK_REGISTER_F(MiniRunners, SEQ5_0_UpdateRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenUpdateDeleteArguments);

BENCHMARK_REGISTER_F(MiniRunners, SEQ5_1_UpdateRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenUpdateDeleteMixedArguments);

BENCHMARK_REGISTER_F(MiniRunners, SEQ5_2_UpdateRunners)
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
  auto is_idx = state->range(6);

  if (row == 0) {
    state->SetItemsProcessed(row);
    InvokeGC();
    return;
  }

  int num_iters = 1;
  if (((is_idx != 0) ? car : row) <= warmup_rows_limit) {
    num_iters += warmup_iterations_num;
  }

  auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
  auto decimal_size = type::TypeUtil::GetTypeSize(type::TypeId::DECIMAL);
  auto tuple_size = int_size * num_integers + decimal_size * num_decimals;
  auto num_col = num_integers + num_decimals;

  std::stringstream query;
  std::pair<std::unique_ptr<execution::ExecutableQuery>, std::unique_ptr<planner::OutputSchema>> equery;
  auto cost = std::make_unique<optimizer::TrivialCostModel>();
  if (is_idx == 0) {
    auto units = std::make_unique<brain::PipelineOperatingUnits>();
    brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::DELETE, row, tuple_size, num_col, car, 1);
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, row, 4, 1, car, 1);
    units->RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

    query << "DELETE FROM "
          << ConstructTableName(type::TypeId::INTEGER, type::TypeId::DECIMAL, tbl_ints, tbl_decimals, row, car);

    equery = OptimizeSqlStatement(query.str(), std::move(cost), std::move(units));
  } else {
    auto tbl_col = tbl_ints + tbl_decimals;
    auto tbl_size = tbl_ints * int_size + tbl_decimals * decimal_size;

    auto units = std::make_unique<brain::PipelineOperatingUnits>();
    brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::DELETE, car, tbl_size, tbl_col, car, 1);
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::IDX_SCAN, row, tuple_size, num_col, car, 1);
    units->RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

    std::string predicate = ConstructIndexScanPredicate(num_col, row, car);
    query << "DELETE FROM " << execution::sql::TableGenerator::GenerateTableIndexName(type::TypeId::INTEGER, row)
          << " WHERE " << predicate;

    auto f = std::bind(&MiniRunners::ChildIndexScanCorrector, this, std::placeholders::_1, std::placeholders::_2);
    equery = OptimizeSqlStatement(query.str(), std::move(cost), std::move(units), f);
  }

  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), false);
  state->SetItemsProcessed(row);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ5_0_DeleteRunners)(benchmark::State &state) { ExecuteDelete(&state); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ5_1_DeleteRunners)(benchmark::State &state) { ExecuteDelete(&state); }

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ5_2_DeleteRunners)(benchmark::State &state) { ExecuteDelete(&state); }

BENCHMARK_REGISTER_F(MiniRunners, SEQ5_0_DeleteRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenUpdateDeleteArguments);

BENCHMARK_REGISTER_F(MiniRunners, SEQ5_1_DeleteRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenUpdateDeleteMixedArguments);

BENCHMARK_REGISTER_F(MiniRunners, SEQ5_2_DeleteRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenUpdateDeleteIndexArguments);

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ2_SortRunners)(benchmark::State &state) {
  auto num_integers = state.range(0);
  auto num_decimals = state.range(1);
  auto tbl_ints = state.range(2);
  auto tbl_decimals = state.range(3);
  auto row = state.range(4);
  auto car = state.range(5);

  int num_iters = 1;
  if (row <= warmup_rows_limit) {
    num_iters += warmup_iterations_num;
  }

  auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
  auto decimal_size = type::TypeUtil::GetTypeSize(type::TypeId::DECIMAL);
  auto tuple_size = int_size * num_integers + decimal_size * num_decimals;
  auto num_col = num_integers + num_decimals;

  auto units = std::make_unique<brain::PipelineOperatingUnits>();
  brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
  brain::ExecutionOperatingUnitFeatureVector pipe1_vec;
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, row, tuple_size, num_col, car, 1);
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SORT_BUILD, row, tuple_size, num_col, car, 1);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::SORT_ITERATE, row, tuple_size, num_col, car, 1);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, row, tuple_size, num_col, 0, 1);
  units->RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

  std::stringstream query;
  auto cols = ConstructColumns("", type::TypeId::INTEGER, type::TypeId::DECIMAL, num_integers, num_decimals);
  auto tbl_name = ConstructTableName(type::TypeId::INTEGER, type::TypeId::DECIMAL, tbl_ints, tbl_decimals, row, car);
  query << "SELECT " << (cols) << " FROM " << tbl_name << " ORDER BY " << (cols);
  auto equery = OptimizeSqlStatement(query.str(), std::make_unique<optimizer::TrivialCostModel>(), std::move(units));
  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), true);
  state.SetItemsProcessed(row);
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ2_SortRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenSortArguments);

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ3_HashJoinSelfRunners)(benchmark::State &state) {
  auto num_integers = state.range(0);
  auto num_bigints = state.range(1);
  auto tbl_ints = state.range(2);
  auto tbl_bigints = state.range(3);
  auto row = state.range(4);
  auto car = state.range(5);

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
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, row, tuple_size, num_col, car, 1);
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::HASHJOIN_BUILD, row, tuple_size, num_col, car, 1);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, row, tuple_size, num_col, car, 1);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::HASHJOIN_PROBE, row, tuple_size, num_col, hj_output, 1);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, hj_output, tuple_size, num_col, 0, 1);
  units->RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));
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

BENCHMARK_REGISTER_F(MiniRunners, SEQ3_HashJoinSelfRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenJoinSelfArguments);

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ3_HashJoinNonSelfRunners)(benchmark::State &state) {
  auto num_integers = state.range(0);
  auto num_bigints = state.range(1);
  auto tbl_ints = state.range(2);
  auto tbl_bigints = state.range(3);
  auto build_row = state.range(4);
  auto build_car = state.range(5);
  auto probe_row = state.range(6);
  auto probe_car = state.range(7);
  auto matched_car = state.range(8);

  auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
  auto bigint_size = type::TypeUtil::GetTypeSize(type::TypeId::BIGINT);
  auto tuple_size = int_size * num_integers + bigint_size * num_bigints;
  auto num_col = num_integers + num_bigints;

  auto units = std::make_unique<brain::PipelineOperatingUnits>();
  brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
  brain::ExecutionOperatingUnitFeatureVector pipe1_vec;
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, build_row, tuple_size, num_col, build_car, 1);
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::HASHJOIN_BUILD, build_row, tuple_size, num_col, build_car,
                         1);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, probe_row, tuple_size, num_col, probe_car, 1);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::HASHJOIN_PROBE, probe_row, tuple_size, num_col, matched_car,
                         1);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, matched_car, tuple_size, num_col, 0, 1);
  units->RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

  auto build_tbl =
      ConstructTableName(type::TypeId::INTEGER, type::TypeId::BIGINT, tbl_ints, tbl_bigints, build_row, build_car);
  auto probe_tbl =
      ConstructTableName(type::TypeId::INTEGER, type::TypeId::BIGINT, tbl_ints, tbl_bigints, probe_row, probe_car);

  std::stringstream query;
  query << "SELECT " << ConstructColumns("b.", type::TypeId::INTEGER, type::TypeId::BIGINT, num_integers, num_bigints);
  query << " FROM " << build_tbl << ", " << probe_tbl << " as b WHERE ";
  query << ConstructPredicate(build_tbl, "b", type::TypeId::INTEGER, type::TypeId::BIGINT, num_integers, num_bigints);

  auto f = std::bind(&MiniRunners::JoinNonSelfCorrector, this, build_tbl, std::placeholders::_1, std::placeholders::_2);
  auto cost = std::make_unique<optimizer::ForcedCostModel>(true);
  auto equery = OptimizeSqlStatement(query.str(), std::move(cost), std::move(units), f);
  BenchmarkExecQuery(1, equery.first.get(), equery.second.get(), true);
  state.SetItemsProcessed(matched_car);
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ3_HashJoinNonSelfRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenJoinNonSelfArguments);

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ4_AggregateRunners)(benchmark::State &state) {
  auto num_integers = state.range(0);
  auto num_bigints = state.range(1);
  auto tbl_ints = state.range(2);
  auto tbl_bigints = state.range(3);
  auto row = state.range(4);
  auto car = state.range(5);

  int num_iters = 1;
  if (row <= warmup_rows_limit && car <= warmup_rows_limit) {
    num_iters += warmup_iterations_num;
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
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, row, tuple_size, num_col, car, 1);
  pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::AGGREGATE_BUILD, row, tuple_size, num_col, car, 1);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::AGGREGATE_ITERATE, car, out_size, out_cols, car, 1);
  pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, car, out_size, out_cols, 0, 1);
  units->RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

  std::stringstream query;
  auto cols = ConstructColumns("", type::TypeId::INTEGER, type::TypeId::BIGINT, num_integers, num_bigints);
  auto tbl_name = ConstructTableName(type::TypeId::INTEGER, type::TypeId::BIGINT, tbl_ints, tbl_bigints, row, car);
  query << "SELECT COUNT(*), " << cols << " FROM " << tbl_name << " GROUP BY " << cols;
  auto equery = OptimizeSqlStatement(query.str(), std::make_unique<optimizer::TrivialCostModel>(), std::move(units));
  BenchmarkExecQuery(num_iters, equery.first.get(), equery.second.get(), true);

  state.SetItemsProcessed(row);
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ4_AggregateRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenAggregateArguments);

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
  auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid);
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(db_oid, common::ManagedPointer(txn), nullptr,
                                                                      nullptr, common::ManagedPointer(accessor));

  execution::sql::TableGenerator table_gen(exec_ctx.get(), block_store, accessor->GetDefaultNamespace());
  table_gen.GenerateTestTables(true);
  table_gen.GenerateMiniRunnerIndexes();

  txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
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

void RunNetworkQueries() {
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

    terrier::runner::NetworkQueries_OutputRunners(&txn);

    txn.commit();
  } catch (std::exception &e) {
  }

  {
    std::unique_lock<std::mutex> lk(network_queries_mutex);
    network_queries_finished = true;
    network_queries_cv.notify_one();
  }
}

void RunNetworkSequence() {
  terrier::runner::db_main->GetMetricsManager()->Aggregate();
  terrier::runner::db_main->GetMetricsManager()->ToCSV();
  terrier::runner::InvokeGC();

  auto network_layer = terrier::runner::db_main->GetNetworkLayer();
  auto server = network_layer->GetServer();
  server->RunServer();

  auto thread = std::thread([] { RunNetworkQueries(); });

  {
    std::unique_lock<std::mutex> lk(network_queries_mutex);
    network_queries_ready = true;
    network_queries_cv.notify_one();
    network_queries_cv.wait(lk, [] { return network_queries_finished; });
  }

  terrier::runner::db_main->GetMetricsManager()->Aggregate();
  terrier::runner::db_main->GetMetricsManager()->ToCSV();
  terrier::runner::InvokeGC();

  // Concat pipeline.csv into execution_seq0.csv
  std::string csv_line;
  std::ofstream of("execution_SEQ0.csv", std::ios_base::binary | std::ios_base::app);
  std::ifstream is("pipeline.csv", std::ios_base::binary);
  std::getline(is, csv_line);
  of.seekp(0, std::ios_base::end);
  of << is.rdbuf();

  terrier::runner::db_main->ForceShutdown();
  thread.join();

  delete terrier::runner::db_main;
}

void RunBenchmarkSequence() {
  // As discussed, certain runners utilize multiple features.
  // In order for the modeller to work correctly, we first need to model
  // the dependent features and then subtract estimations/exact counters
  // from the composite to get an approximation for the target feature.
  //
  // As such: the following sequence is utilized
  // SEQ0: ArithmeticRunners
  // SEQ1: SeqScan, IdxScan
  // SEQ2: Sort
  // SEQ3: HashJoin
  // SEQ4: Aggregate
  // SEQ5: Insert, Update, Delete
  std::vector<std::vector<std::string>> filters = {{"SEQ0"}, {"SEQ1_0", "SEQ1_1", "SEQ1_2"}, {"SEQ2"}, {"SEQ3"},
                                                   {"SEQ4"}, {"SEQ5_0", "SEQ5_1", "SEQ5_2"}};

  char buffer[32];
  const char *argv[2];
  argv[0] = "mini_runners";
  argv[1] = buffer;

  auto vm_modes = {terrier::execution::vm::ExecutionMode::Interpret, terrier::execution::vm::ExecutionMode::Compiled};
  for (size_t i = 0; i < 6; i++) {
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

    snprintf(buffer, sizeof(buffer), "execution_SEQ%lu.csv", i);
    std::rename("pipeline.csv", buffer);
  }
}

int main(int argc, char **argv) {
  // mini_runners --port=9999 --benchmark_filter=
  std::pair<bool, int> port_info{false, -1};
  std::pair<bool, int> filter_info{false, -1};
  for (int i = 0; i < argc; i++) {
    if (strstr(argv[i], "--port=") != NULL)
      port_info = std::make_pair(true, i);
    else if (strstr(argv[i], "--benchmark_filter=") != NULL)
      filter_info = std::make_pair(true, i);
  }

  if (port_info.first) {
    char *arg = argv[port_info.second];
    char *equal_sign = strstr(arg, "=") + 1;
    terrier::runner::port = atoi(equal_sign);
  }

  terrier::LoggersUtil::Initialize();

  // Benchmark Config Environment Variables
  // Check whether we are being passed environment variables to override configuration parameter
  // for this benchmark run.
  const char *env_num_threads = std::getenv(terrier::ENV_NUM_THREADS);
  if (env_num_threads != nullptr) terrier::BenchmarkConfig::num_threads = atoi(env_num_threads);

  const char *env_logfile_path = std::getenv(terrier::ENV_LOGFILE_PATH);
  if (env_logfile_path != nullptr) terrier::BenchmarkConfig::logfile_path = std::string_view(env_logfile_path);

  terrier::runner::InitializeRunnersState();

  if (filter_info.first) {
    // Pass straight through to gbenchmark
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    terrier::runner::EndRunnersState();
  } else {
    RunBenchmarkSequence();
    RunNetworkSequence();
  }

  terrier::LoggersUtil::ShutDown();

  return 0;
}
