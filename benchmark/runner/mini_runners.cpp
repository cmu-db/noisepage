#include <common/macros.h>
#include <cstdio>
#include <functional>
#include <random>
#include <utility>

#include "benchmark/benchmark.h"
#include "benchmark_util/benchmark_config.h"
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
#include "planner/plannodes/seq_scan_plan_node.h"
#include "traffic_cop/traffic_cop_util.h"

namespace terrier::runner {

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
 * Arg <0, 1, 2, 3, 4, 5>
 * 0 - # integers to scan
 * 1 - # bigintegers to scan
 * 2 - integers of table
 * 3 - bigintegers of table
 * 4 - Number of rows
 * 5 - Cardinality
 */
#define GENERATE_MIXED_ARGUMENTS(args)                                                                  \
{                                                                                                       \
  /* Vector of table distributions <INTEGER, BIGINT> */                                                 \
  std::vector<std::pair<uint32_t,uint32_t>> mixed_dist = {{3, 12}, {7, 8}, {11, 4}};                    \
  /* Always generate full table scans for all row_num and cardinalities. */                             \
  for (auto col_dist : mixed_dist) {                                                                    \
    std::pair<uint32_t, uint32_t> start =  {1, 2};                                                      \
    while (true) {                                                                                      \
      for (auto row : row_nums) {                                                                       \
        int64_t car = 1;                                                                                \
        while (car < row) {                                                                             \
          args.push_back({start.first, start.second, col_dist.first, col_dist.second, row, car});       \
          car *= 2;                                                                                     \
        }                                                                                               \
        args.push_back({start.first, start.second, col_dist.first, col_dist.second, row, row});         \
      }                                                                                                 \
      if (start.second < col_dist.second) {                                                             \
        start.second += 2;                                                                              \
      } else if (start.first < col_dist.first) {                                                        \
        start.first += 2;                                                                               \
        start.second = 2;                                                                               \
      } else {                                                                                          \
        break;                                                                                          \
      }                                                                                                 \
    }                                                                                                   \
  }                                                                                                     \
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
 * Arg <0, 1, 2, 3, 4, 5>
 * 0 - # integers to scan
 * 1 - # big integers to scan
 * 2 - # integers in table
 * 3 - # big integers in table
 * 4 - row
 * 5 - cardinality
 */
static void GenScanArguments(benchmark::internal::Benchmark *b) {
  auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
  auto types = {type::TypeId::INTEGER, type::TypeId::BIGINT};
  std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    500,    1000,
                                   2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000};
  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : row_nums) {
        int64_t car = 1;
        while (car < row) {
          if (type == type::TypeId::INTEGER) b->Args({col, 0, 15, 0, row, car});
          else if (type == type::TypeId::BIGINT) b->Args({0, col, 0, 15, row, car});
          car *= 2;
        }

        if (type == type::TypeId::INTEGER) b->Args({col, 0, 15, 0, row, row});
        else if (type == type::TypeId::BIGINT) b->Args({0, col, 0, 15, row, row});
      }
    }
  }

  std::vector<std::vector<int64_t>> args;
  GENERATE_MIXED_ARGUMENTS(args);
  for (auto arg : args) {
    b->Args(arg);
  }
}

/**
 * Arg <0, 1, 2, 3, 4, 5>
 * 0 - # integers to scan
 * 1 - # bigintegers to scan
 * 2 - integers of table
 * 3 - bigintegers of table
 * 4 - Number of rows
 * 5 - Cardinality
 */
static void GenJoinSelfArguments(benchmark::internal::Benchmark *b) {
  auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
  auto types = {type::TypeId::INTEGER, type::TypeId::BIGINT};
  std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    500,    1000,
                                   2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000};
  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : row_nums) {
        int64_t car = 1;
        while (car < row) {
          if (row * row / car <= 1000000000) {
            if (type == type::TypeId::INTEGER) b->Args({col, 0, 15, 0, row, car});
            else if (type == type::TypeId::BIGINT) b->Args({0, col, 0, 15, row, car});
          }

          car *= 2;
        }

        if (type == type::TypeId::INTEGER) b->Args({col, 0, 15, 0, row, row});
        else if (type == type::TypeId::BIGINT) b->Args({0, col, 0, 15, row, row});
      }
    }
  }

  std::vector<std::vector<int64_t>> args;
  GENERATE_MIXED_ARGUMENTS(args);
  for (auto arg : args) {
    if (arg[4] != arg[5]) {
      if (arg[4] * arg[4] / arg[5] > 1000000000) {
        continue;
      }
    }

    b->Args(arg);
  }
}

/**
 * Arg <0, 1, 2, 3, 4, 5, 6, 7, 8>
 * 0 - # integers to scan
 * 1 - # bigintegers ot scan
 * 2 - # integers in table
 * 3 - # bigintegers in table
 * 4 - Build # rows
 * 5 - Build Cardinality
 * 6 - Probe # rows
 * 7 - Probe Cardinality
 * 8 - Matched cardinality
 */
static void GenJoinNonSelfArguments(benchmark::internal::Benchmark *b) {
  auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
  auto types = {type::TypeId::INTEGER, type::TypeId::BIGINT};
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
          if (type == type::TypeId::INTEGER) b->Args({col, 0, 15, 0, build_rows, build_car, probe_rows, probe_car, matched_car});
          else if (type == type::TypeId::BIGINT) b->Args({0, col, 0, 15, build_rows, build_car, probe_rows, probe_car, matched_car});
        }
      }
    }
  }

  /* Vector of table distributions <INTEGER, BIGINT> */
  std::vector<std::pair<uint32_t,uint32_t>> mixed_dist = {{3, 12}, {7, 8}, {11, 4}};
  for (auto col_dist : mixed_dist) {
    std::pair<uint32_t, uint32_t> start = {1, 2};
    while (true) {
      for (size_t i = 0; i < row_nums.size(); i++) {
        auto build_rows = row_nums[i];
        auto build_car = row_nums[i];
        for (size_t j = i + 1; j < row_nums.size(); j++) {
          auto probe_rows = row_nums[j];
          auto probe_car = row_nums[j];

          auto matched_car = row_nums[i];
          b->Args({start.first, start.second, col_dist.first, col_dist.second, build_rows, build_car, probe_rows, probe_car, matched_car});
        }
      }

      if (start.second < col_dist.second) {
        start.second += 2;
      } else if (start.first < col_dist.first) {
        start.first += 2;
        start.second = 2;
      } else {
        break;
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
  auto types = {type::TypeId::INTEGER, type::TypeId::BIGINT};
  auto key_sizes = {1, 2, 4, 8, 15};
  auto idx_sizes = {1, 10, 100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 500000, 1000000};
  std::vector<double> lookup_sizes = {0.1, 0.25, 0.33, 0.5, 0.66, 0.75, 0.9, 1};
  for (auto type : types) {
    for (auto key_size : key_sizes) {
      for (auto idx_size : idx_sizes) {
        std::unordered_set<int> lookups;
        for (auto lookup_size : lookup_sizes) {
          auto size = static_cast<int64_t>(idx_size * lookup_size);
          if (size > 0 && lookups.find(size) == lookups.end()) {
            lookups.insert(size);
            b->Args({static_cast<int64_t>(type), key_size, idx_size, size});
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
  auto types = {type::TypeId::INTEGER, type::TypeId::BIGINT};
  auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
  auto num_rows = {1, 10, 100, 1000, 2000, 5000, 10000, 20000, 50000, 100000};
  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : num_rows) {
        int64_t car = 1;
        while (car < row) {
          b->Args({static_cast<int64_t>(type), col, row, car});
          car *= 2;
        }

        b->Args({static_cast<int64_t>(type), col, row, row});
      }
    }
  }
}
*/

/**
 * Arg <0, 1, 2, 3>
 * 0 - Type
 * 1 - Number of columns
 * 2 - Number of rows
 * 3 - Cardinality
 */
static void GenUpdateArguments(benchmark::internal::Benchmark *b) {
  auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
  auto types = {type::TypeId::INTEGER, type::TypeId::BIGINT};
  std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    500,    1000,
                                   2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000};
  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : row_nums) {
        int64_t car = 1;
        while (car < row) {
          b->Args({static_cast<int64_t>(type), col, row, car});
          car *= 2;
        }

        b->Args({static_cast<int64_t>(type), col, row, row});
      }
    }
  }
}

class MiniRunners : public benchmark::Fixture {
 public:
  static execution::query_id_t query_id;
  static execution::vm::ExecutionMode mode;
  const uint64_t optimizer_timeout_ = 1000000;

  typedef std::unique_ptr<planner::AbstractPlanNode> (*PlanCorrect)(
      common::ManagedPointer<transaction::TransactionContext> txn, std::unique_ptr<planner::AbstractPlanNode>);

  static std::unique_ptr<planner::AbstractPlanNode> PassthroughPlanCorrect(
      UNUSED_ATTRIBUTE common::ManagedPointer<transaction::TransactionContext> txn,
      std::unique_ptr<planner::AbstractPlanNode> plan) {
    return plan;
  }

  std::string ConstructColumns(std::string prefix, int64_t num_integers, int64_t num_bigints) {
    std::stringstream cols;
    for (auto i = 1; i <= num_integers; i++) {
      cols << prefix << (type::TypeUtil::TypeIdToString(type::TypeId::INTEGER)) << i;
      if (i != num_integers || num_bigints != 0) cols << ", ";
    }

    for (auto i = 1; i <= num_bigints; i++) {
      cols << prefix << (type::TypeUtil::TypeIdToString(type::TypeId::BIGINT)) << i;
      if (i != num_bigints) cols << ", ";
    }
    return cols.str();
  }

  std::string ConstructPredicate(std::string left_alias, std::string right_alias, int64_t num_integers, int64_t num_bigints) {
    std::stringstream pred;
    for (auto i = 1; i <= num_integers; i++) {
      auto type_name = type::TypeUtil::TypeIdToString(type::TypeId::INTEGER);
      pred << left_alias << "." << type_name << i << " = " << right_alias << "." << type_name << i;
      if (i != num_integers || num_bigints != 0) pred << " AND ";
    }

    for (auto i = 1; i <= num_bigints; i++) {
      auto type_name = type::TypeUtil::TypeIdToString(type::TypeId::BIGINT);
      pred << left_alias << "." << type_name << i << " = " << right_alias << "." << type_name << i;
      if (i != num_bigints) pred << " AND ";
    }
    return pred.str();
  }

  std::string ConstructTableName(uint32_t tbl_ints, uint32_t tbl_bigints, size_t row, size_t car) {
    std::vector<type::TypeId> types = {type::TypeId::INTEGER, type::TypeId::BIGINT};
    std::vector<uint32_t> col_counts = {static_cast<uint32_t>(tbl_ints), static_cast<uint32_t>(tbl_bigints)};
    auto tbl_name = execution::sql::TableGenerator::GenerateMixedTableName(types, col_counts, row, car);
    if (tbl_ints == 0) tbl_name = execution::sql::TableGenerator::GenerateTableName(type::TypeId::BIGINT, tbl_bigints, row, car);
    else if (tbl_bigints == 0) tbl_name = execution::sql::TableGenerator::GenerateTableName(type::TypeId::INTEGER, tbl_ints, row, car);
    return tbl_name;
  }

  std::unique_ptr<planner::AbstractPlanNode> JoinNonSelfCorrector(
      std::string build_tbl, common::ManagedPointer<transaction::TransactionContext> txn,
      std::unique_ptr<planner::AbstractPlanNode> plan) {
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid);
    auto build_oid = accessor->GetTableOid(build_tbl);

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

  void BenchmarkSqlStatement(
      const std::string &query, brain::PipelineOperatingUnits *units,
      std::unique_ptr<optimizer::AbstractCostModel> cost_model, bool commit,
      std::function<std::unique_ptr<planner::AbstractPlanNode>(common::ManagedPointer<transaction::TransactionContext>,
                                                               std::unique_ptr<planner::AbstractPlanNode>)>
          corrector = std::function<std::unique_ptr<planner::AbstractPlanNode>(
              common::ManagedPointer<transaction::TransactionContext>, std::unique_ptr<planner::AbstractPlanNode>)>(
              PassthroughPlanCorrect)) {
    auto txn = txn_manager_->BeginTransaction();
    auto stmt_list = parser::PostgresParser::BuildParseTree(query);

    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid);
    auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid);
    binder.BindNameToNode(common::ManagedPointer(stmt_list), nullptr);

    auto out_plan = trafficcop::TrafficCopUtil::Optimize(
        common::ManagedPointer(txn), common::ManagedPointer(accessor), common::ManagedPointer(stmt_list), db_oid,
        db_main->GetStatsStorage(), std::move(cost_model), optimizer_timeout_);

    out_plan = corrector(common::ManagedPointer(txn), std::move(out_plan));

    execution::ExecutableQuery::query_identifier.store(MiniRunners::query_id++);
    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
        db_oid, common::ManagedPointer(txn), execution::exec::NoOpResultConsumer(), out_plan->GetOutputSchema().Get(),
        common::ManagedPointer(accessor));
    auto exec_query = execution::ExecutableQuery(common::ManagedPointer(out_plan), common::ManagedPointer(exec_ctx));
    exec_ctx->SetPipelineOperatingUnits(common::ManagedPointer(units));
    exec_query.Run(common::ManagedPointer(exec_ctx), mode);

    if (commit)
      txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    else
      txn_manager_->Abort(txn);

    auto gc = db_main->GetStorageLayer()->GetGarbageCollector();
    gc->PerformGarbageCollection();
    gc->PerformGarbageCollection();
  }

  void BenchmarkIndexScan(type::TypeId type, int key_num, int num_rows, int lookup_size) {
    auto qid = MiniRunners::query_id++;
    auto txn = txn_manager_->BeginTransaction();
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid);
    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(db_oid, common::ManagedPointer(txn), nullptr,
                                                                        nullptr, common::ManagedPointer(accessor));
    exec_ctx->SetExecutionMode(static_cast<uint8_t>(mode));

    auto type_size = type::TypeUtil::GetTypeSize(type);
    auto tuple_size = type_size * key_num;

    brain::PipelineOperatingUnits units;
    brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
    exec_ctx->SetPipelineOperatingUnits(common::ManagedPointer(&units));
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::IDX_SCAN, num_rows, tuple_size, key_num, lookup_size);
    units.RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

    auto table_name = execution::sql::TableGenerator::GenerateTableIndexName(type, num_rows);
    auto tbl_oid = accessor->GetTableOid(table_name);
    auto indexes = accessor->GetIndexes(tbl_oid);

    common::ManagedPointer<storage::index::Index> index = nullptr;
    for (auto index_cand : indexes) {
      if (index_cand.second.GetColumns().size() == static_cast<unsigned int>(key_num)) {
        index = index_cand.first;
        break;
      }
    }

    TERRIER_ASSERT(index != nullptr, "Invalid key_num specified");

    std::vector<storage::TupleSlot> results;
    if (lookup_size == 1) {
      auto *key_buffer =
          common::AllocationUtil::AllocateAligned(index->GetProjectedRowInitializer().ProjectedRowSize());
      auto *const scan_key_pr = index->GetProjectedRowInitializer().InitializeRow(key_buffer);

      std::default_random_engine generator;
      const uint32_t random_key =
          std::uniform_int_distribution(static_cast<uint32_t>(0), static_cast<uint32_t>(num_rows - 1))(generator);
      for (int i = 0; i < key_num; i++) {
        *reinterpret_cast<uint32_t *>(scan_key_pr->AccessForceNotNull(i)) = random_key;
      }

      exec_ctx->StartResourceTracker(metrics::MetricsComponent::EXECUTION_PIPELINE);
      index->ScanKey(*txn, *scan_key_pr, &results);
      exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(0));

      results.clear();
      delete[] key_buffer;
    } else {
      uint32_t high_key;
      uint32_t low_key;
      auto *high_buffer =
          common::AllocationUtil::AllocateAligned(index->GetProjectedRowInitializer().ProjectedRowSize());
      auto *low_buffer =
          common::AllocationUtil::AllocateAligned(index->GetProjectedRowInitializer().ProjectedRowSize());

      std::default_random_engine generator;
      low_key = std::uniform_int_distribution(static_cast<uint32_t>(0),
                                              static_cast<uint32_t>(num_rows - lookup_size))(generator);
      high_key = low_key + lookup_size - 1;
      auto *const low_key_pr = index->GetProjectedRowInitializer().InitializeRow(low_buffer);
      auto *const high_key_pr = index->GetProjectedRowInitializer().InitializeRow(high_buffer);
      for (int i = 0; i < key_num; i++) {
        *reinterpret_cast<uint32_t *>(low_key_pr->AccessForceNotNull(i)) = low_key;
        *reinterpret_cast<uint32_t *>(high_key_pr->AccessForceNotNull(i)) = high_key;
      }

      exec_ctx->StartResourceTracker(metrics::MetricsComponent::EXECUTION_PIPELINE);
      index->ScanAscending(*txn, storage::index::ScanType::Closed, key_num, low_key_pr, high_key_pr, 0, &results);
      exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(0));

      results.clear();
      delete[] low_buffer;
      delete[] high_buffer;
    }

    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto gc = db_main->GetStorageLayer()->GetGarbageCollector();
    gc->PerformGarbageCollection();
    gc->PerformGarbageCollection();
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
    pipe0_vec.emplace_back(type, num_elem, 4, 1, num_elem);
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

    auto gc = db_main->GetStorageLayer()->GetGarbageCollector();
    gc->PerformGarbageCollection();
    gc->PerformGarbageCollection();
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
  // Only benchmark arithmetic runners in interpret mode
  if (MiniRunners::mode != execution::vm::ExecutionMode::Interpret) {
    state.SetItemsProcessed(0);
    return;
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    metrics_manager_->RegisterThread();

    // state.range(0) is the OperatingUnitType
    // state.range(1) is the size
    BenchmarkArithmetic(static_cast<brain::ExecutionOperatingUnitType>(state.range(0)),
                        static_cast<size_t>(state.range(1)));

    metrics_manager_->Aggregate();
    metrics_manager_->UnregisterThread();
  }

  state.SetItemsProcessed(state.range(1));
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ0_ArithmeticRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenArithArguments);

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ0_OutputRunners)(benchmark::State &state) {
  // NOLINTNEXTLINE
  metrics_manager_->RegisterThread();
  for (auto _ : state) {
    std::vector<type::TypeId> types = {type::TypeId::INTEGER, type::TypeId::BIGINT};
    auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
    std::vector<std::string> types_strs = {"Integer", "Integer"};
    std::vector<int64_t> row_nums = {1,    3,    5,     7,     10,    50,     100,    500,    1000,
                                     2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000};
    for (size_t type_idx = 0; type_idx < types.size(); type_idx++) {
      for (auto num_col : num_cols) {
        for (auto row_num : row_nums) {
          auto type = types[type_idx];

          std::stringstream output;
          output << "struct Output {\n";
          for (auto i = 0; i < num_col; i++) output << "col" << i << " : " << types_strs[type_idx] << "\n";
          output << "}\n";

          output << "struct State {\ncount : int64\n}\n";
          output << "fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {\n}\n";
          output << "fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {\n}\n";

          // pipeline
          output << "fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {\n";
          for (auto i = 0; i < num_col; i++) {
            output << "\tvar outval" << i << " = ";
            if (type == type::TypeId::INTEGER)
              output << "@intToSql(" << (i + num_col) << ")\n";
            else if (type == type::TypeId::BIGINT)
              output << "@intToSql(" << (i + num_col) << ")\n";
          }
          output << "\tvar out: *Output\n";
          output << "\tfor(var it = 0; it < " << row_num << "; it = it + 1) {\n";
          output << "\t\tout = @ptrCast(*Output, @outputAlloc(execCtx))\n";
          for (auto i = 0; i < num_col; i++) output << "\t\tout.col" << i << " = outval" << i << "\n";
          output << "\t}\n";
          output << "\t@outputFinalize(execCtx)\n";
          output << "}\n";

          // main
          output << "fun main (execCtx: *ExecutionContext) -> int64 {\n";
          output << "\tvar state: State\n";
          output << "\tsetUpState(execCtx, &state)\n";
          output << "\t@execCtxStartResourceTracker(execCtx, 4)\n";
          output << "\tpipeline1(execCtx, &state)\n";
          output << "\t@execCtxEndPipelineTracker(execCtx, 0, 0)\n";
          output << "\tteardownState(execCtx, &state)\n";
          output << "\treturn 0\n";
          output << "}\n";

          auto type_size = type::TypeUtil::GetTypeSize(type);
          std::vector<planner::OutputSchema::Column> cols;
          for (auto i = 0; i < num_col; i++) {
            std::stringstream col;
            col << "col" << i;
            cols.emplace_back(col.str(), type, nullptr);
          }

          auto txn = txn_manager_->BeginTransaction();
          auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid);
          auto schema = std::make_unique<planner::OutputSchema>(std::move(cols));

          execution::ExecutableQuery::query_identifier.store(MiniRunners::query_id++);
          auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
              db_oid, common::ManagedPointer(txn), execution::exec::NoOpResultConsumer(), schema.get(),
              common::ManagedPointer(accessor));

          auto exec_query = execution::ExecutableQuery(output.str(), common::ManagedPointer(exec_ctx), false);

          brain::PipelineOperatingUnits units;
          brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
          exec_ctx->SetPipelineOperatingUnits(common::ManagedPointer(&units));
          pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, row_num, num_col * type_size, num_col,
                                 row_num);
          units.RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));
          exec_query.Run(common::ManagedPointer(exec_ctx), mode);

          txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

          auto gc = db_main->GetStorageLayer()->GetGarbageCollector();
          gc->PerformGarbageCollection();
          gc->PerformGarbageCollection();
        }
      }
    }

    metrics_manager_->Aggregate();
    metrics_manager_->UnregisterThread();
  }
};

BENCHMARK_REGISTER_F(MiniRunners, SEQ0_OutputRunners)->Unit(benchmark::kMillisecond)->Iterations(1);

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ1_SeqScanRunners)(benchmark::State &state) {
  auto num_integers = state.range(0);
  auto num_bigints = state.range(1);
  auto tbl_ints = state.range(2);
  auto tbl_bigints = state.range(3);
  auto row = state.range(4);
  auto car = state.range(5);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    metrics_manager_->RegisterThread();

    auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
    auto bigint_size = type::TypeUtil::GetTypeSize(type::TypeId::BIGINT);
    auto tuple_size = int_size * num_integers + bigint_size * num_bigints;
    auto num_col = num_integers + num_bigints;

    brain::PipelineOperatingUnits units;
    brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, row, tuple_size, num_col, car);
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, row, tuple_size, num_col, row);
    units.RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

    std::stringstream query;
    auto cols = ConstructColumns("", num_integers, num_bigints);
    auto tbl_name = ConstructTableName(tbl_ints, tbl_bigints, row, car);
    query << "SELECT " << (cols) << " FROM " << tbl_name;
    BenchmarkSqlStatement(query.str(), &units, std::make_unique<optimizer::TrivialCostModel>(), true);
    metrics_manager_->Aggregate();
    metrics_manager_->UnregisterThread();
  }

  state.SetItemsProcessed(row);
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ1_SeqScanRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenScanArguments);

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ1_IndexScanRunners)(benchmark::State &state) {
  if (MiniRunners::mode != execution::vm::ExecutionMode::Interpret) {
    state.SetItemsProcessed(0);
    return;
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    metrics_manager_->RegisterThread();
    BenchmarkIndexScan(static_cast<type::TypeId>(state.range(0)), state.range(1), state.range(2), state.range(3));
    metrics_manager_->Aggregate();
    metrics_manager_->UnregisterThread();
  }

  state.SetItemsProcessed(state.range(2));
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ1_IndexScanRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenIdxScanArguments);

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ1_InsertRunners)(benchmark::State &state) {
  auto type = static_cast<type::TypeId>(state.range(0));
  auto num_cols = state.range(1);
  auto num_rows = state.range(2);
  auto num_car = state.range(3);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    // Create temporary table schema
    std::vector<catalog::Schema::Column> cols;
    for (uint32_t j = 1; j <= num_cols; j++) {
      std::stringstream col_name;
      col_name << "col" << j;

      if (type == type::TypeId::INTEGER) {
        auto expr = parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(0));
        cols.emplace_back(col_name.str(), type, false, expr);
      } else {
        auto expr = parser::ConstantValueExpression(type::TransientValueFactory::GetDecimal(0));
        cols.emplace_back(col_name.str(), type, false, expr);
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

    std::stringstream query;
    query << "INSERT INTO tmp_table VALUES ";

    std::mt19937 generator{};
    std::uniform_int_distribution<int> distribution(0, INT_MAX);
    int64_t counter = 1;
    for (uint32_t idx = 0; idx < num_rows; idx++) {
      if (counter > num_car) {
        counter = 1;
      }

      auto val = counter;
      query << "(";
      for (uint32_t i = 1; i <= num_cols; i++) {
        query << val;
        if (i != num_cols) {
          query << ",";
        } else {
          query << ") ";
        }
      }
      counter++;

      if (idx != num_rows - 1) {
        query << ", ";
      }
    }

    auto type_size = type::TypeUtil::GetTypeSize(type);
    auto tuple_size = type_size * num_cols;

    brain::PipelineOperatingUnits units;
    brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::INSERT, num_rows, tuple_size, num_cols, num_car);
    units.RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      metrics_manager_->RegisterThread();
      BenchmarkSqlStatement(query.str(), &units, std::make_unique<optimizer::TrivialCostModel>(), true);
      metrics_manager_->Aggregate();
      metrics_manager_->UnregisterThread();
    }

    // Drop the table
    {
      auto txn = txn_manager_->BeginTransaction();
      auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid);
      accessor->DropTable(tbl_oid);
      txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

    auto gc = db_main->GetStorageLayer()->GetGarbageCollector();
    gc->PerformGarbageCollection();
    gc->PerformGarbageCollection();
  }

  state.SetItemsProcessed(num_rows);
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ1_InsertRunners)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->Iterations(1)
    ->Apply(GenInsertArguments);

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ2_UpdateRunners)(benchmark::State &state) {
  auto type = static_cast<type::TypeId>(state.range(0));
  auto num_cols = state.range(1);
  auto num_rows = state.range(2);
  auto num_car = state.range(3);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    // UPDATE [] SET [col] = random integer()
    // This does not force a read from the underlying tuple more than getting the slot.
    // Arguably, this approach has the least amount of "SEQ_SCAN" overhead and measures:
    // - Iterating over entire table for the slot
    // - Cost of "merging" updates with the undo/redos
    std::stringstream query;
    query << "UPDATE " << execution::sql::TableGenerator::GenerateTableName(type, 15, num_rows, num_car) << " SET ";
    std::vector<catalog::Schema::Column> cols;
    std::mt19937 generator{};
    std::uniform_int_distribution<int> distribution(0, INT_MAX);
    for (uint32_t j = 1; j <= num_cols; j++) {
      auto type_name = type::TypeUtil::TypeIdToString(type);
      query << type_name << j << " = " << distribution(generator);
      if (j != num_cols) {
        query << ", ";
      }
    }

    auto type_size = type::TypeUtil::GetTypeSize(type);
    auto tuple_size = type_size * num_cols;

    brain::PipelineOperatingUnits units;
    brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::UPDATE, num_rows, tuple_size, num_cols, num_car);

    // key_size and num_cols for SEQ_SCAN is 0 since just getting TupleSlot()
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, num_rows, 0, 0, num_car);
    units.RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      metrics_manager_->RegisterThread();
      BenchmarkSqlStatement(query.str(), &units, std::make_unique<optimizer::TrivialCostModel>(), false);
      metrics_manager_->Aggregate();
      metrics_manager_->UnregisterThread();
    }

    // Perform GC to do any cleanup...becuase the transaction aborted
    auto gc = db_main->GetStorageLayer()->GetGarbageCollector();
    gc->PerformGarbageCollection();
    gc->PerformGarbageCollection();

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }

  state.SetItemsProcessed(num_rows);
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ2_UpdateRunners)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->Iterations(1)
    ->Apply(GenUpdateArguments);

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ2_SortRunners)(benchmark::State &state) {
  auto num_integers = state.range(0);
  auto num_bigints = state.range(1);
  auto tbl_ints = state.range(2);
  auto tbl_bigints = state.range(3);
  auto row = state.range(4);
  auto car = state.range(5);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    metrics_manager_->RegisterThread();

    auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
    auto bigint_size = type::TypeUtil::GetTypeSize(type::TypeId::BIGINT);
    auto tuple_size = int_size * num_integers + bigint_size * num_bigints;
    auto num_col = num_integers + num_bigints;

    brain::PipelineOperatingUnits units;
    brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
    brain::ExecutionOperatingUnitFeatureVector pipe1_vec;
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, row, tuple_size, num_col, car);
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SORT_BUILD, row, tuple_size, num_col, car);
    pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::SORT_ITERATE, row, tuple_size, num_col, car);
    pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, row, tuple_size, num_col, row);
    units.RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));
    units.RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

    std::stringstream query;
    auto cols = ConstructColumns("", num_integers, num_bigints);
    auto tbl_name = ConstructTableName(tbl_ints, tbl_bigints, row, car);
    query << "SELECT " << (cols) << " FROM " << tbl_name << " ORDER BY " << (cols);
    BenchmarkSqlStatement(query.str(), &units, std::make_unique<optimizer::TrivialCostModel>(), true);
    metrics_manager_->Aggregate();
    metrics_manager_->UnregisterThread();
  }

  state.SetItemsProcessed(row);
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ2_SortRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenScanArguments);

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SEQ3_HashJoinSelfRunners)(benchmark::State &state) {
  auto num_integers = state.range(0);
  auto num_bigints = state.range(1);
  auto tbl_ints = state.range(2);
  auto tbl_bigints = state.range(3);
  auto row = state.range(4);
  auto car = state.range(5);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    metrics_manager_->RegisterThread();

    // Size of the scan tuple
    // Size of hash key size, probe key size
    // Size of output since only output 1 side
    auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
    auto bigint_size = type::TypeUtil::GetTypeSize(type::TypeId::BIGINT);
    auto tuple_size = int_size * num_integers + bigint_size * num_bigints;
    auto num_col = num_integers + num_bigints;

    auto hj_output = row * row / car;
    brain::PipelineOperatingUnits units;
    brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
    brain::ExecutionOperatingUnitFeatureVector pipe1_vec;
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, row, tuple_size, num_col, car);
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::HASHJOIN_BUILD, row, tuple_size, num_col, car);
    pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, row, tuple_size, num_col, car);
    pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::HASHJOIN_PROBE, row, tuple_size, num_col, hj_output);
    pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, hj_output, tuple_size, num_col, hj_output);
    units.RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));
    units.RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

    std::stringstream query;
    auto tbl_name = ConstructTableName(tbl_ints, tbl_bigints, row, car);
    query << "SELECT " << ConstructColumns("b.", num_integers, num_bigints);
    query << " FROM " << tbl_name << ", " << tbl_name << " as b WHERE ";
    query << ConstructPredicate(tbl_name, "b", num_integers, num_bigints);
    BenchmarkSqlStatement(query.str(), &units, std::make_unique<optimizer::ForcedCostModel>(true), true);
    metrics_manager_->Aggregate();
    metrics_manager_->UnregisterThread();
  }

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

  // NOLINTNEXTLINE
  for (auto _ : state) {
    metrics_manager_->RegisterThread();

    auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
    auto bigint_size = type::TypeUtil::GetTypeSize(type::TypeId::BIGINT);
    auto tuple_size = int_size * num_integers + bigint_size * num_bigints;
    auto num_col = num_integers + num_bigints;

    brain::PipelineOperatingUnits units;
    brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
    brain::ExecutionOperatingUnitFeatureVector pipe1_vec;
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, build_row, tuple_size, num_col, build_car);
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::HASHJOIN_BUILD, build_row, tuple_size, num_col,
                           build_car);
    pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, probe_row, tuple_size, num_col, probe_car);
    pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::HASHJOIN_PROBE, probe_row, tuple_size, num_col,
                           matched_car);
    pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, matched_car, tuple_size, num_col, matched_car);
    units.RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));
    units.RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

    auto build_tbl = ConstructTableName(tbl_ints, tbl_bigints, build_row, build_car);
    auto probe_tbl = ConstructTableName(tbl_ints, tbl_bigints, probe_row, probe_car);

    std::stringstream query;
    query << "SELECT " << ConstructColumns("b.", num_integers, num_bigints);
    query << " FROM " << build_tbl << ", " << probe_tbl << " as b WHERE ";
    query << ConstructPredicate(build_tbl, "b", num_integers, num_bigints);

    auto f =
        std::bind(&MiniRunners::JoinNonSelfCorrector, this, build_tbl, std::placeholders::_1, std::placeholders::_2);
    BenchmarkSqlStatement(query.str(), &units, std::make_unique<optimizer::ForcedCostModel>(true), true, f);
    metrics_manager_->Aggregate();
    metrics_manager_->UnregisterThread();
  }

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

  // NOLINTNEXTLINE
  for (auto _ : state) {
    metrics_manager_->RegisterThread();

    auto int_size = type::TypeUtil::GetTypeSize(type::TypeId::INTEGER);
    auto bigint_size = type::TypeUtil::GetTypeSize(type::TypeId::BIGINT);
    auto tuple_size = int_size * num_integers + bigint_size * num_bigints;
    auto num_col = num_integers + num_bigints;
    auto out_cols = num_col + 1;     // pulling the count(*) out
    auto out_size = tuple_size + 4;  // count(*) is an integer

    brain::PipelineOperatingUnits units;
    brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
    brain::ExecutionOperatingUnitFeatureVector pipe1_vec;
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, row, tuple_size, num_col, car);
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::AGGREGATE_BUILD, row, tuple_size, num_col, car);
    pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::AGGREGATE_ITERATE, car, out_size, out_cols, car);
    pipe1_vec.emplace_back(brain::ExecutionOperatingUnitType::OUTPUT, car, out_size, out_cols, car);
    units.RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));
    units.RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

    std::stringstream query;
    auto cols = ConstructColumns("", num_integers, num_bigints);
    auto tbl_name = ConstructTableName(tbl_ints, tbl_bigints, row, car);
    query << "SELECT COUNT(*), " << cols << " FROM " << tbl_name << " GROUP BY " << cols;
    BenchmarkSqlStatement(query.str(), &units, std::make_unique<optimizer::TrivialCostModel>(), true);
    metrics_manager_->Aggregate();
    metrics_manager_->UnregisterThread();
  }

  state.SetItemsProcessed(row);
}

BENCHMARK_REGISTER_F(MiniRunners, SEQ4_AggregateRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Apply(GenScanArguments);

void InitializeRunnersState() {
  terrier::execution::CpuInfo::Instance();
  terrier::execution::ExecutionUtil::InitTPL();
  auto db_main_builder = DBMain::Builder()
                             .SetUseGC(true)
                             .SetUseCatalog(true)
                             .SetUseStatsStorage(true)
                             .SetUseMetrics(true)
                             .SetBlockStoreSize(1000000)
                             .SetBlockStoreReuse(1000000)
                             .SetRecordBufferSegmentSize(1000000)
                             .SetRecordBufferSegmentReuse(1000000);

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

void RunBenchmarkSequence(void) {
  // As discussed, certain runners utilize multiple features.
  // In order for the modeller to work correctly, we first need to model
  // the dependent features and then subtract estimations/exact counters
  // from the composite to get an approximation for the target feature.
  //
  // As such: the following sequence is utilized
  // SEQ0: ArithmeticRunners
  // SEQ1: SeqScan, IdxScan, Insert
  // SEQ2: Sort, Update
  // SEQ3: HashJoin
  // SEQ4: Aggregate

  char buffer[32];
  const char *argv[2];
  argv[0] = "mini_runners";
  argv[1] = buffer;

  auto vm_modes = {terrier::execution::vm::ExecutionMode::Interpret, terrier::execution::vm::ExecutionMode::Compiled};
  for (size_t i = 0; i < 5; i++) {
    for (auto mode : vm_modes) {
      terrier::runner::MiniRunners::mode = mode;

      int argc = 2;
      snprintf(buffer, sizeof(buffer), "--benchmark_filter=SEQ%lu", i);
      benchmark::Initialize(&argc, const_cast<char **>(argv));
      benchmark::RunSpecifiedBenchmarks();

      auto gc = terrier::runner::db_main->GetStorageLayer()->GetGarbageCollector();
      gc->PerformGarbageCollection();
      gc->PerformGarbageCollection();
    }

    std::this_thread::sleep_for(std::chrono::seconds(2));
    terrier::runner::db_main->GetMetricsManager()->Aggregate();
    terrier::runner::db_main->GetMetricsManager()->ToCSV();

    snprintf(buffer, sizeof(buffer), "execution_SEQ%lu.csv", i);
    std::rename("pipeline.csv", buffer);
  }
}

int main(int argc, char **argv) {
  terrier::LoggersUtil::Initialize();

  // Benchmark Config Environment Variables
  // Check whether we are being passed environment variables to override configuration parameter
  // for this benchmark run.
  const char *env_num_threads = std::getenv(terrier::ENV_NUM_THREADS);
  if (env_num_threads != nullptr) terrier::BenchmarkConfig::num_threads = atoi(env_num_threads);

  const char *env_logfile_path = std::getenv(terrier::ENV_LOGFILE_PATH);
  if (env_logfile_path != nullptr) terrier::BenchmarkConfig::logfile_path = std::string_view(env_logfile_path);

  terrier::runner::InitializeRunnersState();

  if (argc > 1) {
    // Pass straight through to gbenchmark
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
  } else {
    RunBenchmarkSequence();
  }

  terrier::runner::EndRunnersState();

  terrier::LoggersUtil::ShutDown();

  return 0;
}
