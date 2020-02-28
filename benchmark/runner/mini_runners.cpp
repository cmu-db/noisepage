#include <common/macros.h>
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
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/optimizer.h"
#include "optimizer/properties.h"
#include "optimizer/query_to_operator_transformer.h"

namespace terrier::runner {

/**
 * Static db_main instance
 * This is done so all tests reuse the same DB Main instance
 */
DBMain *db_main_ = nullptr;

/**
 * Database OID
 * This is done so all tests can use the same database OID
 */
catalog::db_oid_t db_oid_{0};

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
  constexpr static bool value = !std::is_trivially_copyable<Decayed>::value ||
                                sizeof(Decayed) > sizeof(int64_t) || std::is_pointer<Decayed>::value;
};

template <typename T>
auto DoNotOptimizeAway(const T &datum) -> typename std::enable_if<!DoNotOptimizeAwayNeedsIndirect<T>::value>::type {
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
      DoNotOptimizeAway(i);                                        \
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
      DoNotOptimizeAway(i);                                                 \
      DoNotOptimizeAway(iterator);                                          \
    }                                                                       \
                                                                            \
    return iterator;                                                        \
  }

#endif

class MiniRunners : public benchmark::Fixture {
 public:
  static constexpr execution::query_id_t OP_INTEGER_PLUS_QID = execution::query_id_t(1);
  static constexpr execution::query_id_t OP_INTEGER_MULTIPLY_QID = execution::query_id_t(2);
  static constexpr execution::query_id_t OP_INTEGER_DIVIDE_QID = execution::query_id_t(3);
  static constexpr execution::query_id_t OP_INTEGER_GEQ_QID = execution::query_id_t(4);
  static constexpr execution::query_id_t OP_DECIMAL_PLUS_QID = execution::query_id_t(5);
  static constexpr execution::query_id_t OP_DECIMAL_MULTIPLY_QID = execution::query_id_t(6);
  static constexpr execution::query_id_t OP_DECIMAL_DIVIDE_QID = execution::query_id_t(7);
  static constexpr execution::query_id_t OP_DECIMAL_GEQ_QID = execution::query_id_t(8);
  static constexpr execution::query_id_t SEQ_SCAN_QID = execution::query_id_t(9);

  const uint64_t optimizer_timeout_ = 1000000;
  const execution::vm::ExecutionMode mode_ = execution::vm::ExecutionMode::Interpret;

  TIGHT_LOOP_OPERATION(uint32_t, PLUS, +);
  TIGHT_LOOP_OPERATION(uint32_t, MULTIPLY, *);
  TIGHT_LOOP_OPERATION(uint32_t, DIVIDE, /);
  TIGHT_LOOP_OPERATION(uint32_t, GEQ, >=);
  TIGHT_LOOP_OPERATION(double, PLUS, +);
  TIGHT_LOOP_OPERATION(double, MULTIPLY, *);
  TIGHT_LOOP_OPERATION(double, DIVIDE, /);
  TIGHT_LOOP_OPERATION(double, GEQ, >=);

  void SetUp(const benchmark::State &state) final {
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    metrics_manager_ = db_main_->GetMetricsManager();
  }

  void BenchmarkSqlStatement(execution::query_id_t qid, const std::string &query,
                             brain::PipelineOperatingUnits *units) {
    auto txn = txn_manager_->BeginTransaction();
    auto stmt_list = parser::PostgresParser::BuildParseTree(query);

    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid_);
    auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), "test_db");
    binder.BindNameToNode(stmt_list->GetStatement(0), stmt_list.get());

    auto transformer = optimizer::QueryToOperatorTransformer(common::ManagedPointer(accessor));
    auto plan = transformer.ConvertToOpExpression(stmt_list->GetStatement(0), stmt_list.get());

    std::unique_ptr<planner::AbstractPlanNode> out_plan;
    auto optimizer = optimizer::Optimizer(std::make_unique<optimizer::TrivialCostModel>(), optimizer_timeout_);
    if (stmt_list->GetStatement(0)->GetType() == parser::StatementType::SELECT) {
      auto sel_stmt = stmt_list->GetStatement(0).CastManagedPointerTo<parser::SelectStatement>();
      auto output = sel_stmt->GetSelectColumns();
      auto property_set = optimizer::PropertySet();

      if (sel_stmt->GetSelectOrderBy()) {
        std::vector<optimizer::OrderByOrderingType> sort_dirs;
        std::vector<common::ManagedPointer<parser::AbstractExpression>> sort_exprs;

        auto order_by = sel_stmt->GetSelectOrderBy();
        auto types = order_by->GetOrderByTypes();
        auto exprs = order_by->GetOrderByExpressions();
        for (size_t idx = 0; idx < order_by->GetOrderByExpressionsSize(); idx++) {
          sort_exprs.emplace_back(exprs[idx]);
          sort_dirs.push_back(types[idx] == parser::OrderType::kOrderAsc ? optimizer::OrderByOrderingType::ASC
                                                                         : optimizer::OrderByOrderingType::DESC);
        }

        auto sort_prop = new optimizer::PropertySort(sort_exprs, sort_dirs);
        property_set.AddProperty(sort_prop);
      }

      auto query_info = optimizer::QueryInfo(parser::StatementType::SELECT, std::move(output), &property_set);
      out_plan =
          optimizer.BuildPlanTree(txn, accessor.get(), db_main_->GetStatsStorage().Get(), query_info, std::move(plan));
    } else {
      UNREACHABLE("BenchmarkSqlStatement expecting a SELECT statement");
    }

    execution::ExecutableQuery::query_identifier.store(qid);
    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
        db_oid_, common::ManagedPointer(txn), execution::exec::NoOpResultConsumer(), out_plan->GetOutputSchema().Get(),
        common::ManagedPointer(accessor));
    auto exec_query = execution::ExecutableQuery(common::ManagedPointer(out_plan), common::ManagedPointer(exec_ctx));
    exec_ctx->SetPipelineOperatingUnits(common::ManagedPointer(units));
    exec_query.Run(common::ManagedPointer(exec_ctx), mode_);

    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

  void BenchmarkArithmetic(brain::ExecutionOperatingUnitType type, size_t num_elem) {
    auto txn = txn_manager_->BeginTransaction();
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid_);
    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(db_oid_, common::ManagedPointer(txn), nullptr,
                                                                        nullptr, common::ManagedPointer(accessor));
    exec_ctx->SetExecutionMode(static_cast<uint8_t>(mode_));
    exec_ctx->StartResourceTracker(metrics::MetricsComponent::EXECUTION_PIPELINE);

    brain::PipelineOperatingUnits units;
    brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
    exec_ctx->SetPipelineOperatingUnits(common::ManagedPointer(&units));
    switch (type) {
      case brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS: {
        pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS, num_elem, num_elem);
        units.RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

        uint32_t ret = __uint32_t_PLUS(num_elem);
        exec_ctx->EndPipelineTracker(OP_INTEGER_PLUS_QID, execution::pipeline_id_t(0));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY: {
        pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY, num_elem, num_elem);
        units.RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

        uint32_t ret = __uint32_t_MULTIPLY(num_elem);
        exec_ctx->EndPipelineTracker(OP_INTEGER_MULTIPLY_QID, execution::pipeline_id_t(0));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_INTEGER_DIVIDE: {
        pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::OP_INTEGER_DIVIDE, num_elem, num_elem);
        units.RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

        uint32_t ret = __uint32_t_DIVIDE(num_elem);
        exec_ctx->EndPipelineTracker(OP_INTEGER_DIVIDE_QID, execution::pipeline_id_t(0));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE: {
        pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE, num_elem, num_elem);
        units.RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

        uint32_t ret = __uint32_t_GEQ(num_elem);
        exec_ctx->EndPipelineTracker(OP_INTEGER_GEQ_QID, execution::pipeline_id_t(0));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_DECIMAL_PLUS_OR_MINUS: {
        pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::OP_DECIMAL_PLUS_OR_MINUS, num_elem, num_elem);
        units.RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

        double ret = __double_PLUS(num_elem);
        exec_ctx->EndPipelineTracker(OP_DECIMAL_PLUS_QID, execution::pipeline_id_t(0));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_DECIMAL_MULTIPLY: {
        pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::OP_DECIMAL_MULTIPLY, num_elem, num_elem);
        units.RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

        double ret = __double_MULTIPLY(num_elem);
        exec_ctx->EndPipelineTracker(OP_DECIMAL_MULTIPLY_QID, execution::pipeline_id_t(0));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_DECIMAL_DIVIDE: {
        pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::OP_DECIMAL_DIVIDE, num_elem, num_elem);
        units.RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

        double ret = __double_DIVIDE(num_elem);
        exec_ctx->EndPipelineTracker(OP_DECIMAL_DIVIDE_QID, execution::pipeline_id_t(0));
        DoNotOptimizeAway(ret);
        break;
      }
      case brain::ExecutionOperatingUnitType::OP_DECIMAL_COMPARE: {
        pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::OP_DECIMAL_COMPARE, num_elem, num_elem);
        units.RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

        double ret = __double_GEQ(num_elem);
        exec_ctx->EndPipelineTracker(OP_DECIMAL_GEQ_QID, execution::pipeline_id_t(0));
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

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, SeqScanRunners)(benchmark::State &state) {
  for (auto _ : state) {
    auto row = state.range(0);
    auto car = state.range(1);
    metrics_manager_->RegisterThread();

    brain::PipelineOperatingUnits units;
    brain::ExecutionOperatingUnitFeatureVector pipe0_vec;
    pipe0_vec.emplace_back(brain::ExecutionOperatingUnitType::SEQ_SCAN, row, static_cast<double>(car));
    units.RecordOperatingUnit(execution::pipeline_id_t(0), std::move(pipe0_vec));

    std::stringstream tbl_name;
    tbl_name << "SELECT * FROM INTEGERCol15Row" << row << "Car" << car;
    BenchmarkSqlStatement(SEQ_SCAN_QID, tbl_name.str(), &units);
    metrics_manager_->Aggregate();
    metrics_manager_->UnregisterThread();
  }

  state.SetItemsProcessed(state.range(0));
}

BENCHMARK_REGISTER_F(MiniRunners, SeqScanRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Args({10, 1})
    ->Args({10, 2})
    ->Args({10, 8})
    ->Args({100, 16})
    ->Args({100, 32})
    ->Args({100, 64})
    ->Args({10000, 128})
    ->Args({10000, 1024})
    ->Args({10000, 4096})
    ->Args({10000, 8192})
    ->Args({1000000, 1024})
    ->Args({1000000, 16384})
    ->Args({1000000, 131072})
    ->Args({1000000, 524288});

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, ArithmeticRunners)(benchmark::State &state) {
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

BENCHMARK_REGISTER_F(MiniRunners, ArithmeticRunners)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS), 10})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS), 100})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS), 10000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS), 1000000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS), 100000000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY), 10})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY), 100})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY), 10000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY), 1000000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY), 100000000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_INTEGER_DIVIDE), 10})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_INTEGER_DIVIDE), 100})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_INTEGER_DIVIDE), 10000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_INTEGER_DIVIDE), 1000000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_INTEGER_DIVIDE), 100000000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE), 10})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE), 100})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE), 10000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE), 1000000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE), 100000000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_DECIMAL_PLUS_OR_MINUS), 10})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_DECIMAL_PLUS_OR_MINUS), 100})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_DECIMAL_PLUS_OR_MINUS), 10000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_DECIMAL_PLUS_OR_MINUS), 1000000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_DECIMAL_PLUS_OR_MINUS), 100000000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_DECIMAL_MULTIPLY), 10})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_DECIMAL_MULTIPLY), 100})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_DECIMAL_MULTIPLY), 10000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_DECIMAL_MULTIPLY), 1000000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_DECIMAL_MULTIPLY), 100000000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_DECIMAL_DIVIDE), 10})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_DECIMAL_DIVIDE), 100})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_DECIMAL_DIVIDE), 10000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_DECIMAL_DIVIDE), 1000000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_DECIMAL_DIVIDE), 100000000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_DECIMAL_COMPARE), 10})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_DECIMAL_COMPARE), 100})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_DECIMAL_COMPARE), 10000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_DECIMAL_COMPARE), 1000000})
    ->Args({static_cast<int64_t>(brain::ExecutionOperatingUnitType::OP_DECIMAL_COMPARE), 100000000});

void InitializeRunnersState() {
  terrier::execution::CpuInfo::Instance();
  terrier::execution::ExecutionUtil::InitTPL();
  auto db_main_builder = DBMain::Builder()
                             .SetUseGC(true)
                             .SetUseCatalog(true)
                             .SetUseStatsStorage(true)
                             .SetUseGCThread(true)
                             .SetUseMetrics(true)
                             .SetBlockStoreSize(1000000)
                             .SetBlockStoreReuse(1000000)
                             .SetRecordBufferSegmentSize(1000000)
                             .SetRecordBufferSegmentReuse(1000000);

  db_main_ = db_main_builder.Build().release();

  auto block_store = db_main_->GetStorageLayer()->GetBlockStore();
  auto catalog = db_main_->GetCatalogLayer()->GetCatalog();
  auto txn_manager = db_main_->GetTransactionLayer()->GetTransactionManager();
  db_main_->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::EXECUTION_PIPELINE, 0);

  // Create the database
  auto txn = txn_manager->BeginTransaction();
  db_oid_ = catalog->CreateDatabase(common::ManagedPointer(txn), "test_db", true);

  // Load the database
  auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid_);
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(db_oid_, common::ManagedPointer(txn), nullptr,
                                                                      nullptr, common::ManagedPointer(accessor));

  execution::sql::TableGenerator table_gen(exec_ctx.get(), block_store, accessor->GetDefaultNamespace());
  table_gen.GenerateTestTables(true);

  txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

void EndRunnersState() {
  terrier::execution::ExecutionUtil::ShutdownTPL();
  db_main_->GetMetricsManager()->Aggregate();
  db_main_->GetMetricsManager()->ToCSV();
  // free db main here so we don't need to use the loggers anymore
  delete db_main_;
}

}  // namespace terrier::runner

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
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  terrier::runner::EndRunnersState();

  terrier::LoggersUtil::ShutDown();

  return 0;
}
