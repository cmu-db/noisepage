#include "benchmark/benchmark.h"
#include "brain/brain_defs.h"
#include "brain/operating_unit.h"
#include "common/scoped_timer.h"
#include "execution/execution_util.h"
#include "execution/table_generator/table_generator.h"
#include "execution/util/cpu_info.h"
#include "execution/vm/module.h"
#include "main/db_main.h"

namespace terrier::runner {

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
                                sizeof(Decayed) > sizeof(long) || std::is_pointer<Decayed>::value;
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
    execution::CpuInfo::Instance();
    terrier::execution::ExecutionUtil::InitTPL();
    auto db_main_builder = DBMain::Builder()
                               .SetUseGC(true)
                               .SetUseCatalog(true)
                               .SetUseGCThread(true)
                               .SetUseMetrics(true)
                               .SetBlockStoreSize(1000000)
                               .SetBlockStoreReuse(1000000)
                               .SetRecordBufferSegmentSize(1000000)
                               .SetRecordBufferSegmentReuse(1000000);
    db_main_ = db_main_builder.Build();
    block_store_ = db_main_->GetStorageLayer()->GetBlockStore();
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();

    metrics_manager_ = db_main_->GetMetricsManager();
    metrics_manager_->EnableMetric(metrics::MetricsComponent::EXECUTION_PIPELINE, 0);

    // Create the database
    auto txn = txn_manager_->BeginTransaction();
    db_oid_ = catalog_->CreateDatabase(common::ManagedPointer(txn), "test_db", true);

    // Load the database
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_oid_);
    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(db_oid_, common::ManagedPointer(txn), nullptr,
                                                                        nullptr, common::ManagedPointer(accessor));
    // execution::sql::TableGenerator table_gen(exec_ctx.get(), block_store_, accessor->GetDefaultNamespace());
    // table_gen.GenerateTestTables(true);

    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

  void TearDown(const benchmark::State &state) final {
    terrier::execution::ExecutionUtil::ShutdownTPL();
    metrics_manager_->Aggregate();
    metrics_manager_->ToCSV();
    // free db main here so we don't need to use the loggers anymore
    db_main_.reset();
  }

  catalog::db_oid_t GetDatabaseOid() { return db_oid_; }
  common::ManagedPointer<metrics::MetricsManager> GetMetricsManager() { return metrics_manager_; }
  common::ManagedPointer<transaction::TransactionManager> GetTxnManager() { return txn_manager_; }
  common::ManagedPointer<catalog::Catalog> GetCatalog() { return catalog_; }

  void BenchmarkArithmetic(brain::ExecutionOperatingUnitType type, size_t num_elem) {
    auto txn = GetTxnManager()->BeginTransaction();
    auto accessor = GetCatalog()->GetAccessor(common::ManagedPointer(txn), GetDatabaseOid());
    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
        GetDatabaseOid(), common::ManagedPointer(txn), nullptr, nullptr, common::ManagedPointer(accessor));
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

    GetTxnManager()->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

 private:
  catalog::db_oid_t db_oid_{0};
  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<storage::BlockStore> block_store_;
  common::ManagedPointer<metrics::MetricsManager> metrics_manager_;
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MiniRunners, ArithmeticRunners)(benchmark::State &state) {
  for (auto _ : state) {
    GetMetricsManager()->RegisterThread();

    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::microseconds> timer(&elapsed_ms);

      // state.range(0) is the OperatingUnitType
      // state.range(1) is the size
      BenchmarkArithmetic(static_cast<brain::ExecutionOperatingUnitType>(state.range(0)),
                          static_cast<size_t>(state.range(1)));
    }

    GetMetricsManager()->Aggregate();
    GetMetricsManager()->UnregisterThread();
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

}  // namespace terrier::runner
