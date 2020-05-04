#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "benchmark/benchmark.h"
#include "benchmark_util/benchmark_config.h"
#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "common/scoped_timer.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/table_generator/table_generator.h"
#include "loggers/loggers_util.h"
#include "main/db_main.h"
#include "storage/storage_util.h"
#include "test_util/storage_test_util.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/timestamp_manager.h"
#include "transaction/transaction_context.h"

#include "catalog/catalog_defs.h"
#include "execution/compiler/compiler.h"
#include "execution/compiler/expression_util.h"
#include "execution/compiler/output_checker.h"
#include "execution/compiler/output_schema_util.h"
#include "execution/exec/output.h"
#include "execution/executable_query.h"
#include "execution/execution_util.h"
#include "execution/sema/sema.h"
#include "execution/sql/value.h"
#include "execution/vm/llvm_engine.h"
#include "execution/vm/module.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "type/transient_value.h"
#include "type/transient_value_factory.h"
#include "type/type_id.h"

namespace terrier {
class ParalleScanBenchmark : public benchmark::Fixture {};

BENCHMARK_DEFINE_F(ParalleScanBenchmark, TableVectorParallel)(benchmark::State &state) {
  // Below is the Working Version
  uint32_t num_row = 10000000;
  LoggersUtil::Initialize();
  auto db_main = terrier::DBMain::Builder().SetUseGC(true).SetUseGCThread(true).SetUseCatalog(true).Build();

  auto block_store = db_main->GetStorageLayer()->GetBlockStore();
  auto catalog = db_main->GetCatalogLayer()->GetCatalog();
  auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();

  auto test_txn = txn_manager->BeginTransaction();

  // Create catalog and test namespace
  auto test_db_oid = catalog->CreateDatabase(common::ManagedPointer(test_txn), "test_db", true);
  auto accessor = catalog->GetAccessor(common::ManagedPointer(test_txn), test_db_oid);
  auto test_ns_oid = accessor->GetDefaultNamespace();
  auto exe_ctx = std::make_unique<execution::exec::ExecutionContext>(
      test_db_oid, common::ManagedPointer(test_txn), nullptr, nullptr, common::ManagedPointer(accessor));
  execution::sql::TableGenerator table_generator{exe_ctx.get(), block_store, test_ns_oid};
  table_generator.GenerateBenchmarkTables(false, num_row);
  auto table_oid = exe_ctx->GetAccessor()->GetTableOid(test_ns_oid, "benchmark_1");
  struct Counter {
    uint32_t c_;
  };

  auto init_count = [](void *ctx, void *tls) { reinterpret_cast<Counter *>(tls)->c_ = 0; };

  // Scan function just counts all tuples it sees
  auto scanner = [](void *state, void *tls, execution::sql::TableVectorIterator *tvi) {
    auto *counter = reinterpret_cast<Counter *>(tls);
    while (tvi->Advance()) {
      for (auto *pci = tvi->GetProjectedColumnsIterator(); pci->HasNext(); pci->Advance()) {
        counter->c_++;
      }
    }
  };

  uint32_t col_oids[] = {1, 2};

  // Setup thread states
  execution::sql::ThreadStateContainer thread_state_container(
      common::ManagedPointer<execution::sql::MemoryPool>(exe_ctx->GetMemoryPool()));
  thread_state_container.Reset(sizeof(Counter),  // The type of each thread state structure
                               init_count,       // The thread state initialization function
                               nullptr,          // The thread state destruction function
                               nullptr);         // Context passed to init/destroy functions
  // NOLINTNEXTLINE
  for (auto _ : state) {
    uint64_t elapsed_ms = 0;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      execution::sql::TableVectorIterator::ParallelScan(static_cast<uint32_t>(table_oid),  // ID of table to scan
                                                        col_oids,  // Query state to pass to scan threads
                                                        2,         // Container for thread states
                                                        nullptr,   // Query state
                                                        scanner,   // Scan function
                                                        exe_ctx.get());
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_row);
  txn_manager->Commit(test_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

BENCHMARK_DEFINE_F(ParalleScanBenchmark, ParallelScan)(benchmark::State &state) {
  uint32_t num_row = 10000000;
  LoggersUtil::Initialize();
  auto db_main = terrier::DBMain::Builder().SetUseGC(true).SetUseGCThread(true).SetUseCatalog(true).Build();

  auto block_store = db_main->GetStorageLayer()->GetBlockStore();
  auto catalog = db_main->GetCatalogLayer()->GetCatalog();
  auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();

  auto test_txn = txn_manager->BeginTransaction();

  // Create catalog and test namespace
  auto test_db_oid = catalog->CreateDatabase(common::ManagedPointer(test_txn), "test_db", true);
  auto accessor = catalog->GetAccessor(common::ManagedPointer(test_txn), test_db_oid);
  auto test_ns_oid = accessor->GetDefaultNamespace();
  auto exe_ctx = std::make_unique<execution::exec::ExecutionContext>(
      test_db_oid, common::ManagedPointer(test_txn), nullptr, nullptr, common::ManagedPointer(accessor));
  execution::sql::TableGenerator table_generator{exe_ctx.get(), block_store, test_ns_oid};
  table_generator.GenerateBenchmarkTables(false, num_row);
  auto table_oid = exe_ctx->GetAccessor()->GetTableOid(test_ns_oid, "benchmark_1");
  auto table_schema = accessor->GetSchema(table_oid);
  execution::compiler::ExpressionMaker expr_maker;
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  execution::compiler::OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema.GetColumn("colA").Oid();
    auto colb_oid = table_schema.GetColumn("colB").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    // Make New Column
    auto col3 = expr_maker.OpMul(col1, col2);
    auto col4 = expr_maker.ComparisonGe(col1, expr_maker.OpMul(expr_maker.Constant(100), col2));
    seq_scan_out.AddOutput("col1", common::ManagedPointer(col1));
    seq_scan_out.AddOutput("col2", common::ManagedPointer(col2));
    seq_scan_out.AddOutput("col3", common::ManagedPointer(col3));
    seq_scan_out.AddOutput("col4", common::ManagedPointer(col4));
    auto schema = seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({cola_oid, colb_oid})
                   .SetScanPredicate(nullptr)
                   .SetIsForUpdateFlag(false)
                   .SetNamespaceOid(test_ns_oid)
                   .SetTableOid(table_oid)
                   .Build();
  }
  // Make the output checkers
  execution::compiler::SingleIntComparisonChecker col1_checker(std::greater_equal<>(), 0, 0);
  execution::compiler::MultiChecker multi_checker{std::vector<execution::compiler::OutputChecker *>{&col1_checker}};

  // Create the execution context
  execution::compiler::OutputStore store{&multi_checker, seq_scan->GetOutputSchema().Get()};
  execution::compiler::MultiOutputCallback callback{std::vector<execution::exec::OutputCallback>{store}};
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
      test_db_oid, common::ManagedPointer(test_txn), std::move(callback), seq_scan->GetOutputSchema().Get(),
      common::ManagedPointer(accessor));

  // Run & Check
  auto executable = execution::ExecutableQuery(common::ManagedPointer(seq_scan), common::ManagedPointer(exec_ctx));

  for (auto _ : state) {
    uint64_t elapsed_ms = 0;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      executable.Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_row);
  txn_manager->Commit(test_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

BENCHMARK_REGISTER_F(ParalleScanBenchmark, TableVectorParallel)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->UseManualTime();
BENCHMARK_REGISTER_F(ParalleScanBenchmark, ParallelScan)->Unit(benchmark::kMillisecond)->UseRealTime()->UseManualTime();
}  // namespace terrier