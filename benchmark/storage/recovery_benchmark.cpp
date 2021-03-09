#include <vector>

#include "benchmark/benchmark.h"
#include "benchmark_util/benchmark_config.h"
#include "catalog/catalog_accessor.h"
#include "common/scoped_timer.h"
#include "main/db_main.h"
#include "storage/index/index_builder.h"
#include "storage/recovery/disk_log_provider.h"
#include "storage/recovery/recovery_manager.h"
#include "storage/storage_defs.h"
#include "test_util/sql_table_test_util.h"

namespace noisepage {

class RecoveryBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final { unlink(noisepage::BenchmarkConfig::logfile_path.data()); }
  void TearDown(const benchmark::State &state) final { unlink(noisepage::BenchmarkConfig::logfile_path.data()); }

  const uint32_t initial_table_size_ = 1000000;
  const uint32_t num_txns_ = 100000;
  const uint32_t num_indexes_ = 5;
  std::default_random_engine generator_;

  /**
   * Runs the recovery benchmark with the provided config
   * @param state benchmark state
   * @param config config to use for test object
   */
  void RunBenchmark(benchmark::State *state, const LargeSqlTableTestConfiguration &config) {
    // NOLINTNEXTLINE
    for (auto _ : *state) {
      // Blow away log file after every benchmark iteration
      unlink(noisepage::BenchmarkConfig::logfile_path.data());
      // Initialize table and run workload with logging enabled
      auto db_main = noisepage::DBMain::Builder()
                         .SetWalFilePath(noisepage::BenchmarkConfig::logfile_path.data())
                         .SetUseLogging(true)
                         .SetUseGC(true)
                         .SetUseGCThread(true)
                         .SetUseCatalog(true)
                         .SetRecordBufferSegmentSize(1e6)
                         .SetRecordBufferSegmentReuse(1e6)
                         .Build();
      auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
      auto log_manager = db_main->GetLogManager();
      auto block_store = db_main->GetStorageLayer()->GetBlockStore();
      auto catalog = db_main->GetCatalogLayer()->GetCatalog();

      // Run the test object and log all transactions
      auto *tested =
          new LargeSqlTableTestObject(config, txn_manager.Get(), catalog.Get(), block_store.Get(), &generator_);
      tested->SimulateOltp(num_txns_, BenchmarkConfig::num_threads);
      log_manager->ForceFlush();

      // Start a new components with logging disabled, we don't want to log the log replaying
      auto recovery_db_main = DBMain::Builder()
                                  .SetUseThreadRegistry(true)
                                  .SetUseGC(true)
                                  .SetUseGCThread(true)
                                  .SetUseCatalog(true)
                                  .SetCreateDefaultDatabase(false)
                                  .Build();
      auto recovery_txn_manager = recovery_db_main->GetTransactionLayer()->GetTransactionManager();
      auto recovery_deferred_action_manager = recovery_db_main->GetTransactionLayer()->GetDeferredActionManager();
      auto recovery_block_store = recovery_db_main->GetStorageLayer()->GetBlockStore();
      auto recovery_catalog = recovery_db_main->GetCatalogLayer()->GetCatalog();
      auto recovery_thread_registry = recovery_db_main->GetThreadRegistry();
      auto recovery_replication_manager = recovery_db_main->GetReplicationManager();

      // Instantiate recovery manager, and recover the tables.
      storage::DiskLogProvider log_provider(noisepage::BenchmarkConfig::logfile_path.data());
      storage::RecoveryManager recovery_manager(common::ManagedPointer<storage::AbstractLogProvider>(&log_provider),
                                                recovery_catalog, recovery_txn_manager,
                                                recovery_deferred_action_manager, recovery_replication_manager,
                                                recovery_thread_registry, recovery_block_store);

      uint64_t elapsed_ms;
      {
        common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
        recovery_manager.StartRecovery();
        recovery_manager.WaitForRecoveryToFinish();
      }

      state->SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

      // the table can't be freed until after all GC on it is guaranteed to be done. The easy way to do that is to use a
      // DeferredAction
      db_main->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() { delete tested; });
    }
    state->SetItemsProcessed(num_txns_ * state->iterations());
  }
};

/**
 * Run a a read-write workload (5 statements per txn, 50% inserts, 50% select).
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(RecoveryBenchmark, ReadWriteWorkload)(benchmark::State &state) {
  LargeSqlTableTestConfiguration config = LargeSqlTableTestConfiguration::Builder()
                                              .SetNumDatabases(1)
                                              .SetNumTables(1)
                                              .SetMaxColumns(5)
                                              .SetInitialTableSize(initial_table_size_)
                                              .SetTxnLength(5)
                                              .SetInsertUpdateSelectDeleteRatio({0.5, 0.0, 0.5, 0.0})
                                              .SetVarlenAllowed(true)
                                              .Build();

  RunBenchmark(&state, config);
}

/**
 * High-stress workload, blast a narrow table with inserts (5 statements per txn, 100% inserts).
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(RecoveryBenchmark, HighStress)(benchmark::State &state) {
  LargeSqlTableTestConfiguration config = LargeSqlTableTestConfiguration::Builder()
                                              .SetNumDatabases(1)
                                              .SetNumTables(1)
                                              .SetMaxColumns(1)
                                              .SetInitialTableSize(initial_table_size_)
                                              .SetTxnLength(5)
                                              .SetInsertUpdateSelectDeleteRatio({1.0, 0.0, 0.0, 0.0})
                                              .SetVarlenAllowed(false)
                                              .Build();

  RunBenchmark(&state, config);
}

/**
 * Similar to high-stress workload, blast a narrow table with inserts (1 statements per txn, 100% inserts), but also
 * recovery indexes built on the table
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(RecoveryBenchmark, IndexRecovery)(benchmark::State &state) {
  auto db_name = "testdb";
  auto table_name = "testtable";
  auto index_name = "testindex";
  auto namespace_name = "testnamespace";

  // NOLINTNEXTLINE
  for (auto _ : state) {
    // Blow away log file after every benchmark iteration
    unlink(noisepage::BenchmarkConfig::logfile_path.data());
    // Initialize table and run workload with logging enabled
    auto db_main = noisepage::DBMain::Builder()
                       .SetWalFilePath(noisepage::BenchmarkConfig::logfile_path.data())
                       .SetUseLogging(true)
                       .SetUseGC(true)
                       .SetUseGCThread(true)
                       .SetUseCatalog(true)
                       .SetRecordBufferSegmentSize(1e6)
                       .SetRecordBufferSegmentReuse(1e6)
                       .Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    auto log_manager = db_main->GetLogManager();
    auto block_store = db_main->GetStorageLayer()->GetBlockStore();
    auto catalog = db_main->GetCatalogLayer()->GetCatalog();

    // Create database, namespace, and table
    auto *txn = txn_manager->BeginTransaction();
    auto db_oid = catalog->CreateDatabase(common::ManagedPointer(txn), db_name, true);
    auto catalog_accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
    auto namespace_oid = catalog_accessor->CreateNamespace(namespace_name);

    // Create random table
    auto col =
        catalog::Schema::Column("col1", type::TypeId::INTEGER, false,
                                parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(0)));
    catalog::Schema schema({col});
    auto table_oid = catalog_accessor->CreateTable(namespace_oid, table_name, schema);
    schema = catalog_accessor->GetSchema(table_oid);
    auto *table = new storage::SqlTable(block_store, schema);
    catalog_accessor->SetTablePointer(table_oid, table);

    // Create indexes on the table
    auto cols = schema.GetColumns();
    for (uint16_t i = 0; i < num_indexes_; i++) {
      auto index_col =
          catalog::IndexSchema::Column("index_col", type::TypeId::INTEGER, false,
                                       parser::ColumnValueExpression(db_oid, table_oid, schema.GetColumn(0).Oid()));
      catalog::IndexSchema index_schema({index_col}, storage::index::IndexType::BWTREE, true, false, false, true);
      auto index_oid =
          catalog_accessor->CreateIndex(namespace_oid, table_oid, index_name + std::to_string(i), index_schema);
      auto *index = storage::index::IndexBuilder().SetKeySchema(index_schema).Build();
      catalog_accessor->SetIndexPointer(index_oid, index);
    }
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto initializer = table->InitializerForProjectedRow({schema.GetColumn(0).Oid()});

    // Create and execute insert workload. We actually don't need to insert into indexes here, since we only care about
    // recovery doing it
    auto workload = [&](uint32_t id) {
      auto start_key = num_txns_ / BenchmarkConfig::num_threads * id;
      auto end_key = start_key + num_txns_ / BenchmarkConfig::num_threads;
      for (auto key = start_key; key < end_key; key++) {
        auto *txn = txn_manager->BeginTransaction();
        auto redo_record = txn->StageWrite(db_oid, table_oid, initializer);
        *reinterpret_cast<int32_t *>(redo_record->Delta()->AccessForceNotNull(0)) = key;
        table->Insert(common::ManagedPointer(txn), redo_record);
        txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      }
    };
    common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, BenchmarkConfig::num_threads, workload);
    log_manager->ForceFlush();

    // Start a new components with logging disabled, we don't want to log the log replaying
    auto recovery_db_main = DBMain::Builder()
                                .SetUseThreadRegistry(true)
                                .SetUseGC(true)
                                .SetUseGCThread(true)
                                .SetUseCatalog(true)
                                .SetCreateDefaultDatabase(false)
                                .Build();
    auto recovery_txn_manager = recovery_db_main->GetTransactionLayer()->GetTransactionManager();
    auto recovery_deferred_action_manager = recovery_db_main->GetTransactionLayer()->GetDeferredActionManager();
    auto recovery_block_store = recovery_db_main->GetStorageLayer()->GetBlockStore();
    auto recovery_catalog = recovery_db_main->GetCatalogLayer()->GetCatalog();
    auto recovery_thread_registry = recovery_db_main->GetThreadRegistry();
    auto recovery_replication_manager = recovery_db_main->GetReplicationManager();

    // Instantiate recovery manager, and recover the tables.
    storage::DiskLogProvider log_provider(noisepage::BenchmarkConfig::logfile_path.data());
    storage::RecoveryManager recovery_manager(
        common::ManagedPointer<storage::AbstractLogProvider>(&log_provider), recovery_catalog, recovery_txn_manager,
        recovery_deferred_action_manager, recovery_replication_manager, recovery_thread_registry, recovery_block_store);

    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      recovery_manager.StartRecovery();
      recovery_manager.WaitForRecoveryToFinish();
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(num_txns_ * state.iterations());
}

// ----------------------------------------------------------------------------
// BENCHMARK REGISTRATION
// ----------------------------------------------------------------------------
// clang-format off
BENCHMARK_REGISTER_F(RecoveryBenchmark, ReadWriteWorkload)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(10);
BENCHMARK_REGISTER_F(RecoveryBenchmark, HighStress)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(10);
BENCHMARK_REGISTER_F(RecoveryBenchmark, IndexRecovery)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(4);
// clang-format on

}  // namespace noisepage
