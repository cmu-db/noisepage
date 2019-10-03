#include <vector>
#include "benchmark/benchmark.h"
#include "catalog/catalog_accessor.h"
#include "common/scoped_timer.h"
#include "storage/garbage_collector_thread.h"
#include "storage/recovery/disk_log_provider.h"
#include "storage/recovery/recovery_manager.h"
#include "storage/storage_defs.h"
#include "storage/write_ahead_log/log_manager.h"
#include "util/sql_table_test_util.h"

#define LOG_FILE_NAME "/mnt/ramdisk/benchmark.txt"

namespace terrier {

class RecoveryBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final { unlink(LOG_FILE_NAME); }
  void TearDown(const benchmark::State &state) final { unlink(LOG_FILE_NAME); }

  const uint32_t initial_table_size_ = 1000000;
  const uint32_t num_txns_ = 100000;
  const uint32_t num_indexes_ = 5;
  storage::BlockStore block_store_{1000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{1000000, 1000000};
  std::default_random_engine generator_;
  const uint32_t num_concurrent_txns_ = 4;
  const std::chrono::milliseconds gc_period_{10};
  common::DedicatedThreadRegistry thread_registry_;

  // Settings for log manager
  const uint64_t num_log_buffers_ = 100;
  const std::chrono::microseconds log_serialization_interval_{5};
  const std::chrono::milliseconds log_persist_interval_{10};
  const uint64_t log_persist_threshold_ = (1u << 20u);  // 1MB

  /**
   * Runs the recovery benchmark with the provided config
   * @param state benchmark state
   * @param config config to use for test object
   */
  void RunBenchmark(benchmark::State *state, const LargeSqlTableTestConfiguration &config) {
    // NOLINTNEXTLINE
    for (auto _ : *state) {
      // Blow away log file after every benchmark iteration
      unlink(LOG_FILE_NAME);
      // Initialize table and run workload with logging enabled
      storage::LogManager log_manager(LOG_FILE_NAME, num_log_buffers_, log_serialization_interval_,
                                      log_persist_interval_, log_persist_threshold_, &buffer_pool_,
                                      common::ManagedPointer(&thread_registry_));
      log_manager.Start();

      transaction::TimestampManager timestamp_manager;
      transaction::DeferredActionManager deferred_action_manager(&timestamp_manager);
      transaction::TransactionManager txn_manager(&timestamp_manager, &deferred_action_manager, &buffer_pool_, true,
                                                  &log_manager);
      catalog::Catalog catalog(&txn_manager, &block_store_);
      storage::GarbageCollector gc(&timestamp_manager, &deferred_action_manager, &txn_manager, DISABLED);
      auto gc_thread = new storage::GarbageCollectorThread(&gc, gc_period_);  // Enable background GC

      // Run the test object and log all transactions
      auto *tested = new LargeSqlTableTestObject(config, &txn_manager, &catalog, &block_store_, &generator_);
      tested->SimulateOltp(num_txns_, num_concurrent_txns_);
      log_manager.ForceFlush();

      // Start a new components with logging disabled, we don't want to log the log replaying
      transaction::TimestampManager recovery_timestamp_manager;
      transaction::DeferredActionManager recovery_deferred_action_manager(&recovery_timestamp_manager);
      transaction::TransactionManager recovery_txn_manager{
          &recovery_timestamp_manager, &recovery_deferred_action_manager, &buffer_pool_, true, DISABLED};
      storage::GarbageCollector recovery_gc(&recovery_timestamp_manager, &recovery_deferred_action_manager,
                                            &recovery_txn_manager, DISABLED);
      auto recovery_gc_thread = new storage::GarbageCollectorThread(&recovery_gc, gc_period_);  // Enable background GC

      // Create catalog for recovery
      catalog::Catalog recovered_catalog(&recovery_txn_manager, &block_store_);

      // Instantiate recovery manager, and recover the tables.
      storage::DiskLogProvider log_provider(LOG_FILE_NAME);
      storage::RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(&recovered_catalog),
                                                &recovery_txn_manager, &recovery_deferred_action_manager,
                                                common::ManagedPointer(&thread_registry_), &block_store_);

      uint64_t elapsed_ms;
      {
        common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
        recovery_manager.StartRecovery();
        recovery_manager.WaitForRecoveryToFinish();
      }

      state->SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

      // Clean up recovered data
      recovered_catalog.TearDown();
      delete recovery_gc_thread;
      StorageTestUtil::FullyPerformGC(&recovery_gc, DISABLED);

      // Clean up test data
      catalog.TearDown();
      delete tested;
      delete gc_thread;
      StorageTestUtil::FullyPerformGC(&gc, &log_manager);
      log_manager.PersistAndStop();
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
    unlink(LOG_FILE_NAME);
    // Initialize table and run workload with logging enabled
    storage::LogManager log_manager(LOG_FILE_NAME, num_log_buffers_, log_serialization_interval_, log_persist_interval_,
                                    log_persist_threshold_, &buffer_pool_, common::ManagedPointer(&thread_registry_));
    log_manager.Start();

    transaction::TimestampManager timestamp_manager;
    transaction::DeferredActionManager deferred_action_manager(&timestamp_manager);
    transaction::TransactionManager txn_manager(&timestamp_manager, &deferred_action_manager, &buffer_pool_, true,
                                                &log_manager);
    catalog::Catalog catalog(&txn_manager, &block_store_);
    storage::GarbageCollector gc(&timestamp_manager, &deferred_action_manager, &txn_manager, DISABLED);
    auto gc_thread = new storage::GarbageCollectorThread(&gc, gc_period_);  // Enable background GC

    // Create database, namespace, and table
    auto *txn = txn_manager.BeginTransaction();
    auto db_oid = catalog.CreateDatabase(txn, db_name, true);
    auto catalog_accessor = catalog.GetAccessor(txn, db_oid);
    auto namespace_oid = catalog_accessor->CreateNamespace(namespace_name);

    // Create random table
    auto col = catalog::Schema::Column("col1", type::TypeId::INTEGER, false,
                                       parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(0)));
    catalog::Schema schema({col});
    auto table_oid = catalog_accessor->CreateTable(namespace_oid, table_name, schema);
    schema = catalog_accessor->GetSchema(table_oid);
    auto *table = new storage::SqlTable(&block_store_, schema);
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
    txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto initializer = table->InitializerForProjectedRow({schema.GetColumn(0).Oid()});

    // Create and execute insert workload. We actually don't need to insert into indexes here, since we only care about
    // recovery doing it
    auto workload = [&](uint32_t id) {
      auto start_key = num_txns_ / num_concurrent_txns_ * id;
      auto end_key = start_key + num_txns_ / num_concurrent_txns_;
      for (auto key = start_key; key < end_key; key++) {
        auto *txn = txn_manager.BeginTransaction();
        auto redo_record = txn->StageWrite(db_oid, table_oid, initializer);
        *reinterpret_cast<int32_t *>(redo_record->Delta()->AccessForceNotNull(0)) = key;
        table->Insert(txn, redo_record);
        txn_manager.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      }
    };
    common::WorkerPool thread_pool(num_concurrent_txns_, {});
    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_concurrent_txns_, workload);
    log_manager.ForceFlush();

    // Start a new components with logging disabled, we don't want to log the log replaying
    transaction::TimestampManager recovery_timestamp_manager;
    transaction::DeferredActionManager recovery_deferred_action_manager(&recovery_timestamp_manager);
    transaction::TransactionManager recovery_txn_manager{&recovery_timestamp_manager, &recovery_deferred_action_manager,
                                                         &buffer_pool_, true, DISABLED};
    storage::GarbageCollector recovery_gc(&recovery_timestamp_manager, &recovery_deferred_action_manager,
                                          &recovery_txn_manager, DISABLED);
    auto recovery_gc_thread = new storage::GarbageCollectorThread(&recovery_gc, gc_period_);  // Enable background GC

    // Create catalog for recovery
    catalog::Catalog recovered_catalog(&recovery_txn_manager, &block_store_);

    // Instantiate recovery manager, and recover the tables.
    storage::DiskLogProvider log_provider(LOG_FILE_NAME);
    storage::RecoveryManager recovery_manager(&log_provider, common::ManagedPointer(&recovered_catalog),
                                              &recovery_txn_manager, &recovery_deferred_action_manager,
                                              common::ManagedPointer(&thread_registry_), &block_store_);

    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      recovery_manager.StartRecovery();
      recovery_manager.WaitForRecoveryToFinish();
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

    // Clean up recovered data
    recovered_catalog.TearDown();
    delete recovery_gc_thread;
    StorageTestUtil::FullyPerformGC(&recovery_gc, DISABLED);

    // Clean up test data
    catalog.TearDown();
    delete gc_thread;
    StorageTestUtil::FullyPerformGC(&gc, &log_manager);
    log_manager.PersistAndStop();
  }
  state.SetItemsProcessed(num_txns_ * state.iterations());
}

BENCHMARK_REGISTER_F(RecoveryBenchmark, ReadWriteWorkload)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(4);

BENCHMARK_REGISTER_F(RecoveryBenchmark, HighStress)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(4);

BENCHMARK_REGISTER_F(RecoveryBenchmark, IndexRecovery)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(4);

}  // namespace terrier
