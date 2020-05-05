#include <vector>

#include "benchmark/benchmark.h"
#include "benchmark_util/benchmark_config.h"
#include "catalog/catalog_accessor.h"
#include "common/scoped_timer.h"
#include "main/db_main.h"
#include "storage/recovery/disk_log_provider.h"
#include "storage/recovery/recovery_manager.h"
#include "storage/storage_defs.h"
#include "test_util/sql_table_test_util.h"
#include "storage/checkpoints/checkpoint.h"

namespace terrier {

class CheckpointRecoveryBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {
    unlink(terrier::BenchmarkConfig::logfile_path.data());
    std::string suffix = "_catalog";
    unlink((terrier::BenchmarkConfig::logfile_path.data() + suffix).c_str());
  }
  void TearDown(const benchmark::State &state) final { unlink(terrier::BenchmarkConfig::logfile_path.data()); }

  const uint32_t initial_table_size_ = 1000000;
  const uint32_t num_txns_ = 0;
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
      unlink(terrier::BenchmarkConfig::logfile_path.data());
      std::string suffix = "_catalog";
      unlink((terrier::BenchmarkConfig::logfile_path.data() + suffix).c_str());
      // Initialize table and run workload with logging enabled
      auto db_main = terrier::DBMain::Builder()
                         .SetLogFilePath(terrier::BenchmarkConfig::logfile_path.data())
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
      auto deferred_action_manager = db_main->GetTransactionLayer()->GetDeferredActionManager();
      auto gc = db_main->GetGarbageCollectorThread()->GetGarbageCollector();

      // Run the test object and log all transactions
      auto *tested =
          new LargeSqlTableTestObject(config, txn_manager.Get(), catalog.Get(), block_store.Get(), &generator_);
      tested->SimulateOltp(num_txns_, BenchmarkConfig::num_threads);
      log_manager->ForceFlush();

      std::string secondary_log_file = "test3.log";
      std::string ckpt_path = "ckpt_test/";
      mkdir(ckpt_path.c_str(), S_IRWXU | S_IRWXG | S_IRWXO);
      // get db_oid
      catalog::db_oid_t db;
      for (auto &database : tested->GetTables()) {
        db = database.first;
      }
      // initalize threads for checkpoint
      common::WorkerPool thread_pool_{BenchmarkConfig::num_threads, {}};
      thread_pool_.Startup();
      // take checkpoint
      unlink(secondary_log_file.c_str());
      storage::Checkpoint ckpt(catalog, txn_manager, deferred_action_manager, gc, log_manager);
      ckpt.TakeCheckpoint(ckpt_path, db, terrier::BenchmarkConfig::logfile_path.data(), BenchmarkConfig::num_threads, &thread_pool_);

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

      // Instantiate recovery manager, and recover the catalogs.
      storage::DiskLogProvider log_provider(terrier::BenchmarkConfig::logfile_path.data() + suffix);
      storage::RecoveryManager recovery_manager(
          common::ManagedPointer<storage::AbstractLogProvider>(&log_provider), recovery_catalog, recovery_txn_manager,
          recovery_deferred_action_manager, recovery_thread_registry, recovery_block_store);

      uint64_t elapsed_ms;
      {
        common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
        recovery_manager.StartRecovery();
        recovery_manager.WaitForRecoveryToFinish();
        recovery_manager.RecoverFromCheckpoint(ckpt_path, db);
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
BENCHMARK_DEFINE_F(CheckpointRecoveryBenchmark, ReadWriteWorkload)(benchmark::State &state) {
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
BENCHMARK_DEFINE_F(CheckpointRecoveryBenchmark, HighStress)(benchmark::State &state) {
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

// ----------------------------------------------------------------------------
// BENCHMARK REGISTRATION
// ----------------------------------------------------------------------------
// clang-format off
BENCHMARK_REGISTER_F(CheckpointRecoveryBenchmark, ReadWriteWorkload)
->Unit(benchmark::kMillisecond)
->UseManualTime()
->MinTime(10);
BENCHMARK_REGISTER_F(CheckpointRecoveryBenchmark, HighStress)
->Unit(benchmark::kMillisecond)
->UseManualTime()
->MinTime(10);
// clang-format on

}  // namespace terrier
