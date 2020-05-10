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

namespace terrier {

class UpdateSchemaBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final { unlink(terrier::BenchmarkConfig::logfile_path.data()); }
  void TearDown(const benchmark::State &state) final { unlink(terrier::BenchmarkConfig::logfile_path.data()); }

  const uint32_t initial_table_size_ = 10000;
  const uint32_t num_txns_ = 5;
  const uint32_t num_indexes_ = 5;
  std::default_random_engine generator_;

  /**
   * Runs the recovery benchmark with the provided config
   * @param state benchmark state
   * @param config config to use for test object
   */
  void RunBenchmark(benchmark::State *state, const LargeSqlTableTestConfiguration &config, bool update_schema) {
    // NOLINTNEXTLINE
    for (auto _ : *state) {
      // Blow away log file after every benchmark iteration
      unlink(terrier::BenchmarkConfig::logfile_path.data());
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

      // Run the test object and log all transactions
      auto *tested =
          new LargeSqlTableTestObject(config, txn_manager.Get(), catalog.Get(), block_store.Get(), &generator_);

      int num_threads = 3;
      int num_schema_updates = 10;

      uint64_t elapsed_ms;
      {
        common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);

        for (int i = 0; i < num_schema_updates; i++) {
          if (update_schema) {
            tested->SimulateOltpAndUpdateSchema(num_txns_, num_threads + 1);
          } else {
            tested->SimulateOltp(num_txns_, num_threads);
          }
        }
      }

      log_manager->ForceFlush();

      state->SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

      // the table can't be freed until after all GC on it is guaranteed to be done. The easy way to do that is to use a
      // DeferredAction
      db_main->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() { delete tested; });
    }
    state->SetItemsProcessed(num_txns_ * state->iterations());
  }
};

/**
 * Run a a read-write workload with update schema (5 statements per txn, 50% inserts, 50% select).
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(UpdateSchemaBenchmark, ReadWriteUpdateSchemaWorkload)(benchmark::State &state) {
LargeSqlTableTestConfiguration config = LargeSqlTableTestConfiguration::Builder()
    .SetNumDatabases(1)
    .SetNumTables(1)
    .SetMaxColumns(5)
    .SetInitialTableSize(initial_table_size_)
    .SetTxnLength(5)
    .SetInsertUpdateSelectDeleteRatio({1.0, 0.0, 0.0, 0.0})
    .SetVarlenAllowed(true)
    .Build();

RunBenchmark(&state, config, true);
}

/**
 * Run a a read-write workload without update schema (5 statements per txn, 50% inserts, 50% select).
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(UpdateSchemaBenchmark, ReadWriteWorkload)(benchmark::State &state) {
LargeSqlTableTestConfiguration config = LargeSqlTableTestConfiguration::Builder()
    .SetNumDatabases(1)
    .SetNumTables(1)
    .SetMaxColumns(5)
    .SetInitialTableSize(initial_table_size_)
    .SetTxnLength(5)
    .SetInsertUpdateSelectDeleteRatio({0.5, 0.0, 0.5, 0.0})
    .SetVarlenAllowed(true)
    .Build();

RunBenchmark(&state, config, false);
}

// ----------------------------------------------------------------------------
// BENCHMARK REGISTRATION
// ----------------------------------------------------------------------------
// clang-format off
//BENCHMARK_REGISTER_F(UpdateSchemaBenchmark, ReadWriteWorkload)
//->Unit(benchmark::kMillisecond)
//->UseManualTime()
//->MinTime(10);
BENCHMARK_REGISTER_F(UpdateSchemaBenchmark, ReadWriteUpdateSchemaWorkload)
->Unit(benchmark::kMillisecond)
->UseManualTime()
->MinTime(10);
// clang-format on

}  // namespace terrier
