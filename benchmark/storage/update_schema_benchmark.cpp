#include <vector>

#include "benchmark/benchmark.h"
#include "benchmark_util/benchmark_config.h"
#include "catalog/catalog_accessor.h"
#include "common/scoped_timer.h"
#include "main/db_main.h"
#include "storage/storage_defs.h"
#include "test_util/sql_table_test_util.h"

namespace terrier {

class UpdateSchemaBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final { unlink(terrier::BenchmarkConfig::logfile_path.data()); }
  void TearDown(const benchmark::State &state) final { unlink(terrier::BenchmarkConfig::logfile_path.data()); }

  const uint32_t initial_table_size_ = 100000;
  const uint32_t num_txns_ = 10000;
  const uint32_t num_indexes_ = 5;
  // num_threads_ is how many threads to run concurrent operations with, we will use 1 extra thread to update schemas
  const uint32_t num_threads_ = 4;
  // how many consecutive schema updates (add/drop column) to perform in a single iteration
  const uint32_t num_schema_updates_ = 10;
  std::default_random_engine generator_;

  /**
   * Runs the update schema benchmark with the provided config
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

      uint64_t elapsed_ms;
      {
        common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);

        for (size_t i = 0; i < num_schema_updates_; i++) {
          if (update_schema) {
            // use 1 extra thread to update schemas. For fairness, we still use num_threads_ to perform oltp operations.
            tested->SimulateOltpAndUpdateSchema(num_txns_, num_threads_ + 1);
          } else {
            tested->SimulateOltp(num_txns_, num_threads_);
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

// TODO(Schema-change): Once we can deal with sql_table::update failing, add concurrent update tests

/**
 * Run a read-write-delete workload with update schema (5 statements per txn, 40% inserts, 40% select, 20% delete).
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(UpdateSchemaBenchmark, ReadWriteDeleteSchemaWorkload)(benchmark::State &state) {
  LargeSqlTableTestConfiguration config = LargeSqlTableTestConfiguration::Builder()
                                              .SetNumDatabases(1)
                                              .SetNumTables(1)
                                              .SetMaxColumns(5)
                                              .SetInitialTableSize(initial_table_size_)
                                              .SetTxnLength(5)
                                              .SetInsertUpdateSelectDeleteRatio({0.4, 0.0, 0.4, 0.2})
                                              .SetVarlenAllowed(true)
                                              .Build();

  RunBenchmark(&state, config, true);
}

/**
 * Run an insert-heavy workload with update schema (5 statements per txn, 90% inserts, 10% select).
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(UpdateSchemaBenchmark, InsertHeavySchemaWorkload)(benchmark::State &state) {
  LargeSqlTableTestConfiguration config = LargeSqlTableTestConfiguration::Builder()
                                              .SetNumDatabases(1)
                                              .SetNumTables(1)
                                              .SetMaxColumns(5)
                                              .SetInitialTableSize(initial_table_size_)
                                              .SetTxnLength(5)
                                              .SetInsertUpdateSelectDeleteRatio({0.9, 0.0, 0.1, 0.0})
                                              .SetVarlenAllowed(true)
                                              .Build();

  RunBenchmark(&state, config, true);
}

/**
 * Run a read-write-delete workload without update schema (5 statements per txn, 40% inserts, 40% select, 20% delete).
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(UpdateSchemaBenchmark, ReadWriteWorkload)(benchmark::State &state) {
  LargeSqlTableTestConfiguration config = LargeSqlTableTestConfiguration::Builder()
                                              .SetNumDatabases(1)
                                              .SetNumTables(1)
                                              .SetMaxColumns(5)
                                              .SetInitialTableSize(initial_table_size_)
                                              .SetTxnLength(5)
                                              .SetInsertUpdateSelectDeleteRatio({0.4, 0.0, 0.4, 0.2})
                                              .SetVarlenAllowed(true)
                                              .Build();

  RunBenchmark(&state, config, false);
}

/**
 * Run an insert-heavy workload without update schema (5 statements per txn, 90% inserts, 10% select).
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(UpdateSchemaBenchmark, InsertHeavyWorkload)(benchmark::State &state) {
  LargeSqlTableTestConfiguration config = LargeSqlTableTestConfiguration::Builder()
                                              .SetNumDatabases(1)
                                              .SetNumTables(1)
                                              .SetMaxColumns(5)
                                              .SetInitialTableSize(initial_table_size_)
                                              .SetTxnLength(5)
                                              .SetInsertUpdateSelectDeleteRatio({0.9, 0.0, 0.1, 0.0})
                                              .SetVarlenAllowed(true)
                                              .Build();

  RunBenchmark(&state, config, false);
}

// ----------------------------------------------------------------------------
// BENCHMARK REGISTRATION
// ----------------------------------------------------------------------------
// clang-format off
BENCHMARK_REGISTER_F(UpdateSchemaBenchmark, ReadWriteDeleteSchemaWorkload)
->Unit(benchmark::kMillisecond)
->UseManualTime()
->MinTime(10);

BENCHMARK_REGISTER_F(UpdateSchemaBenchmark, InsertHeavySchemaWorkload)
->Unit(benchmark::kMillisecond)
->UseManualTime()
->MinTime(10);

BENCHMARK_REGISTER_F(UpdateSchemaBenchmark, ReadWriteWorkload)
->Unit(benchmark::kMillisecond)
->UseManualTime()
->MinTime(10);

BENCHMARK_REGISTER_F(UpdateSchemaBenchmark, InsertHeavyWorkload)
->Unit(benchmark::kMillisecond)
->UseManualTime()
->MinTime(10);
// clang-format on

}  // namespace terrier
