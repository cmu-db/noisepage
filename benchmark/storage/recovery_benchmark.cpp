#include <vector>
#include "benchmark/benchmark.h"
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
  storage::BlockStore block_store_{1000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{1000000, 1000000};
  std::default_random_engine generator_;
  const uint32_t num_concurrent_txns_ = 4;
  storage::LogManager *log_manager_ = nullptr;
  storage::RecoveryManager *recovery_manager_ = nullptr;
  storage::GarbageCollectorThread *gc_thread_ = nullptr;
  const std::chrono::milliseconds gc_period_{10};
  common::DedicatedThreadRegistry thread_registry_;

  // Settings for log manager
  const uint64_t num_log_buffers_ = 100;
  const std::chrono::milliseconds log_serialization_interval_{5};
  const std::chrono::milliseconds log_persist_interval_{10};
  const uint64_t log_persist_threshold_ = (1 << 20);  // 1MB

  /**
   * Runs the recovery benchmark with the provided config
   * @param state benchmark state
   * @param config config to use for test object
   */
  void RunBenchmark(benchmark::State *state, const LargeSqlTableTestConfiguration &config) {
    uint32_t recovered_txns = 0;

    // NOLINTNEXTLINE
    for (auto _ : *state) {
      // Blow away log file after every benchmark iteration
      unlink(LOG_FILE_NAME);
      // Initialize table and run workload with logging enabled
      log_manager_ =
          new storage::LogManager(LOG_FILE_NAME, num_log_buffers_, log_serialization_interval_, log_persist_interval_,
                                  log_persist_threshold_, &buffer_pool_, common::ManagedPointer(&thread_registry_));
      log_manager_->Start();
      LargeSqlTableTestObject tested =
          LargeSqlTableTestObject(config, &block_store_, &buffer_pool_, &generator_, log_manager_);

      // Run the test object and log all transactions
      tested.SimulateOltp(num_txns_, num_concurrent_txns_);
      log_manager_->PersistAndStop();

      // Start a transaction manager with logging disabled, we don't want to log the log replaying
      transaction::TransactionManager recovery_txn_manager{&buffer_pool_, true, LOGGING_DISABLED};

      // Create catalog for recovery
      catalog::Catalog recovered_catalog(&recovery_txn_manager, &block_store_);

      // Instantiate recovery manager, and recover the tables.
      storage::DiskLogProvider log_provider(LOG_FILE_NAME);
      recovery_manager_ =
          new storage::RecoveryManager(&log_provider, common::ManagedPointer(&recovered_catalog), &recovery_txn_manager,
                                       common::ManagedPointer(&thread_registry_), &block_store_);

      uint64_t elapsed_ms;
      {
        common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
        recovery_manager_->StartRecovery();
        recovery_manager_->FinishRecovery();
        recovered_txns += recovery_manager_->GetRecoveredTxnCount();
      }

      state->SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

      // Delete test txns
      gc_thread_ = new storage::GarbageCollectorThread(tested.GetTxnManager(), gc_period_);
      delete gc_thread_;

      // Delete recovery txns
      gc_thread_ = new storage::GarbageCollectorThread(&recovery_txn_manager, gc_period_);
      delete gc_thread_;
      delete recovery_manager_;
      delete log_manager_;
    }
    state->SetItemsProcessed(recovered_txns);
  }
};

/**
 * Run an OLTP-like workload(5 statements per txn, 60% updates, 30% select, 10% deletes).
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(RecoveryBenchmark, OLTPWorkload)(benchmark::State &state) {
  LargeSqlTableTestConfiguration config = LargeSqlTableTestConfiguration::Builder()
                                              .SetNumDatabases(1)
                                              .SetNumTables(1)
                                              .SetMaxColumns(5)
                                              .SetInitialTableSize(initial_table_size_)
                                              .SetTxnLength(5)
                                              .SetUpdateSelectDeleteRatio({0.6, 0.3, 0.1})
                                              .SetVarlenAllowed(true)
                                              .build();

  RunBenchmark(&state, config);
}

/**
 * Run transactions with a large number of updates to trigger high aborts. We use a large number of max columns to have
 * large changes and increase the chances that txns will flush logs before aborted.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(RecoveryBenchmark, HighAbortRate)(benchmark::State &state) {
  LargeSqlTableTestConfiguration config = LargeSqlTableTestConfiguration::Builder()
                                              .SetNumDatabases(1)
                                              .SetNumTables(1)
                                              .SetMaxColumns(100)
                                              .SetInitialTableSize(initial_table_size_)
                                              .SetTxnLength(40)
                                              .SetUpdateSelectDeleteRatio({0.8, 0.2, 0.0})
                                              .SetVarlenAllowed(true)
                                              .build();

  RunBenchmark(&state, config);
}

BENCHMARK_REGISTER_F(RecoveryBenchmark, OLTPWorkload)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(3);

BENCHMARK_REGISTER_F(RecoveryBenchmark, HighAbortRate)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(10);

}  // namespace terrier
