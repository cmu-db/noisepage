#include <vector>
#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "storage/garbage_collector_thread.h"
#include "storage/recovery/disk_log_provider.h"
#include "storage/recovery/recovery_manager.h"
#include "storage/storage_defs.h"
#include "storage/write_ahead_log/log_manager.h"
#include "util/sql_table_test_util.h"

// TODO(Gus): Change to ramdisk
// #define LOG_FILE_NAME "/mnt/ramdisk/benchmark.txt"
#define LOG_FILE_NAME "./benchmark.log"

namespace terrier {

class RecoveryBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final { unlink(LOG_FILE_NAME); }
  void TearDown(const benchmark::State &state) final { unlink(LOG_FILE_NAME); }

  const uint32_t initial_table_size = 1000000;
  const uint32_t num_txns_ = 100000;
  storage::BlockStore block_store_{1000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{1000000, 1000000};
  std::default_random_engine generator_;
  const uint32_t num_concurrent_txns_ = 4;
  storage::LogManager *log_manager_ = nullptr;
  storage::RecoveryManager *recovery_manager_ = nullptr;
  storage::GarbageCollectorThread *gc_thread_ = nullptr;
  const std::chrono::milliseconds gc_period_{10};

  // Settings for log manager
  const uint64_t num_log_buffers_ = 100;
  const std::chrono::milliseconds log_serialization_interval_{5};
  const std::chrono::milliseconds log_persist_interval_{10};
  const uint64_t log_persist_threshold_ = (1 << 20);  // 1MB
};

/**
 * Run an OLTP-like workload(5 statements per txn, 60% updates, 30% select, 10% deletes).
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(RecoveryBenchmark, OLTPWorkload)(benchmark::State &state) {
  uint32_t recovered_txns = 0;
  const uint16_t num_databases = 1;
  const uint16_t num_tables = 1;
  const uint16_t num_max_columns = 5;
  const uint32_t txn_length = 5;
  const std::vector<double> update_select_delete_ratio = {0.6, 0.3, 0.1};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    // Blow away log file after every benchmark iteration
    unlink(LOG_FILE_NAME);
    // Initialize table and run workload with logging enabled
    log_manager_ = new storage::LogManager(LOG_FILE_NAME, num_log_buffers_, log_serialization_interval_,
                                           log_persist_interval_, log_persist_threshold_, &buffer_pool_);
    log_manager_->Start();
    LargeSqlTableTestObject tested = LargeSqlTableTestObject::Builder()
                                         .SetNumDatabases(num_databases)
                                         .SetNumTables(num_tables)
                                         .SetMaxColumns(num_max_columns)
                                         .SetInitialTableSize(initial_table_size)
                                         .SetTxnLength(txn_length)
                                         .SetUpdateSelectDeleteRatio(update_select_delete_ratio)
                                         .SetBlockStore(&block_store_)
                                         .SetBufferPool(&buffer_pool_)
                                         .SetGenerator(&generator_)
                                         .SetGcOn(true)
                                         .SetVarlenAllowed(true)
                                         .SetLogManager(log_manager_)
                                         .build();

    // Run the test object and log all transactions
    tested.SimulateOltp(num_txns_, num_concurrent_txns_);
    log_manager_->PersistAndStop();

    // Create recovery table and recovery catalog
    EXPECT_EQ(1, tested.GetDatabases().size());
    auto database_oid = tested.GetDatabases()[0];
    EXPECT_EQ(1, tested.GetTablesForDatabase(database_oid).size());
    auto table_oid = tested.GetTablesForDatabase(database_oid)[0];
    auto *table_schema = tested.GetSchemaForTable(database_oid, table_oid);
    auto *recovered_sql_table = new storage::SqlTable(&block_store_, *table_schema, table_oid);
    storage::RecoveryCatalog catalog;
    catalog[database_oid][table_oid] = recovered_sql_table;

    // Create recovery transaction manager that doesn't log
    transaction::TransactionManager recovery_txn_manager_{&buffer_pool_, true, LOGGING_DISABLED};

    // Instantiate recovery manager, and recover the tables.
    storage::DiskLogProvider log_provider(&catalog, LOG_FILE_NAME);
    recovery_manager_ = new storage::RecoveryManager(&log_provider, &catalog, &recovery_txn_manager_);

    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      recovered_txns += recovery_manager_->Recover();
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

    // Delete test txns
    gc_thread_ = new storage::GarbageCollectorThread(tested.GetTxnManager(), gc_period_);
    delete gc_thread_;

    // Delete recovery txns
    gc_thread_ = new storage::GarbageCollectorThread(&recovery_txn_manager_, gc_period_);
    delete gc_thread_;
    delete recovery_manager_;
    delete recovered_sql_table;
    delete log_manager_;
  }
  state.SetItemsProcessed(recovered_txns);
}


BENCHMARK_REGISTER_F(RecoveryBenchmark, OLTPWorkload)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(3);

// BENCHMARK_REGISTER_F(LoggingBenchmark, HighAbortRate)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(10);

}  // namespace terrier
