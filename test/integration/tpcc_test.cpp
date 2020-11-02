#include <random>
#include <string>
#include <vector>

#include "common/macros.h"
#include "common/scoped_timer.h"
#include "common/worker_pool.h"
#include "main/db_main.h"
#include "metrics/metrics_manager.h"
#include "test_util/test_harness.h"
#include "test_util/tpcc/builder.h"
#include "test_util/tpcc/database.h"
#include "test_util/tpcc/loader.h"
#include "test_util/tpcc/worker.h"
#include "test_util/tpcc/workload.h"

namespace noisepage::tpcc {

#define LOG_TEST_LOG_FILE_NAME "./test_tpcc_test.log"

/**
 * The behavior in these tests mimics that of /benchmark/integration/tpcc_benchmark.cpp. If something changes here, it
 * should probably change there as well.
 */
class TPCCTests : public TerrierTest {
 public:
  void SetUp() final {
    unlink(LOG_TEST_LOG_FILE_NAME);
    for (const auto &file : metrics::LoggingMetricRawData::FILES) unlink(std::string(file).c_str());
  }

  void TearDown() final { unlink(LOG_TEST_LOG_FILE_NAME); }

  std::default_random_engine generator_;

  const int8_t num_threads_ = 4;  // defines the number of terminals (workers running txns) and warehouses for the
  // benchmark. Sometimes called scale factor
  const uint32_t num_precomputed_txns_per_worker_ = 10000;  // Number of txns to run per terminal (worker thread)
  TransactionWeights txn_weights_;                          // default txn_weights. See definition for values

  common::WorkerPool thread_pool_{static_cast<uint32_t>(num_threads_), {}};

  void RunTPCC(const bool logging_enabled, const bool metrics_enabled, const storage::index::IndexType type) {
    // one TPCC worker = one TPCC terminal = one thread
    thread_pool_.Startup();
    std::vector<Worker> workers;
    workers.reserve(num_threads_);

    auto db_main = noisepage::DBMain::Builder()
                       .SetUseMetrics(metrics_enabled)
                       .SetUseMetricsThread(metrics_enabled)
                       .SetUseLogging(logging_enabled)
                       .SetUseGC(true)
                       .SetUseCatalog(true)
                       .SetUseGCThread(true)
                       .SetRecordBufferSegmentSize(1e6)
                       .Build();

    if (metrics_enabled) {
      db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::LOGGING);
      db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::TRANSACTION);
      db_main->GetMetricsManager()->SetMetricSampleInterval(metrics::MetricsComponent::LOGGING, 0);
      db_main->GetMetricsManager()->SetMetricSampleInterval(metrics::MetricsComponent::TRANSACTION, 100);
    }

    auto block_store = db_main->GetStorageLayer()->GetBlockStore();
    auto catalog = db_main->GetCatalogLayer()->GetCatalog();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();

    Builder tpcc_builder(block_store, catalog, txn_manager);

    // Precompute all of the input arguments for every txn to be run. We want to avoid the overhead at benchmark time
    const auto precomputed_args =
        PrecomputeArgs(&generator_, txn_weights_, num_threads_, num_precomputed_txns_per_worker_);

    // build the TPCC database
    auto *const tpcc_db = tpcc_builder.Build(type);

    // prepare the workers
    workers.clear();
    for (int8_t i = 0; i < num_threads_; i++) {
      workers.emplace_back(tpcc_db);
    }

    // populate the tables and indexes, as well as force log manager to log all changes
    Loader::PopulateDatabase(txn_manager, tpcc_db, &workers, &thread_pool_);

    std::this_thread::sleep_for(std::chrono::seconds(2));  // Let GC clean up

    // run the TPCC workload to completion
    for (int8_t i = 0; i < num_threads_; i++) {
      thread_pool_.SubmitTask([i, tpcc_db, &txn_manager, precomputed_args, &workers] {
        Workload(i, tpcc_db, txn_manager.Get(), precomputed_args, &workers);
      });
    }
    thread_pool_.WaitUntilAllFinished();

    delete tpcc_db;
    CleanUpVarlensInPrecomputedArgs(&precomputed_args);
  }
};

// NOLINTNEXTLINE
TEST_F(TPCCTests, WithoutLoggingHashIndexes) { RunTPCC(false, false, storage::index::IndexType::HASHMAP); }

// NOLINTNEXTLINE
TEST_F(TPCCTests, WithoutLoggingBwTreeIndexes) { RunTPCC(false, false, storage::index::IndexType::BWTREE); }

// NOLINTNEXTLINE
TEST_F(TPCCTests, WithLogging) { RunTPCC(true, false, storage::index::IndexType::HASHMAP); }

// NOLINTNEXTLINE
TEST_F(TPCCTests, WithLoggingAndMetrics) { RunTPCC(true, true, storage::index::IndexType::HASHMAP); }

}  // namespace noisepage::tpcc
