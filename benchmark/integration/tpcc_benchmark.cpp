#include <random>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"
#include "catalog/catalog.h"
#include "common/macros.h"
#include "common/scoped_timer.h"
#include "common/worker_pool.h"
#include "main/db_main.h"
#include "metrics/logging_metric.h"
#include "metrics/metrics_thread.h"
#include "storage/garbage_collector_thread.h"
#include "storage/storage_defs.h"
#include "test_util/tpcc/builder.h"
#include "test_util/tpcc/database.h"
#include "test_util/tpcc/loader.h"
#include "test_util/tpcc/worker.h"
#include "test_util/tpcc/workload.h"
#include "transaction/transaction_manager.h"

namespace terrier::tpcc {

#define LOG_FILE_NAME "/mnt/ramdisk/tpcc.log"

/**
 * The behavior in these benchmarks mimics that of /test/integration/tpcc_test.cpp. If something changes here, it should
 * probably change there as well.
 */
class TPCCBenchmark : public benchmark::Fixture {
 public:
  std::default_random_engine generator_;

  const bool only_count_new_order_ = false;  // TPC-C specification is to only measure throughput for New Order in final
                                             // result, but most academic papers use all txn types
  const int8_t num_threads_ = 4;  // defines the number of terminals (workers running txns) and warehouses for the
                                  // benchmark. Sometimes called scale factor
  const uint32_t num_precomputed_txns_per_worker_ = 100000;  // Number of txns to run per terminal (worker thread)
  TransactionWeights txn_weights_;                           // default txn_weights. See definition for values

  common::WorkerPool thread_pool_{static_cast<uint32_t>(num_threads_), {}};
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TPCCBenchmark, ScaleFactor4WithoutLogging)(benchmark::State &state) {
  // one TPCC worker = one TPCC terminal = one thread
  std::vector<Worker> workers;
  workers.reserve(num_threads_);

  // Precompute all of the input arguments for every txn to be run. We want to avoid the overhead at benchmark time
  const auto precomputed_args =
      PrecomputeArgs(&generator_, txn_weights_, num_threads_, num_precomputed_txns_per_worker_);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    thread_pool_.Startup();

    auto db_main = DBMain::Builder()
                       .SetUseGC(true)
                       .SetUseCatalog(true)
                       .SetUseGCThread(true)
                       .SetRecordBufferSegmentSize(1e6)
                       .SetRecordBufferSegmentReuse(1e6)
                       .Build();

    auto block_store = db_main->GetStorageLayer()->GetBlockStore();
    auto catalog = db_main->GetCatalogLayer()->GetCatalog();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();

    Builder tpcc_builder(block_store, catalog, txn_manager);

    // build the TPCC database using HashMaps where possible
    auto *const tpcc_db = tpcc_builder.Build(storage::index::IndexType::HASHMAP);

    // prepare the workers
    workers.clear();
    for (int8_t i = 0; i < num_threads_; i++) {
      workers.emplace_back(tpcc_db);
    }

    // populate the tables and indexes
    Loader::PopulateDatabase(txn_manager, tpcc_db, &workers, &thread_pool_);

    Util::RegisterIndexesForGC(db_main->GetStorageLayer()->GetGarbageCollector(), common::ManagedPointer(tpcc_db));
    std::this_thread::sleep_for(std::chrono::seconds(2));  // Let GC clean up

    // run the TPCC workload to completion, timing the execution
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      for (int8_t i = 0; i < num_threads_; i++) {
        thread_pool_.SubmitTask([i, tpcc_db, &txn_manager, &precomputed_args, &workers] {
          Workload(i, tpcc_db, txn_manager.Get(), precomputed_args, &workers);
        });
      }
      thread_pool_.WaitUntilAllFinished();
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

    // cleanup
    Util::UnregisterIndexesForGC(db_main->GetStorageLayer()->GetGarbageCollector(), common::ManagedPointer(tpcc_db));
    thread_pool_.Shutdown();
    delete tpcc_db;
  }

  CleanUpVarlensInPrecomputedArgs(&precomputed_args);

  // Count the number of txns processed
  if (only_count_new_order_) {
    uint64_t num_new_orders = 0;
    for (const auto &worker_txns : precomputed_args) {
      for (const auto &txn : worker_txns) {
        if (txn.type_ == TransactionType::NewOrder) num_new_orders++;
      }
    }
    state.SetItemsProcessed(state.iterations() * num_new_orders);
  } else {
    state.SetItemsProcessed(state.iterations() * num_precomputed_txns_per_worker_ * num_threads_);
  }
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TPCCBenchmark, ScaleFactor4WithLogging)(benchmark::State &state) {
  // one TPCC worker = one TPCC terminal = one thread
  std::vector<Worker> workers;
  workers.reserve(num_threads_);

  // Precompute all of the input arguments for every txn to be run. We want to avoid the overhead at benchmark time
  const auto precomputed_args =
      PrecomputeArgs(&generator_, txn_weights_, num_threads_, num_precomputed_txns_per_worker_);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    thread_pool_.Startup();
    unlink(LOG_FILE_NAME);

    auto db_main = DBMain::Builder()
                       .SetUseGC(true)
                       .SetUseCatalog(true)
                       .SetUseGCThread(true)
                       .SetRecordBufferSegmentSize(1e6)
                       .SetRecordBufferSegmentReuse(1e6)
                       .SetUseLogging(true)
                       .Build();

    auto block_store = db_main->GetStorageLayer()->GetBlockStore();
    auto catalog = db_main->GetCatalogLayer()->GetCatalog();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();

    Builder tpcc_builder(block_store, catalog, txn_manager);

    // build the TPCC database
    auto *const tpcc_db = tpcc_builder.Build(storage::index::IndexType::HASHMAP);

    // prepare the workers
    workers.clear();
    for (int8_t i = 0; i < num_threads_; i++) {
      workers.emplace_back(tpcc_db);
    }

    // populate the tables and indexes
    Loader::PopulateDatabase(txn_manager, tpcc_db, &workers, &thread_pool_);

    db_main->GetLogManager()->ForceFlush();

    Util::RegisterIndexesForGC(db_main->GetStorageLayer()->GetGarbageCollector(), common::ManagedPointer(tpcc_db));
    std::this_thread::sleep_for(std::chrono::seconds(2));  // Let GC clean up

    // run the TPCC workload to completion, timing the execution
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      for (int8_t i = 0; i < num_threads_; i++) {
        thread_pool_.SubmitTask([i, tpcc_db, &txn_manager, &precomputed_args, &workers] {
          Workload(i, tpcc_db, txn_manager.Get(), precomputed_args, &workers);
        });
      }
      thread_pool_.WaitUntilAllFinished();
      db_main->GetLogManager()->ForceFlush();
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

    // cleanup
    Util::UnregisterIndexesForGC(db_main->GetStorageLayer()->GetGarbageCollector(), common::ManagedPointer(tpcc_db));
    thread_pool_.Shutdown();
    delete tpcc_db;
    unlink(LOG_FILE_NAME);
  }

  CleanUpVarlensInPrecomputedArgs(&precomputed_args);

  // Count the number of txns processed
  if (only_count_new_order_) {
    uint64_t num_new_orders = 0;
    for (const auto &worker_txns : precomputed_args) {
      for (const auto &txn : worker_txns) {
        if (txn.type_ == TransactionType::NewOrder) num_new_orders++;
      }
    }
    state.SetItemsProcessed(state.iterations() * num_new_orders);
  } else {
    state.SetItemsProcessed(state.iterations() * num_precomputed_txns_per_worker_ * num_threads_);
  }
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TPCCBenchmark, ScaleFactor4WithLoggingAndMetrics)(benchmark::State &state) {
  // one TPCC worker = one TPCC terminal = one thread
  std::vector<Worker> workers;
  workers.reserve(num_threads_);

  // Precompute all of the input arguments for every txn to be run. We want to avoid the overhead at benchmark time
  const auto precomputed_args =
      PrecomputeArgs(&generator_, txn_weights_, num_threads_, num_precomputed_txns_per_worker_);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    thread_pool_.Startup();
    unlink(LOG_FILE_NAME);
    for (const auto &file : metrics::LoggingMetricRawData::FILES) unlink(std::string(file).c_str());

    auto db_main = DBMain::Builder()
                       .SetUseGC(true)
                       .SetUseCatalog(true)
                       .SetUseGCThread(true)
                       .SetRecordBufferSegmentSize(1e6)
                       .SetRecordBufferSegmentReuse(1e6)
                       .SetUseLogging(true)
                       .SetUseMetrics(true)
                       .SetUseMetricsThread(true)
                       .Build();

    auto block_store = db_main->GetStorageLayer()->GetBlockStore();
    auto catalog = db_main->GetCatalogLayer()->GetCatalog();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();

    Builder tpcc_builder(block_store, catalog, txn_manager);

    // build the TPCC database using HashMaps where possible
    auto *const tpcc_db = tpcc_builder.Build(storage::index::IndexType::HASHMAP);

    // prepare the workers
    workers.clear();
    for (int8_t i = 0; i < num_threads_; i++) {
      workers.emplace_back(tpcc_db);
    }

    // populate the tables and indexes
    Loader::PopulateDatabase(txn_manager, tpcc_db, &workers, &thread_pool_);

    db_main->GetLogManager()->ForceFlush();

    Util::RegisterIndexesForGC(db_main->GetStorageLayer()->GetGarbageCollector(), common::ManagedPointer(tpcc_db));
    std::this_thread::sleep_for(std::chrono::seconds(2));  // Let GC clean up

    // run the TPCC workload to completion, timing the execution
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      for (int8_t i = 0; i < num_threads_; i++) {
        thread_pool_.SubmitTask([i, tpcc_db, &txn_manager, &precomputed_args, &workers] {
          Workload(i, tpcc_db, txn_manager.Get(), precomputed_args, &workers);
        });
      }
      thread_pool_.WaitUntilAllFinished();
      db_main->GetLogManager()->ForceFlush();
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

    // cleanup
    Util::UnregisterIndexesForGC(db_main->GetStorageLayer()->GetGarbageCollector(), common::ManagedPointer(tpcc_db));
    thread_pool_.Shutdown();
    delete tpcc_db;
    unlink(LOG_FILE_NAME);
  }

  CleanUpVarlensInPrecomputedArgs(&precomputed_args);

  // Count the number of txns processed
  if (only_count_new_order_) {
    uint64_t num_new_orders = 0;
    for (const auto &worker_txns : precomputed_args) {
      for (const auto &txn : worker_txns) {
        if (txn.type_ == TransactionType::NewOrder) num_new_orders++;
      }
    }
    state.SetItemsProcessed(state.iterations() * num_new_orders);
  } else {
    state.SetItemsProcessed(state.iterations() * num_precomputed_txns_per_worker_ * num_threads_);
  }
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TPCCBenchmark, ScaleFactor4WithMetrics)(benchmark::State &state) {
  // one TPCC worker = one TPCC terminal = one thread
  std::vector<Worker> workers;
  workers.reserve(num_threads_);

  // Precompute all of the input arguments for every txn to be run. We want to avoid the overhead at benchmark time
  const auto precomputed_args =
      PrecomputeArgs(&generator_, txn_weights_, num_threads_, num_precomputed_txns_per_worker_);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    thread_pool_.Startup();

    auto db_main = DBMain::Builder()
                       .SetUseGC(true)
                       .SetUseCatalog(true)
                       .SetUseGCThread(true)
                       .SetRecordBufferSegmentSize(1e6)
                       .SetRecordBufferSegmentReuse(1e6)
                       .SetUseMetrics(true)
                       .SetUseMetricsThread(true)
                       .Build();

    auto block_store = db_main->GetStorageLayer()->GetBlockStore();
    auto catalog = db_main->GetCatalogLayer()->GetCatalog();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();

    Builder tpcc_builder(block_store, catalog, txn_manager);

    // build the TPCC database
    auto *const tpcc_db = tpcc_builder.Build(storage::index::IndexType::HASHMAP);

    // prepare the workers
    workers.clear();
    for (int8_t i = 0; i < num_threads_; i++) {
      workers.emplace_back(tpcc_db);
    }

    // populate the tables and indexes
    Loader::PopulateDatabase(txn_manager, tpcc_db, &workers, &thread_pool_);

    Util::RegisterIndexesForGC(db_main->GetStorageLayer()->GetGarbageCollector(), common::ManagedPointer(tpcc_db));
    std::this_thread::sleep_for(std::chrono::seconds(2));  // Let GC clean up

    // run the TPCC workload to completion, timing the execution
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      for (int8_t i = 0; i < num_threads_; i++) {
        thread_pool_.SubmitTask([i, tpcc_db, &txn_manager, &precomputed_args, &workers, &db_main] {
          db_main->GetMetricsManager()->RegisterThread();
          Workload(i, tpcc_db, txn_manager.Get(), precomputed_args, &workers);
          db_main->GetMetricsManager()->UnregisterThread();
        });
      }
      thread_pool_.WaitUntilAllFinished();
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

    // cleanup
    Util::UnregisterIndexesForGC(db_main->GetStorageLayer()->GetGarbageCollector(), common::ManagedPointer(tpcc_db));
    thread_pool_.Shutdown();
    delete tpcc_db;
  }

  CleanUpVarlensInPrecomputedArgs(&precomputed_args);

  // Count the number of txns processed
  if (only_count_new_order_) {
    uint64_t num_new_orders = 0;
    for (const auto &worker_txns : precomputed_args) {
      for (const auto &txn : worker_txns) {
        if (txn.type_ == TransactionType::NewOrder) num_new_orders++;
      }
    }
    state.SetItemsProcessed(state.iterations() * num_new_orders);
  } else {
    state.SetItemsProcessed(state.iterations() * num_precomputed_txns_per_worker_ * num_threads_);
  }
}

BENCHMARK_REGISTER_F(TPCCBenchmark, ScaleFactor4WithoutLogging)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(20);

BENCHMARK_REGISTER_F(TPCCBenchmark, ScaleFactor4WithLogging)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(20);

BENCHMARK_REGISTER_F(TPCCBenchmark, ScaleFactor4WithLoggingAndMetrics)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(20);

BENCHMARK_REGISTER_F(TPCCBenchmark, ScaleFactor4WithMetrics)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(20);
}  // namespace terrier::tpcc
