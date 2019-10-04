#include <random>
#include <vector>
#include "benchmark/benchmark.h"
#include "common/macros.h"
#include "common/scoped_timer.h"
#include "common/worker_pool.h"
#include "storage/garbage_collector_thread.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_manager.h"
#include "util/tpcc/builder.h"
#include "util/tpcc/database.h"
#include "util/tpcc/delivery.h"
#include "util/tpcc/loader.h"
#include "util/tpcc/new_order.h"
#include "util/tpcc/order_status.h"
#include "util/tpcc/payment.h"
#include "util/tpcc/stock_level.h"
#include "util/tpcc/worker.h"
#include "util/tpcc/workload.h"

namespace terrier::tpcc {

#define LOG_FILE_NAME "/mnt/ramdisk/tpcc.log"

/**
 * The behavior in these benchmarks mimics that of /test/integration/tpcc_test.cpp. If something changes here, it should
 * probably change there as well.
 */
class TPCCBenchmark : public benchmark::Fixture {
 public:
  const uint64_t blockstore_size_limit_ =
      1000;  // May need to increase this if num_threads_ or num_precomputed_txns_per_worker_ are greatly increased
             // (table sizes grow with a bigger workload)
  const uint64_t blockstore_reuse_limit_ = 1000;
  const uint64_t buffersegment_size_limit_ = 1000000;
  const uint64_t buffersegment_reuse_limit_ = 1000000;
  storage::BlockStore block_store_{blockstore_size_limit_, blockstore_reuse_limit_};
  storage::RecordBufferSegmentPool buffer_pool_{buffersegment_size_limit_, buffersegment_reuse_limit_};
  std::default_random_engine generator_;
  storage::LogManager *log_manager_ = DISABLED;  // logging enabled will override this value

  // Settings for log manager
  const uint64_t num_log_buffers_ = 100;
  const std::chrono::microseconds log_serialization_interval_{5};
  const std::chrono::milliseconds log_persist_interval_{10};
  const uint64_t log_persist_threshold_ = (1U << 20U);  // 1MB

  const bool only_count_new_order_ = false;  // TPC-C specification is to only measure throughput for New Order in final
                                             // result, but most academic papers use all txn types
  const int8_t num_threads_ = 4;  // defines the number of terminals (workers running txns) and warehouses for the
                                  // benchmark. Sometimes called scale factor
  const uint32_t num_precomputed_txns_per_worker_ = 100000;  // Number of txns to run per terminal (worker thread)
  TransactionWeights txn_weights_;                           // default txn_weights. See definition for values

  common::WorkerPool thread_pool_{static_cast<uint32_t>(num_threads_), {}};
  common::DedicatedThreadRegistry thread_registry_;

  storage::GarbageCollector *gc_;
  storage::GarbageCollectorThread *gc_thread_ = nullptr;
  const std::chrono::milliseconds gc_period_{10};
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TPCCBenchmark, ScaleFactor4WithoutLogging)(benchmark::State &state) {
  // one TPCC worker = one TPCC terminal = one thread
  std::vector<Worker> workers;
  workers.reserve(num_threads_);

  // Reset the worker pool
  thread_pool_.Shutdown();
  thread_pool_.SetNumWorkers(num_threads_);
  thread_pool_.Startup();

  auto tpcc_builder = Builder(&block_store_);

  // Precompute all of the input arguments for every txn to be run. We want to avoid the overhead at benchmark time
  const auto precomputed_args =
      PrecomputeArgs(&generator_, txn_weights_, num_threads_, num_precomputed_txns_per_worker_);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    unlink(LOG_FILE_NAME);
    // we need transactions, TPCC database, and GC
    transaction::TimestampManager timestamp_manager;
    transaction::DeferredActionManager deferred_action_manager(&timestamp_manager);
    transaction::TransactionManager txn_manager(&timestamp_manager, &deferred_action_manager, &buffer_pool_, true,
                                                log_manager_);

    // build the TPCC database using HashMaps where possible
    auto *const tpcc_db = tpcc_builder.Build(storage::index::IndexType::HASHMAP);

    // prepare the workers
    workers.clear();
    for (int8_t i = 0; i < num_threads_; i++) {
      workers.emplace_back(tpcc_db);
    }

    // populate the tables and indexes
    Loader::PopulateDatabase(&txn_manager, &generator_, tpcc_db, workers);

    gc_ = new storage::GarbageCollector(&timestamp_manager, &deferred_action_manager, &txn_manager, DISABLED);
    gc_thread_ = new storage::GarbageCollectorThread(gc_, gc_period_);
    Util::RegisterIndexesForGC(&(gc_thread_->GetGarbageCollector()), tpcc_db);
    std::this_thread::sleep_for(std::chrono::seconds(2));  // Let GC clean up

    // run the TPCC workload to completion, timing the execution
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      for (int8_t i = 0; i < num_threads_; i++) {
        thread_pool_.SubmitTask([i, tpcc_db, &txn_manager, &precomputed_args, &workers] {
          Workload(i, tpcc_db, &txn_manager, precomputed_args, &workers);
        });
      }
      thread_pool_.WaitUntilAllFinished();
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

    // cleanup
    delete gc_thread_;
    delete gc_;
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
BENCHMARK_DEFINE_F(TPCCBenchmark, ScaleFactor4WithLogging)(benchmark::State &state) {
  // one TPCC worker = one TPCC terminal = one thread
  std::vector<Worker> workers;
  workers.reserve(num_threads_);

  // Reset the worker pool
  thread_pool_.Shutdown();
  thread_pool_.SetNumWorkers(num_threads_);
  thread_pool_.Startup();

  auto tpcc_builder = Builder(&block_store_);

  // Precompute all of the input arguments for every txn to be run. We want to avoid the overhead at benchmark time
  const auto precomputed_args =
      PrecomputeArgs(&generator_, txn_weights_, num_threads_, num_precomputed_txns_per_worker_);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    unlink(LOG_FILE_NAME);
    // we need transactions, TPCC database, and GC
    log_manager_ = new storage::LogManager(LOG_FILE_NAME, num_log_buffers_, log_serialization_interval_,
                                           log_persist_interval_, log_persist_threshold_, &buffer_pool_,
                                           common::ManagedPointer<common::DedicatedThreadRegistry>(&thread_registry_));
    log_manager_->Start();
    transaction::TimestampManager timestamp_manager;
    transaction::DeferredActionManager deferred_action_manager(&timestamp_manager);
    transaction::TransactionManager txn_manager(&timestamp_manager, &deferred_action_manager, &buffer_pool_, true,
                                                log_manager_);

    // build the TPCC database using HashMaps where possible
    auto *const tpcc_db = tpcc_builder.Build(storage::index::IndexType::HASHMAP);

    // prepare the workers
    workers.clear();
    for (int8_t i = 0; i < num_threads_; i++) {
      workers.emplace_back(tpcc_db);
    }

    // populate the tables and indexes
    Loader::PopulateDatabase(&txn_manager, &generator_, tpcc_db, workers);
    log_manager_->ForceFlush();
    gc_ = new storage::GarbageCollector(&timestamp_manager, &deferred_action_manager, &txn_manager, DISABLED);
    gc_thread_ = new storage::GarbageCollectorThread(gc_, gc_period_);
    Util::RegisterIndexesForGC(&(gc_thread_->GetGarbageCollector()), tpcc_db);
    std::this_thread::sleep_for(std::chrono::seconds(2));  // Let GC clean up

    // run the TPCC workload to completion, timing the execution
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      for (int8_t i = 0; i < num_threads_; i++) {
        thread_pool_.SubmitTask([i, tpcc_db, &txn_manager, &precomputed_args, &workers] {
          Workload(i, tpcc_db, &txn_manager, precomputed_args, &workers);
        });
      }
      thread_pool_.WaitUntilAllFinished();
      log_manager_->ForceFlush();
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

    // cleanup
    log_manager_->PersistAndStop();
    delete log_manager_;
    delete gc_thread_;
    delete gc_;
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

BENCHMARK_REGISTER_F(TPCCBenchmark, ScaleFactor4WithoutLogging)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(20);

BENCHMARK_REGISTER_F(TPCCBenchmark, ScaleFactor4WithLogging)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(20);
}  // namespace terrier::tpcc
