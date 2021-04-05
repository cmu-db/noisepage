#include <random>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"
#include "benchmark_util/benchmark_config.h"
#include "catalog/catalog.h"
#include "common/dedicated_thread_registry.h"
#include "common/macros.h"
#include "common/scoped_timer.h"
#include "common/worker_pool.h"
#include "metrics/logging_metric.h"
#include "metrics/metrics_thread.h"
#include "storage/garbage_collector_thread.h"
#include "storage/storage_defs.h"
#include "test_util/tpcc/builder.h"
#include "test_util/tpcc/database.h"
#include "test_util/tpcc/loader.h"
#include "test_util/tpcc/worker.h"
#include "test_util/tpcc/workload.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_manager.h"

namespace noisepage::tpcc {
/**
 * The behavior in these benchmarks mimics that of /test/integration/tpcc_test.cpp. If something changes here, it should
 * probably change there as well.
 */
class TPCCBenchmark : public benchmark::Fixture {
 public:
  /**
   * May need to increase this if num_threads_ or num_precomputed_txns_per_worker_ are greatly increased
   * (table sizes grow with a bigger workload)
   */
  const uint64_t blockstore_size_limit_ = 1000;
  const uint64_t blockstore_reuse_limit_ = 1000;
  const uint64_t buffersegment_size_limit_ = 1000000;
  const uint64_t buffersegment_reuse_limit_ = 1000000;
  storage::BlockStore block_store_{blockstore_size_limit_, blockstore_reuse_limit_};
  storage::RecordBufferSegmentPool buffer_pool_{buffersegment_size_limit_, buffersegment_reuse_limit_};
  std::default_random_engine generator_;
  storage::LogManager *log_manager_ = DISABLED;  // logging enabled will override this value
  common::ConcurrentBlockingQueue<storage::BufferedLogWriter *> empty_buffer_queue_;

  // Settings for log manager
  const uint64_t num_log_buffers_ = 100;
  const std::chrono::microseconds log_serialization_interval_{100};
  const std::chrono::microseconds log_persist_interval_{100};
  const uint64_t log_persist_threshold_ = (1U << 20U);  // 1MB

  /**
   * TPC-C specification is to only measure throughput for New Order in final
   * result, but most academic papers use all txn types
   */
  const bool only_count_new_order_ = false;

  /**
   * Number of txns to run per terminal (worker thread)
   * default txn_weights. See definition for values
   */
  const uint32_t num_precomputed_txns_per_worker_ = 100000;
  TransactionWeights txn_weights_;
  common::DedicatedThreadRegistry *thread_registry_ = nullptr;

  storage::GarbageCollector *gc_ = nullptr;
  storage::GarbageCollectorThread *gc_thread_ = nullptr;
  const std::chrono::microseconds gc_period_{1000};
  const std::chrono::microseconds metrics_period_{10000};
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TPCCBenchmark, ScaleFactor4WithoutLogging)(benchmark::State &state) {
  // one TPCC worker = one TPCC terminal = one thread
  common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
  std::vector<Worker> workers;
  workers.reserve(noisepage::BenchmarkConfig::num_threads);

  // Precompute all of the input arguments for every txn to be run. We want to avoid the overhead at benchmark time
  const auto precomputed_args = PrecomputeArgs(&generator_, txn_weights_, noisepage::BenchmarkConfig::num_threads,
                                               num_precomputed_txns_per_worker_);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    thread_pool.Startup();
    unlink(noisepage::BenchmarkConfig::logfile_path.data());
    // we need transactions, TPCC database, and GC
    transaction::TimestampManager timestamp_manager;
    transaction::DeferredActionManager deferred_action_manager{common::ManagedPointer(&timestamp_manager)};
    transaction::TransactionManager txn_manager{common::ManagedPointer(&timestamp_manager),
                                                common::ManagedPointer(&deferred_action_manager),
                                                common::ManagedPointer(&buffer_pool_),
                                                true,
                                                false,
                                                common::ManagedPointer(log_manager_)};
    gc_ = new storage::GarbageCollector(common::ManagedPointer(&timestamp_manager),
                                        common::ManagedPointer(&deferred_action_manager),
                                        common::ManagedPointer(&txn_manager), DISABLED);
    catalog::Catalog catalog{common::ManagedPointer(&txn_manager), common::ManagedPointer(&block_store_),
                             common::ManagedPointer(gc_)};
    Builder tpcc_builder{common::ManagedPointer(&block_store_), common::ManagedPointer(&catalog),
                         common::ManagedPointer(&txn_manager)};

    // build the TPCC database using HashMaps where possible
    auto *const tpcc_db = tpcc_builder.Build(storage::index::IndexType::HASHMAP);

    // prepare the workers
    workers.clear();
    for (uint32_t i = 0; i < noisepage::BenchmarkConfig::num_threads; i++) {
      workers.emplace_back(tpcc_db);
    }

    // populate the tables and indexes
    Loader::PopulateDatabase(common::ManagedPointer(&txn_manager), tpcc_db, &workers, &thread_pool);

    // Let GC clean up
    gc_thread_ = new storage::GarbageCollectorThread(common::ManagedPointer(gc_), gc_period_, nullptr);
    std::this_thread::sleep_for(std::chrono::seconds(2));  // Let GC clean up

    // run the TPCC workload to completion, timing the execution
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      for (uint32_t i = 0; i < noisepage::BenchmarkConfig::num_threads; i++) {
        thread_pool.SubmitTask([i, tpcc_db, &txn_manager, &precomputed_args, &workers] {
          Workload(i, tpcc_db, &txn_manager, precomputed_args, &workers);
        });
      }
      thread_pool.WaitUntilAllFinished();
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

    // cleanup
    delete gc_thread_;
    catalog.TearDown();
    deferred_action_manager.FullyPerformGC(common::ManagedPointer(gc_), DISABLED);
    thread_pool.Shutdown();
    delete gc_;
    delete tpcc_db;
    unlink(noisepage::BenchmarkConfig::logfile_path.data());
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
    state.SetItemsProcessed(state.iterations() * num_precomputed_txns_per_worker_ *
                            noisepage::BenchmarkConfig::num_threads);
  }
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TPCCBenchmark, ScaleFactor4WithLogging)(benchmark::State &state) {
  // one TPCC worker = one TPCC terminal = one thread
  common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
  std::vector<Worker> workers;
  workers.reserve(noisepage::BenchmarkConfig::num_threads);

  // Precompute all of the input arguments for every txn to be run. We want to avoid the overhead at benchmark time
  const auto precomputed_args = PrecomputeArgs(&generator_, txn_weights_, noisepage::BenchmarkConfig::num_threads,
                                               num_precomputed_txns_per_worker_);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    thread_pool.Startup();
    unlink(noisepage::BenchmarkConfig::logfile_path.data());
    thread_registry_ = new common::DedicatedThreadRegistry(DISABLED);
    // we need transactions, TPCC database, and GC
    log_manager_ = new storage::LogManager(
        noisepage::BenchmarkConfig::logfile_path.data(), num_log_buffers_, log_serialization_interval_,
        log_persist_interval_, log_persist_threshold_, common::ManagedPointer(&buffer_pool_),
        common::ManagedPointer(&empty_buffer_queue_), DISABLED, common::ManagedPointer(thread_registry_));
    log_manager_->Start();
    transaction::TimestampManager timestamp_manager;
    transaction::DeferredActionManager deferred_action_manager{common::ManagedPointer(&timestamp_manager)};
    transaction::TransactionManager txn_manager{common::ManagedPointer(&timestamp_manager),
                                                common::ManagedPointer(&deferred_action_manager),
                                                common::ManagedPointer(&buffer_pool_),
                                                true,
                                                false,
                                                common::ManagedPointer(log_manager_)};
    gc_ = new storage::GarbageCollector(common::ManagedPointer(&timestamp_manager),
                                        common::ManagedPointer(&deferred_action_manager),
                                        common::ManagedPointer(&txn_manager), DISABLED);
    catalog::Catalog catalog{common::ManagedPointer(&txn_manager), common::ManagedPointer(&block_store_),
                             common::ManagedPointer(gc_)};
    Builder tpcc_builder{common::ManagedPointer(&block_store_), common::ManagedPointer(&catalog),
                         common::ManagedPointer(&txn_manager)};

    // build the TPCC database
    auto *const tpcc_db = tpcc_builder.Build(storage::index::IndexType::HASHMAP);

    // prepare the workers
    workers.clear();
    for (uint32_t i = 0; i < noisepage::BenchmarkConfig::num_threads; i++) {
      workers.emplace_back(tpcc_db);
    }

    // populate the tables and indexes
    Loader::PopulateDatabase(common::ManagedPointer(&txn_manager), tpcc_db, &workers, &thread_pool);
    log_manager_->ForceFlush();
    gc_thread_ = new storage::GarbageCollectorThread(common::ManagedPointer(gc_), gc_period_, nullptr);
    std::this_thread::sleep_for(std::chrono::seconds(2));  // Let GC clean up

    // run the TPCC workload to completion, timing the execution
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      for (uint32_t i = 0; i < noisepage::BenchmarkConfig::num_threads; i++) {
        thread_pool.SubmitTask([i, tpcc_db, &txn_manager, &precomputed_args, &workers] {
          Workload(i, tpcc_db, &txn_manager, precomputed_args, &workers);
        });
      }
      thread_pool.WaitUntilAllFinished();
      log_manager_->ForceFlush();
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

    // cleanup
    delete gc_thread_;
    catalog.TearDown();
    deferred_action_manager.FullyPerformGC(common::ManagedPointer(gc_), common::ManagedPointer(log_manager_));
    thread_pool.Shutdown();
    log_manager_->PersistAndStop();
    delete log_manager_;
    delete gc_;
    delete thread_registry_;
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
    state.SetItemsProcessed(state.iterations() * num_precomputed_txns_per_worker_ *
                            noisepage::BenchmarkConfig::num_threads);
  }
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TPCCBenchmark, ScaleFactor4WithLoggingAndMetrics)(benchmark::State &state) {
  // one TPCC worker = one TPCC terminal = one thread
  common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
  std::vector<Worker> workers;
  workers.reserve(noisepage::BenchmarkConfig::num_threads);

  // Precompute all of the input arguments for every txn to be run. We want to avoid the overhead at benchmark time
  const auto precomputed_args = PrecomputeArgs(&generator_, txn_weights_, noisepage::BenchmarkConfig::num_threads,
                                               num_precomputed_txns_per_worker_);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    thread_pool.Startup();
    unlink(noisepage::BenchmarkConfig::logfile_path.data());
    for (const auto &file : metrics::LoggingMetricRawData::FILES) unlink(std::string(file).c_str());
    auto *const metrics_manager = new metrics::MetricsManager;
    auto *const metrics_thread = new metrics::MetricsThread(common::ManagedPointer(metrics_manager), metrics_period_);
    metrics_manager->SetMetricSampleRate(metrics::MetricsComponent::LOGGING, 100);
    metrics_manager->EnableMetric(metrics::MetricsComponent::LOGGING);
    thread_registry_ = new common::DedicatedThreadRegistry{common::ManagedPointer(metrics_manager)};
    // we need transactions, TPCC database, and GC
    log_manager_ = new storage::LogManager(
        noisepage::BenchmarkConfig::logfile_path.data(), num_log_buffers_, log_serialization_interval_,
        log_persist_interval_, log_persist_threshold_, common::ManagedPointer(&buffer_pool_),
        common::ManagedPointer(&empty_buffer_queue_), DISABLED, common::ManagedPointer(thread_registry_));
    log_manager_->Start();
    transaction::TimestampManager timestamp_manager;
    transaction::DeferredActionManager deferred_action_manager{common::ManagedPointer(&timestamp_manager)};
    transaction::TransactionManager txn_manager{common::ManagedPointer(&timestamp_manager),
                                                common::ManagedPointer(&deferred_action_manager),
                                                common::ManagedPointer(&buffer_pool_),
                                                true,
                                                false,
                                                common::ManagedPointer(log_manager_)};
    gc_ = new storage::GarbageCollector(common::ManagedPointer(&timestamp_manager),
                                        common::ManagedPointer(&deferred_action_manager),
                                        common::ManagedPointer(&txn_manager), DISABLED);
    catalog::Catalog catalog{common::ManagedPointer(&txn_manager), common::ManagedPointer(&block_store_),
                             common::ManagedPointer(gc_)};
    Builder tpcc_builder{common::ManagedPointer(&block_store_), common::ManagedPointer(&catalog),
                         common::ManagedPointer(&txn_manager)};

    // build the TPCC database using HashMaps where possible
    auto *const tpcc_db = tpcc_builder.Build(storage::index::IndexType::HASHMAP);

    // prepare the workers
    workers.clear();
    for (uint32_t i = 0; i < noisepage::BenchmarkConfig::num_threads; i++) {
      workers.emplace_back(tpcc_db);
    }

    // populate the tables and indexes
    Loader::PopulateDatabase(common::ManagedPointer(&txn_manager), tpcc_db, &workers, &thread_pool);
    log_manager_->ForceFlush();

    // Let GC clean up
    gc_thread_ = new storage::GarbageCollectorThread(common::ManagedPointer(gc_), gc_period_, nullptr);
    std::this_thread::sleep_for(std::chrono::seconds(2));  // Let GC clean up

    // run the TPCC workload to completion, timing the execution
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      for (uint32_t i = 0; i < noisepage::BenchmarkConfig::num_threads; i++) {
        thread_pool.SubmitTask([i, tpcc_db, &txn_manager, &precomputed_args, &workers] {
          Workload(i, tpcc_db, &txn_manager, precomputed_args, &workers);
        });
      }
      thread_pool.WaitUntilAllFinished();
      log_manager_->ForceFlush();
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

    // cleanup
    delete gc_thread_;
    catalog.TearDown();
    deferred_action_manager.FullyPerformGC(common::ManagedPointer(gc_), common::ManagedPointer(log_manager_));
    thread_pool.Shutdown();
    log_manager_->PersistAndStop();
    delete log_manager_;
    delete gc_;
    delete thread_registry_;
    delete metrics_thread;
    delete metrics_manager;
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
    state.SetItemsProcessed(state.iterations() * num_precomputed_txns_per_worker_ *
                            noisepage::BenchmarkConfig::num_threads);
  }
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TPCCBenchmark, ScaleFactor4WithMetrics)(benchmark::State &state) {
  // one TPCC worker = one TPCC terminal = one thread
  common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
  std::vector<Worker> workers;
  workers.reserve(noisepage::BenchmarkConfig::num_threads);

  // Precompute all of the input arguments for every txn to be run. We want to avoid the overhead at benchmark time
  const auto precomputed_args = PrecomputeArgs(&generator_, txn_weights_, noisepage::BenchmarkConfig::num_threads,
                                               num_precomputed_txns_per_worker_);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    thread_pool.Startup();
    unlink(noisepage::BenchmarkConfig::logfile_path.data());
    for (const auto &file : metrics::TransactionMetricRawData::FILES) unlink(std::string(file).c_str());
    auto *const metrics_manager = new metrics::MetricsManager;
    auto *const metrics_thread = new metrics::MetricsThread(common::ManagedPointer(metrics_manager), metrics_period_);
    metrics_manager->SetMetricSampleRate(metrics::MetricsComponent::TRANSACTION, 100);
    metrics_manager->EnableMetric(metrics::MetricsComponent::TRANSACTION);
    // we need transactions, TPCC database, and GC
    transaction::TimestampManager timestamp_manager;
    transaction::DeferredActionManager deferred_action_manager{common::ManagedPointer(&timestamp_manager)};
    transaction::TransactionManager txn_manager{common::ManagedPointer(&timestamp_manager),
                                                common::ManagedPointer(&deferred_action_manager),
                                                common::ManagedPointer(&buffer_pool_),
                                                true,
                                                false,
                                                common::ManagedPointer(log_manager_)};
    gc_ = new storage::GarbageCollector(common::ManagedPointer(&timestamp_manager),
                                        common::ManagedPointer(&deferred_action_manager),
                                        common::ManagedPointer(&txn_manager), DISABLED);
    catalog::Catalog catalog{common::ManagedPointer(&txn_manager), common::ManagedPointer(&block_store_),
                             common::ManagedPointer(gc_)};
    Builder tpcc_builder{common::ManagedPointer(&block_store_), common::ManagedPointer(&catalog),
                         common::ManagedPointer(&txn_manager)};

    // build the TPCC database
    auto *const tpcc_db = tpcc_builder.Build(storage::index::IndexType::HASHMAP);

    // prepare the workers
    workers.clear();
    for (uint32_t i = 0; i < noisepage::BenchmarkConfig::num_threads; i++) {
      workers.emplace_back(tpcc_db);
    }

    // populate the tables and indexes
    Loader::PopulateDatabase(common::ManagedPointer(&txn_manager), tpcc_db, &workers, &thread_pool);
    gc_thread_ = new storage::GarbageCollectorThread(common::ManagedPointer(gc_), gc_period_, nullptr);
    std::this_thread::sleep_for(std::chrono::seconds(2));  // Let GC clean up

    // run the TPCC workload to completion, timing the execution
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      for (uint32_t i = 0; i < noisepage::BenchmarkConfig::num_threads; i++) {
        thread_pool.SubmitTask([i, tpcc_db, &txn_manager, &precomputed_args, &workers, metrics_manager] {
          metrics_manager->RegisterThread();
          Workload(i, tpcc_db, &txn_manager, precomputed_args, &workers);
        });
      }
      thread_pool.WaitUntilAllFinished();
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

    // cleanup
    delete gc_thread_;
    catalog.TearDown();
    deferred_action_manager.FullyPerformGC(common::ManagedPointer(gc_), common::ManagedPointer(log_manager_));
    thread_pool.Shutdown();
    delete gc_;
    delete metrics_thread;
    delete metrics_manager;
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
    state.SetItemsProcessed(state.iterations() * num_precomputed_txns_per_worker_ *
                            noisepage::BenchmarkConfig::num_threads);
  }
}

// ----------------------------------------------------------------------------
// BENCHMARK REGISTRATION
// ----------------------------------------------------------------------------
// clang-format off
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
// clang-format on

}  // namespace noisepage::tpcc
