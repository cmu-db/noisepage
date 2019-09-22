#include <random>
#include <string>
#include <vector>
#include "common/macros.h"
#include "common/scoped_timer.h"
#include "common/worker_pool.h"
#include "storage/garbage_collector_thread.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_manager.h"
#include "util/tpcc/builder.h"
#include "util/tpcc/database.h"
#include "util/tpcc/loader.h"
#include "util/tpcc/worker.h"
#include "util/tpcc/workload.h"

namespace terrier::tpcc {

#define LOG_FILE_NAME "./tpcc.log"

/**
 * The behavior in these tests mimics that of /benchmark/integration/tpcc_benchmark.cpp. If something changes here, it
 * should probably change there as well.
 */
class TPCCTests : public TerrierTest {
 public:
  void SetUp() final {
    TerrierTest::SetUp();
    unlink(LOG_FILE_NAME);
  }

  void TearDown() final {
    TerrierTest::TearDown();
    unlink(LOG_FILE_NAME);
  }

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

  const int8_t num_threads_ = 4;  // defines the number of terminals (workers running txns) and warehouses for the
  // benchmark. Sometimes called scale factor
  const uint32_t num_precomputed_txns_per_worker_ = 10000;  // Number of txns to run per terminal (worker thread)
  TransactionWeights txn_weights_;                          // default txn_weights. See definition for values

  common::WorkerPool thread_pool_{static_cast<uint32_t>(num_threads_), {}};
  common::DedicatedThreadRegistry *thread_registry_ = nullptr;

  // Settings for log manager
  const uint64_t num_log_buffers_ = 100;
  const std::chrono::microseconds log_serialization_interval_{10};
  const std::chrono::milliseconds log_persist_interval_{20};
  const uint64_t log_persist_threshold_ = (1U << 20U);  // 1MB

  storage::GarbageCollector *gc_;
  storage::GarbageCollectorThread *gc_thread_ = nullptr;
  const std::chrono::milliseconds gc_period_{10};
};

// NOLINTNEXTLINE
TEST_F(TPCCTests, WithoutLoggingBwTreeIndexes) {
  // one TPCC worker = one TPCC terminal = one thread
  std::vector<Worker> workers;
  workers.reserve(num_threads_);

  // Reset the worker pool
  thread_pool_.Shutdown();
  thread_pool_.SetNumWorkers(num_threads_);
  thread_pool_.Startup();

  // we need transactions, TPCC database, and GC
  transaction::TimestampManager timestamp_manager;
  transaction::DeferredActionManager deferred_action_manager(&timestamp_manager);
  transaction::TransactionManager txn_manager(&timestamp_manager, &deferred_action_manager, &buffer_pool_, true,
                                              log_manager_);
  auto tpcc_builder = Builder(&block_store_);

  // Precompute all of the input arguments for every txn to be run. We want to avoid the overhead at benchmark time
  const auto precomputed_args =
      PrecomputeArgs(&generator_, txn_weights_, num_threads_, num_precomputed_txns_per_worker_);

  // build the TPCC database using only BwTrees
  auto *const tpcc_db = tpcc_builder.Build(storage::index::IndexType::BWTREE);

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

  // run the TPCC workload to completion
  for (int8_t i = 0; i < num_threads_; i++) {
    thread_pool_.SubmitTask([i, tpcc_db, &txn_manager, &precomputed_args, &workers] {
      Workload(i, tpcc_db, &txn_manager, precomputed_args, &workers);
    });
  }
  thread_pool_.WaitUntilAllFinished();

  // cleanup
  delete gc_thread_;
  delete gc_;
  delete tpcc_db;

  CleanUpVarlensInPrecomputedArgs(&precomputed_args);
}

// NOLINTNEXTLINE
TEST_F(TPCCTests, WithoutLoggingHashIndexes) {
  // one TPCC worker = one TPCC terminal = one thread
  std::vector<Worker> workers;
  workers.reserve(num_threads_);

  // Reset the worker pool
  thread_pool_.Shutdown();
  thread_pool_.SetNumWorkers(num_threads_);
  thread_pool_.Startup();

  // we need transactions, TPCC database, and GC
  transaction::TimestampManager timestamp_manager;
  transaction::DeferredActionManager deferred_action_manager(&timestamp_manager);
  transaction::TransactionManager txn_manager(&timestamp_manager, &deferred_action_manager, &buffer_pool_, true,
                                              log_manager_);
  auto tpcc_builder = Builder(&block_store_);

  // Precompute all of the input arguments for every txn to be run. We want to avoid the overhead at benchmark time
  const auto precomputed_args =
      PrecomputeArgs(&generator_, txn_weights_, num_threads_, num_precomputed_txns_per_worker_);

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

  // run the TPCC workload to completion
  for (int8_t i = 0; i < num_threads_; i++) {
    thread_pool_.SubmitTask([i, tpcc_db, &txn_manager, precomputed_args, &workers] {
      Workload(i, tpcc_db, &txn_manager, precomputed_args, &workers);
    });
  }
  thread_pool_.WaitUntilAllFinished();

  // cleanup
  delete gc_thread_;
  delete gc_;
  delete tpcc_db;

  CleanUpVarlensInPrecomputedArgs(&precomputed_args);
}

// NOLINTNEXTLINE
TEST_F(TPCCTests, WithLogging) {
  // one TPCC worker = one TPCC terminal = one thread
  std::vector<Worker> workers;
  workers.reserve(num_threads_);

  // Reset the worker pool
  thread_pool_.Shutdown();
  thread_pool_.SetNumWorkers(num_threads_);
  thread_pool_.Startup();

  thread_registry_ = new common::DedicatedThreadRegistry;
  // we need transactions, TPCC database, and GC
  log_manager_ =
      new storage::LogManager(LOG_FILE_NAME, num_log_buffers_, log_serialization_interval_, log_persist_interval_,
                              log_persist_threshold_, &buffer_pool_, common::ManagedPointer(thread_registry_));
  log_manager_->Start();
  transaction::TimestampManager timestamp_manager;
  transaction::DeferredActionManager deferred_action_manager(&timestamp_manager);
  transaction::TransactionManager txn_manager(&timestamp_manager, &deferred_action_manager, &buffer_pool_, true,
                                              log_manager_);
  auto tpcc_builder = Builder(&block_store_);

  // Precompute all of the input arguments for every txn to be run. We want to avoid the overhead at benchmark time
  const auto precomputed_args =
      PrecomputeArgs(&generator_, txn_weights_, num_threads_, num_precomputed_txns_per_worker_);

  // build the TPCC database using HashMaps where possible
  auto *const tpcc_db = tpcc_builder.Build(storage::index::IndexType::HASHMAP);

  // prepare the workers
  workers.clear();
  for (int8_t i = 0; i < num_threads_; i++) {
    workers.emplace_back(tpcc_db);
  }

  // populate the tables and indexes, as well as force log manager to log all changes
  Loader::PopulateDatabase(&txn_manager, &generator_, tpcc_db, workers);
  log_manager_->ForceFlush();

  // Let GC clean up
  gc_ = new storage::GarbageCollector(&timestamp_manager, &deferred_action_manager, &txn_manager, DISABLED);
  gc_thread_ = new storage::GarbageCollectorThread(gc_, gc_period_);
  Util::RegisterIndexesForGC(&(gc_thread_->GetGarbageCollector()), tpcc_db);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // run the TPCC workload to completion
  for (int8_t i = 0; i < num_threads_; i++) {
    thread_pool_.SubmitTask([i, tpcc_db, &txn_manager, precomputed_args, &workers] {
      Workload(i, tpcc_db, &txn_manager, precomputed_args, &workers);
    });
  }
  thread_pool_.WaitUntilAllFinished();

  // cleanup
  log_manager_->PersistAndStop();
  delete log_manager_;
  delete gc_thread_;
  delete gc_;
  delete thread_registry_;
  delete tpcc_db;

  CleanUpVarlensInPrecomputedArgs(&precomputed_args);
}

}  // namespace terrier::tpcc
