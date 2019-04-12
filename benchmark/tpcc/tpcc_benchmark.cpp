#include <random>
#include <vector>
#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "common/worker_pool.h"
#include "storage/garbage_collector.h"
#include "storage/storage_defs.h"
#include "tpcc/builder.h"
#include "tpcc/database.h"
#include "tpcc/loader.h"
#include "tpcc/new_order.h"
#include "tpcc/worker.h"
#include "tpcc/workload.h"
#include "transaction/transaction_manager.h"

namespace terrier {

#define LOG_FILE_NAME "./tpcc.log"

class TPCCBenchmark : public benchmark::Fixture {
 public:
  void StartLogging() {
    logging_ = true;
    log_thread_ = std::thread([this] { LogThreadLoop(); });
  }

  void EndLogging() {
    logging_ = false;
    log_thread_.join();
    log_manager_->Shutdown();
  }

  void StartGC(transaction::TransactionManager *const txn_manager) {
    gc_ = new storage::GarbageCollector(txn_manager);
    run_gc_ = true;
    gc_thread_ = std::thread([this] { GCThreadLoop(); });
  }

  void EndGC() {
    run_gc_ = false;
    gc_thread_.join();
    // Make sure all garbage is collected. This take 2 runs for unlink and deallocate
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    delete gc_;
  }

  const uint64_t blockstore_size_limit_ = 1000;
  const uint64_t blockstore_reuse_limit_ = 1000;
  const uint64_t buffersegment_size_limit_ = 1000000;
  const uint64_t buffersegment_reuse_limit_ = 1000000;
  const uint32_t num_threads_ = 4;
  const uint32_t num_precomputed_txns_per_worker_ = 100000;
  storage::BlockStore block_store_{blockstore_size_limit_, blockstore_reuse_limit_};
  storage::RecordBufferSegmentPool buffer_pool_{buffersegment_size_limit_, buffersegment_reuse_limit_};
  std::default_random_engine generator_;
  storage::LogManager *log_manager_ = nullptr;
  common::WorkerPool thread_pool_{num_threads_, {}};

 private:
  std::thread log_thread_;
  volatile bool logging_ = false;
  const std::chrono::milliseconds log_period_milli_{10};

  void LogThreadLoop() {
    while (logging_) {
      std::this_thread::sleep_for(log_period_milli_);
      log_manager_->Process();
    }
  }

  std::thread gc_thread_;
  storage::GarbageCollector *gc_ = nullptr;
  volatile bool run_gc_ = false;
  const std::chrono::milliseconds gc_period_{10};

  void GCThreadLoop() {
    while (run_gc_) {
      std::this_thread::sleep_for(gc_period_);
      gc_->PerformGarbageCollection();
    }
  }
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TPCCBenchmark, Basic)(benchmark::State &state) {
  // one TPCC worker = one TPCC terminal = one thread
  std::vector<tpcc::Worker> workers;
  workers.reserve(num_threads_);

  thread_pool_.Shutdown();
  thread_pool_.SetNumWorkers(num_threads_);
  thread_pool_.Startup();

  // we need transactions, TPCC database, and GC
  transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
  auto tpcc_builder = tpcc::Builder(&block_store_);
  StartGC(&txn_manager);

  // random number generation is slow, so we precompute the args
  std::vector<std::vector<tpcc::TransactionArgs>> precomputed_args;
  precomputed_args.reserve(workers.size());

  for (uint32_t warehouse_id = 1; warehouse_id <= num_threads_; warehouse_id++) {
    std::vector<tpcc::TransactionArgs> txns;
    txns.reserve(num_precomputed_txns_per_worker_);
    for (uint32_t i = 0; i < num_precomputed_txns_per_worker_; i++) {
      // TODO(WAN): support other transaction types
      txns.emplace_back(tpcc::BuildNewOrderArgs(&generator_, warehouse_id));
    }
    precomputed_args.emplace_back(txns);
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    // build the TPCC database
    //    log_manager_ = new storage::LogManager(LOG_FILE_NAME, &buffer_pool_);
    auto *const tpcc_db = tpcc_builder.Build();

    // prepare the workers
    workers.clear();
    for (uint32_t i = 0; i < num_threads_; i++) {
      workers.emplace_back(tpcc_db);
    }

    // TODO(WAN): I have a stashed commit for multi-threaded loaders, but we don't want that right now
    tpcc::Loader::PopulateDatabase(&txn_manager, &generator_, tpcc_db, &workers[0]);
    //    log_manager_->Process();  // log all of the Inserts from table creation
    StartGC(&txn_manager);
    //    StartLogging();

    // define the TPCC workload
    auto tpcc_workload = [&](uint32_t worker_id) {
      auto new_order = tpcc::NewOrder(tpcc_db);
      for (uint32_t i = 0; i < num_precomputed_txns_per_worker_; i++) {
        // TODO(WAN): eventually switch on transaction type
        auto new_order_committed =
            new_order.Execute(&txn_manager, &generator_, tpcc_db, &workers[worker_id], precomputed_args[worker_id][i]);
        workers[worker_id].num_committed_txns += new_order_committed ? 1 : 0;
      }
    };

    // run the TPCC workload to completion
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      for (uint32_t i = 0; i < num_threads_; i++) {
        thread_pool_.SubmitTask([i, &tpcc_workload] { tpcc_workload(i); });
      }
      thread_pool_.WaitUntilAllFinished();
    }

    // figure out how many transactions committed
    uint32_t num_items_processed = state.items_processed();
    for (uint32_t i = 0; i < num_threads_; i++) {
      num_items_processed += workers[i].num_committed_txns;
    }

    // update benchmark state
    state.SetItemsProcessed(num_items_processed);
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);

    // cleanup
    //    EndLogging();
    EndGC();
    delete tpcc_db;
    //    delete log_manager_;
  }
}

BENCHMARK_REGISTER_F(TPCCBenchmark, Basic)->Unit(benchmark::kMillisecond)->UseManualTime();
}  // namespace terrier
