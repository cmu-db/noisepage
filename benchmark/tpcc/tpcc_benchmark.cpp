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
#include "tpcc/transactions.h"
#include "tpcc/worker.h"
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
  storage::BlockStore block_store_{blockstore_size_limit_, blockstore_reuse_limit_};
  storage::RecordBufferSegmentPool buffer_pool_{buffersegment_size_limit_, buffersegment_reuse_limit_};
  std::default_random_engine generator_;
  storage::LogManager *log_manager_ = nullptr;

 private:
  std::thread log_thread_;
  volatile bool logging_ = false;
  const std::chrono::milliseconds log_period_milli_{40};

  void LogThreadLoop() {
    while (logging_) {
      std::this_thread::sleep_for(log_period_milli_);
      log_manager_->Process();
    }
  }

  std::thread gc_thread_;
  storage::GarbageCollector *gc_ = nullptr;
  volatile bool run_gc_ = false;
  const std::chrono::milliseconds gc_period_{40};

  void GCThreadLoop() {
    while (run_gc_) {
      std::this_thread::sleep_for(gc_period_);
      gc_->PerformGarbageCollection();
    }
  }
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TPCCBenchmark, Basic)(benchmark::State &state) {
  const uint32_t num_txns = 100000;

  // NOLINTNEXTLINE
  for (auto _ : state) {
    //    log_manager_ = new storage::LogManager(LOG_FILE_NAME, &buffer_pool_);
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    auto tpcc_builder = tpcc::Builder(&block_store_);
    auto *const tpcc_db = tpcc_builder.Build();

    tpcc::Worker worker_buffers1(tpcc_db);
    tpcc::Worker worker_buffers2(tpcc_db);
    tpcc::Worker worker_buffers3(tpcc_db);
    tpcc::Worker worker_buffers4(tpcc_db);

    tpcc::Loader::PopulateDatabase(&txn_manager, &generator_, tpcc_db, &worker_buffers1);
    //    log_manager_->Process();  // log all of the Inserts from table creation
    StartGC(&txn_manager);
    //    StartLogging();

    std::vector<tpcc::NewOrderArgs> args1;
    args1.reserve(num_txns);
    std::vector<tpcc::NewOrderArgs> args2;
    args2.reserve(num_txns);
    std::vector<tpcc::NewOrderArgs> args3;
    args3.reserve(num_txns);
    std::vector<tpcc::NewOrderArgs> args4;
    args4.reserve(num_txns);

    for (uint32_t i = 0; i < num_txns; i++) {
      args1.push_back(tpcc::BuildNewOrderArgs(&generator_, 1));
      args2.push_back(tpcc::BuildNewOrderArgs(&generator_, 2));
      args3.push_back(tpcc::BuildNewOrderArgs(&generator_, 3));
      args4.push_back(tpcc::BuildNewOrderArgs(&generator_, 4));
    }

    tpcc::NewOrder new_order(tpcc_db);
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);

      std::thread worker1([&] {
        for (const auto &arg : args1) {
          new_order.Execute(&txn_manager, &generator_, tpcc_db, &worker_buffers1, arg);
        }
      });
      std::thread worker2([&] {
        for (const auto &arg : args2) {
          new_order.Execute(&txn_manager, &generator_, tpcc_db, &worker_buffers2, arg);
        }
      });
      std::thread worker3([&] {
        for (const auto &arg : args3) {
          new_order.Execute(&txn_manager, &generator_, tpcc_db, &worker_buffers3, arg);
        }
      });
      std::thread worker4([&] {
        for (const auto &arg : args4) {
          new_order.Execute(&txn_manager, &generator_, tpcc_db, &worker_buffers4, arg);
        }
      });

      worker1.join();
      worker2.join();
      worker3.join();
      worker4.join();
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    //    EndLogging();
    EndGC();
    delete tpcc_db;
    //    delete log_manager_;
  }
  state.SetItemsProcessed(state.iterations() * num_txns * 4);
}

BENCHMARK_REGISTER_F(TPCCBenchmark, Basic)->Unit(benchmark::kMillisecond)->UseManualTime();
}  // namespace terrier
