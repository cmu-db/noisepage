#include <random>
#include <vector>
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

#define LOG_FILE_NAME "./tpcc.log"

class TPCCTests : public TerrierTest {
 public:
  void SetUp() final { TerrierTest::SetUp(); }

  void StartLogging() {
    logging_ = true;
    log_thread_ = std::thread([this] { LogThreadLoop(); });
  }

  void EndLogging() {
    logging_ = false;
    log_thread_.join();
    log_manager_->Shutdown();
  }

  void TearDown() final {
    TerrierTest::TearDown();
    if (logging_enabled_) unlink(LOG_FILE_NAME);
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
  storage::LogManager *log_manager_ = nullptr;

  const int8_t num_threads_ = 4;  // defines the number of terminals (workers running txns) and warehouses for the
  // benchmark. Sometimes called scale factor
  const uint32_t num_precomputed_txns_per_worker_ = 10000;  // Number of txns to run per terminal (worker thread)
  TransactionWeights txn_weights;                           // default txn_weights. See definition for values

  // Toggle WAL on or off for the test
  const bool logging_enabled_ = false;

  common::WorkerPool thread_pool_{static_cast<uint32_t>(num_threads_), {}};

  storage::GarbageCollectorThread *gc_thread_ = nullptr;
  const std::chrono::milliseconds gc_period_{10};

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
};

// NOLINTNEXTLINE
TEST_F(TPCCTests, TPCCTest) {
  // one TPCC worker = one TPCC terminal = one thread
  std::vector<Worker> workers;
  workers.reserve(num_threads_);

  // Reset the worker pool
  thread_pool_.Shutdown();
  thread_pool_.SetNumWorkers(num_threads_);
  thread_pool_.Startup();

  // we need transactions, TPCC database, and GC
  if (logging_enabled_) log_manager_ = new storage::LogManager(LOG_FILE_NAME, &buffer_pool_);
  transaction::TransactionManager txn_manager(&buffer_pool_, true, log_manager_);
  auto tpcc_builder = Builder(&block_store_);

  // Precompute all of the input arguments for every txn to be run. We want to avoid the overhead at benchmark time
  const auto precomputed_args =
      PrecomputeArgs(&generator_, txn_weights, num_threads_, num_precomputed_txns_per_worker_);

  // build the TPCC database
  auto *const tpcc_db = tpcc_builder.Build();

  // prepare the workers
  workers.clear();
  for (int8_t i = 0; i < num_threads_; i++) {
    workers.emplace_back(tpcc_db);
  }

  // populate the tables and indexes
  Loader::PopulateDatabase(&txn_manager, &generator_, tpcc_db, workers);
  if (logging_enabled_) log_manager_->Process();  // log all of the Inserts from table creation
  gc_thread_ = new storage::GarbageCollectorThread(&txn_manager, gc_period_);
  if (logging_enabled_) StartLogging();
  std::this_thread::sleep_for(std::chrono::seconds(2));  // Let GC clean up

  // define the TPCC workload
  auto tpcc_workload = [&](const int8_t worker_id) {
    auto new_order = NewOrder(tpcc_db);
    auto payment = Payment(tpcc_db);
    auto order_status = OrderStatus(tpcc_db);
    auto delivery = Delivery(tpcc_db);
    auto stock_level = StockLevel(tpcc_db);

    for (uint32_t i = 0; i < num_precomputed_txns_per_worker_; i++) {
      const auto &txn_args = precomputed_args[worker_id][i];
      switch (txn_args.type) {
        case TransactionType::NewOrder: {
          new_order.Execute(&txn_manager, tpcc_db, &workers[worker_id], txn_args);
          break;
        }
        case TransactionType::Payment: {
          payment.Execute(&txn_manager, tpcc_db, &workers[worker_id], txn_args);
          break;
        }
        case TransactionType::OrderStatus: {
          order_status.Execute(&txn_manager, tpcc_db, &workers[worker_id], txn_args);
          break;
        }
        case TransactionType::Delivery: {
          delivery.Execute(&txn_manager, tpcc_db, &workers[worker_id], txn_args);
          break;
        }
        case TransactionType::StockLevel: {
          stock_level.Execute(&txn_manager, tpcc_db, &workers[worker_id], txn_args);
          break;
        }
        default:
          throw std::runtime_error("Unexpected transaction type.");
      }
    }
  };

  // run the TPCC workload to completion
  for (int8_t i = 0; i < num_threads_; i++) {
    thread_pool_.SubmitTask([i, &tpcc_workload] { tpcc_workload(i); });
  }
  thread_pool_.WaitUntilAllFinished();

  // cleanup
  if (logging_enabled_) EndLogging();
  delete gc_thread_;
  delete tpcc_db;
  if (logging_enabled_) delete log_manager_;

  // Clean up the buffers from any non-inlined VarlenEntrys in the precomputed args
  for (const auto &worker_id : precomputed_args) {
    for (const auto &args : worker_id) {
      if ((args.type == TransactionType::Payment || args.type == TransactionType::OrderStatus) && args.use_c_last &&
          !args.c_last.IsInlined()) {
        delete[] args.c_last.Content();
      }
    }
  }
}

}  // namespace terrier::tpcc
