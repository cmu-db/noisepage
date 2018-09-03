#include "execution/sql_table.h"
#include "storage/data_table.h"
#include "storage/log_manager.h"
#include "transaction/transaction_manager.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"
#include "storage/garbage_collector.h"
#include "gtest/gtest.h"

namespace terrier {
class LargeLoggingTests : public TerrierTest {
 public:
  void StartLogging(uint32_t log_period_milli) {
    logging_ = true;
    log_thread_ = std::thread([log_period_milli, this] { LogThreadLoop(log_period_milli); });
  }

  void EndLogging() {
    logging_ = false;
    log_thread_.join();
    log_manager_.Process();
    log_manager_.Flush();
  }

  void StartGC(transaction::TransactionManager *txn_manager, uint32_t gc_period_milli) {
    gc_ = new storage::GarbageCollector(txn_manager);
    run_gc_ = true;
    gc_thread_ = std::thread([gc_period_milli, this] { GCThreadLoop(gc_period_milli); });
  }

  void EndGC() {
    run_gc_ = false;
    gc_thread_.join();
    // Make sure all garbage is collected. This take 2 runs for unlink and deallocate
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    delete gc_;
  }

  std::default_random_engine generator_;
  storage::RecordBufferSegmentPool pool_{1000, 100};
  storage::BlockStore block_store_{100, 100};
  storage::LogManager log_manager_{"test.txt", &pool_};
  std::thread log_thread_;
  bool logging_;
  volatile bool run_gc_ = false;
  std::thread gc_thread_;
  storage::GarbageCollector *gc_;

 private:
  void LogThreadLoop(uint32_t log_period_milli) {
    while (logging_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(log_period_milli));
      log_manager_.Process();
    }
  }

  void GCThreadLoop(uint32_t gc_period_milli) {
    while (run_gc_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(gc_period_milli));
      gc_->PerformGarbageCollection();
    }
  }
};

// NOLINTNEXTLINE
TEST_F(LargeLoggingTests, LargeLogTest) {
  LargeTransactionTestObject
      tested(5, 100, 5, {0.3, 0.7}, &block_store_, &pool_, &generator_, true, false, &log_manager_);
  StartLogging(10);
  StartGC(tested.GetTxnManager(), 10);
  tested.SimulateOltp(100, 4);
  EndGC();
  EndLogging();
}
}  // namespace terrier
