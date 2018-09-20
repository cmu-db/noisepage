#include <vector>
#include "gtest/gtest.h"
#include "storage/garbage_collector.h"
#include "util/transaction_test_util.h"

namespace terrier {
class LargeGCTests : public TerrierTest {
 public:
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

  const uint32_t num_iterations = 10;
  const uint16_t max_columns = 20;
  const uint32_t initial_table_size = 1000;
  const uint32_t num_txns = 1000;
  const uint32_t batch_size = 100;
  storage::BlockStore block_store_{1000, 1000};
  common::ObjectPool<storage::BufferSegment> buffer_pool_{10000, 10000};
  std::default_random_engine generator_;
  volatile bool run_gc_ = false;
  volatile bool gc_paused_ = false;
  std::thread gc_thread_;
  storage::GarbageCollector *gc_ = nullptr;

 private:
  const std::chrono::milliseconds gc_period_{10};

  void GCThreadLoop() {
    while (run_gc_) {
      std::this_thread::sleep_for(gc_period_);
      if (!gc_paused_) gc_->PerformGarbageCollection();
    }
  }
};

// These test cases generates random update-selects in concurrent transactions on a pre-populated database.
// Each transaction logs their operations locally. At the end of the run, we can reconstruct the snapshot of
// a database using the updates at every timestamp, and compares the reads with the reconstructed snapshot versions
// to make sure they are the same.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, MixedReadWriteWithGC) {
  const uint32_t txn_length = 10;
  const std::vector<double> update_select_ratio = {0.5, 0.5};
  const uint32_t num_concurrent_txns = TestThreadPool::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested(max_columns, initial_table_size, txn_length, update_select_ratio, &block_store_,
                                      &buffer_pool_, &generator_, true, true);
    StartGC(tested.GetTxnManager());
    for (uint32_t batch = 0; batch * batch_size < num_txns; batch++) {
      auto result = tested.SimulateOltp(batch_size, num_concurrent_txns);
      gc_paused_ = true;
      tested.CheckReadsCorrect(&result.first);
      for (auto w : result.first) delete w;
      for (auto w : result.second) delete w;
      gc_paused_ = false;
    }
    EndGC();
  }
}

// Double the thread count to force more thread swapping and try to capture unexpected races
// NOLINTNEXTLINE
TEST_F(LargeGCTests, MixedReadWriteHighThreadWithGC) {
  const uint32_t txn_length = 10;
  const std::vector<double> update_select_ratio = {0.5, 0.5};
  const uint32_t num_concurrent_txns = 2 * TestThreadPool::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested(max_columns, initial_table_size, txn_length, update_select_ratio, &block_store_,
                                      &buffer_pool_, &generator_, true, true);
    StartGC(tested.GetTxnManager());
    for (uint32_t batch = 0; batch * batch_size < num_txns; batch++) {
      auto result = tested.SimulateOltp(batch_size, num_concurrent_txns);
      gc_paused_ = true;
      tested.CheckReadsCorrect(&result.first);
      for (auto w : result.first) delete w;
      for (auto w : result.second) delete w;
      gc_paused_ = false;
    }
    EndGC();
  }
}

// This test targets the scenario of low abort rate (~1% of num_txns) and high throughput of statements
// NOLINTNEXTLINE
TEST_F(LargeGCTests, LowAbortHighThroughputWithGC) {
  const uint32_t txn_length = 1;
  const std::vector<double> update_select_ratio = {0.5, 0.5};
  const uint32_t num_concurrent_txns = TestThreadPool::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested(max_columns, initial_table_size, txn_length, update_select_ratio, &block_store_,
                                      &buffer_pool_, &generator_, true, true);
    StartGC(tested.GetTxnManager());
    for (uint32_t batch = 0; batch * batch_size < num_txns; batch++) {
      auto result = tested.SimulateOltp(batch_size, num_concurrent_txns);
      gc_paused_ = true;
      tested.CheckReadsCorrect(&result.first);
      for (auto w : result.first) delete w;
      for (auto w : result.second) delete w;
      gc_paused_ = false;
    }
    EndGC();
  }
}

// This test is a duplicate of LowAbortHighThroughputWithGC but with higher number of thread swapouts
// NOLINTNEXTLINE
TEST_F(LargeGCTests, LowAbortHighThroughputHighThreadWithGC) {
  const uint32_t txn_length = 1;
  const std::vector<double> update_select_ratio = {0.5, 0.5};
  const uint32_t num_concurrent_txns = 2 * TestThreadPool::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested(max_columns, initial_table_size, txn_length, update_select_ratio, &block_store_,
                                      &buffer_pool_, &generator_, true, true);
    StartGC(tested.GetTxnManager());
    for (uint32_t batch = 0; batch * batch_size < num_txns; batch++) {
      auto result = tested.SimulateOltp(batch_size, num_concurrent_txns);
      gc_paused_ = true;
      tested.CheckReadsCorrect(&result.first);
      for (auto w : result.first) delete w;
      for (auto w : result.second) delete w;
      gc_paused_ = false;
    }
    EndGC();
  }
}

// This test is similar to the previous one, but with a higher ratio of updates
// and longer transactions leading to more aborts.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, HighAbortRateWithGC) {
  const uint32_t txn_length = 40;
  const std::vector<double> update_select_ratio = {0.8, 0.2};
  const uint32_t num_concurrent_txns = TestThreadPool::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested(max_columns, initial_table_size, txn_length, update_select_ratio, &block_store_,
                                      &buffer_pool_, &generator_, true, true);
    StartGC(tested.GetTxnManager());
    for (uint32_t batch = 0; batch * batch_size < num_txns; batch++) {
      auto result = tested.SimulateOltp(batch_size, num_concurrent_txns);
      gc_paused_ = true;
      tested.CheckReadsCorrect(&result.first);
      for (auto w : result.first) delete w;
      for (auto w : result.second) delete w;
      gc_paused_ = false;
    }
    EndGC();
  }
}

// This test duplicates the previous one with a higher number of thread swapouts.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, HighAbortRateHighThreadWithGC) {
  const uint32_t txn_length = 40;
  const std::vector<double> update_select_ratio = {0.8, 0.2};
  const uint32_t num_concurrent_txns = 2 * TestThreadPool::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested(max_columns, initial_table_size, txn_length, update_select_ratio, &block_store_,
                                      &buffer_pool_, &generator_, true, true);
    StartGC(tested.GetTxnManager());
    for (uint32_t batch = 0; batch * batch_size < num_txns; batch++) {
      auto result = tested.SimulateOltp(batch_size, num_concurrent_txns);
      gc_paused_ = true;
      tested.CheckReadsCorrect(&result.first);
      for (auto w : result.first) delete w;
      for (auto w : result.second) delete w;
      gc_paused_ = false;
    }
    EndGC();
  }
}

// This test attempts to simulate a TPC-C-like scenario.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, TPCCWithGC) {
  const uint32_t txn_length = 5;
  const std::vector<double> update_select_ratio = {0.4, 0.6};
  const uint32_t num_concurrent_txns = TestThreadPool::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested(max_columns, initial_table_size, txn_length, update_select_ratio, &block_store_,
                                      &buffer_pool_, &generator_, true, true);
    StartGC(tested.GetTxnManager());
    for (uint32_t batch = 0; batch * batch_size < num_txns; batch++) {
      auto result = tested.SimulateOltp(batch_size, num_concurrent_txns);
      gc_paused_ = true;
      tested.CheckReadsCorrect(&result.first);
      for (auto w : result.first) delete w;
      for (auto w : result.second) delete w;
      gc_paused_ = false;
    }
    EndGC();
  }
}

// This test duplicates the previous one with a higher number of thread swapouts.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, TPCCHighThreadWithGC) {
  const uint32_t txn_length = 5;
  const std::vector<double> update_select_ratio = {0.4, 0.6};
  const uint32_t num_concurrent_txns = 2 * TestThreadPool::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested(max_columns, initial_table_size, txn_length, update_select_ratio, &block_store_,
                                      &buffer_pool_, &generator_, true, true);
    StartGC(tested.GetTxnManager());
    for (uint32_t batch = 0; batch * batch_size < num_txns; batch++) {
      auto result = tested.SimulateOltp(batch_size, num_concurrent_txns);
      gc_paused_ = true;
      tested.CheckReadsCorrect(&result.first);
      for (auto w : result.first) delete w;
      for (auto w : result.second) delete w;
      gc_paused_ = false;
    }
    EndGC();
  }
}
}  // namespace terrier
