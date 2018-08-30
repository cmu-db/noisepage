#include <chrono>
#include <vector>
#include "gtest/gtest.h"
#include "storage/garbage_collector.h"
#include "util/transaction_test_util.h"

namespace terrier {
class LargeGCTests : public TerrierTest {
 public:
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

  storage::BlockStore block_store_{1000, 1000};
  common::ObjectPool<storage::BufferSegment> buffer_pool_{1000, 1000};
  std::default_random_engine generator_;
  volatile bool run_gc_ = false;
  volatile bool paused_ = false;
  std::thread gc_thread_;
  storage::GarbageCollector *gc_ = nullptr;

 private:
  void GCThreadLoop(uint32_t gc_period_milli) {
    while (run_gc_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(gc_period_milli));
      if (!paused_) gc_->PerformGarbageCollection();
    }
  }
};

// This test case generates random update-selects in concurrent transactions on a pre-populated database.
// Each transaction logs their operations locally. At the end of the run, we can reconstruct the snapshot of
// a database using the updates at every timestamp, and compares the reads with the reconstructed snapshot versions
// to make sure they are the same.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, MixedReadWriteWithGC) {
  const uint32_t num_iterations = 5;
  const uint16_t max_columns = 2;
  const uint32_t initial_table_size = 1000000;
  const uint32_t txn_length = 10;
  const uint32_t num_txns = 1000000;
  const std::vector<double> update_select_ratio = {0.4, 0.6};
  const uint32_t num_concurrent_txns = TestThreadPool::HardwareConcurrency();
  double unlink_time = 0.0;
  double deallocation_time = 0.0;
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested(max_columns, initial_table_size, txn_length, update_select_ratio, &block_store_,
                                      &buffer_pool_, &generator_, true, false);
    tested.SimulateOltp(num_txns, num_concurrent_txns);

    gc_ = new storage::GarbageCollector(tested.GetTxnManager());
    auto start = std::chrono::high_resolution_clock::now();
    auto gc_result = gc_->PerformGarbageCollection();
    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
    std::cout << gc_result.second << " txns unlinked in " << elapsed_seconds.count() << " seconds" << std::endl;
    unlink_time += elapsed_seconds.count();

    start = std::chrono::high_resolution_clock::now();
    gc_result = gc_->PerformGarbageCollection();
    end = std::chrono::high_resolution_clock::now();
    elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
    std::cout << gc_result.first << " txns deallocated in " << elapsed_seconds.count() << " seconds" << std::endl;
    deallocation_time += elapsed_seconds.count();
  }

  std::cout << "mean unlink time: " << unlink_time / num_iterations << " seconds" << std::endl;
  std::cout << "mean deallocation time: " << deallocation_time / num_iterations << " seconds" << std::endl;
}
}  // namespace terrier
