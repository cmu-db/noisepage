#include <vector>
#include "util/transaction_test_util.h"
#include "storage/garbage_collector.h"
#include "gtest/gtest.h"

namespace terrier {
class LargeGCTests : public TerrierTest {
 public:
  void StartGC(transaction::TransactionManager *txn_manager, uint32_t gc_period_milli) {
    gc_ = new storage::GarbageCollector(txn_manager);
    run_gc_ = true;
    gc_thread_ = std::thread([gc_period_milli, this] {
      GCThreadLoop(gc_period_milli);
    });
  }

  void EndGC() {
    run_gc_ = false;
    gc_thread_.join();
    // Make sure all garbage is collected. This take 2 runs for unlink and deallocate
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    delete gc_;
  }

  storage::BlockStore block_store_{1000};
  common::ObjectPool<storage::BufferSegment> buffer_pool_{1000};
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
  const uint32_t num_iterations = 1;
  const uint16_t max_columns = 2;
  const uint32_t initial_table_size = 1000;
  const uint32_t txn_length = 10;
  const uint32_t num_txns = 1000;
  const uint32_t batch_size = 100;
  const std::vector<double> update_select_ratio = {0.3, 0.7};
  const uint32_t num_concurrent_txns = 4;
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested(max_columns,
                                      initial_table_size,
                                      txn_length,
                                      update_select_ratio,
                                      &block_store_,
                                      &buffer_pool_,
                                      &generator_,
                                      true,
                                      true);
    StartGC(tested.GetTxnManager(), 10);
    for (uint32_t batch = 0; batch * batch_size < num_txns; batch++) {
      auto result = tested.SimulateOltp(batch_size, num_concurrent_txns);
      paused_ = true;
      tested.CheckReadsCorrect(&result.first);
      for (auto w : result.first) delete w;
      for (auto w : result.second) delete w;
      paused_ = false;
    }
    EndGC();
  }
}
}  // namespace terrier
