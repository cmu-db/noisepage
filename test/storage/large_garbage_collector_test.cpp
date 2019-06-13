#include <vector>
#include "gtest/gtest.h"
#include "storage/garbage_collector_thread.h"
#include "util/transaction_test_util.h"

namespace terrier {
class LargeGCTests : public TerrierTest {
 public:
  const uint32_t num_iterations = 10;
  const uint16_t max_columns = 20;
  const uint32_t initial_table_size = 1000;
  const uint32_t num_txns = 1000;
  const uint32_t batch_size = 100;
  storage::BlockStore block_store_{1000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{10000, 10000};
  std::default_random_engine generator_;
  transaction::TimestampManager timestamp_manager_;
  transaction::DeferredActionManager deferred_action_manager_{&timestamp_manager_};
  storage::VersionChainGC version_chain_gc_{&deferred_action_manager_};
  const std::chrono::milliseconds gc_period{10};
};

// These test cases generates random update-selects in concurrent transactions on a pre-populated database.
// Each transaction logs their operations locally. At the end of the run, we can reconstruct the snapshot of
// a database using the updates at every timestamp, and compares the reads with the reconstructed snapshot versions
// to make sure they are the same.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, MixedReadWriteWithGC) {
  const uint32_t txn_length = 10;
  const std::vector<double> update_select_ratio = {0.5, 0.5};
  const uint32_t num_concurrent_txns = MultiThreadTestUtil::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested = LargeTransactionTestObject::Builder()
                                            .SetMaxColumns(max_columns)
                                            .SetInitialTableSize(initial_table_size)
                                            .SetTxnLength(txn_length)
                                            .SetUpdateSelectRatio(update_select_ratio)
                                            .SetBlockStore(&block_store_)
                                            .SetBufferPool(&buffer_pool_)
                                            .SetGenerator(&generator_)
                                            .SetTimestampManager(&timestamp_manager_)
                                            .SetDeferredActionManager(&deferred_action_manager_)
                                            .SetVersionChainGC(&version_chain_gc_)
                                            .SetBookkeeping(true)
                                            .SetVarlenAllowed(true)
                                            .build();
    storage::GarbageCollectorThread gc_thread(&deferred_action_manager_, DISABLED, gc_period);
    for (uint32_t batch = 0; batch * batch_size < num_txns; batch++) {
      auto result = tested.SimulateOltp(batch_size, num_concurrent_txns);
      gc_thread.PauseGC();
      tested.CheckReadsCorrect(&result.first);
      for (auto w : result.first) delete w;
      for (auto w : result.second) delete w;
      gc_thread.ResumeGC();
    }
  }
}

// Double the thread count to force more thread swapping and try to capture unexpected races
// NOLINTNEXTLINE
TEST_F(LargeGCTests, MixedReadWriteHighThreadWithGC) {
  const uint32_t txn_length = 10;
  const std::vector<double> update_select_ratio = {0.5, 0.5};
  const uint32_t num_concurrent_txns = 2 * MultiThreadTestUtil::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested = LargeTransactionTestObject::Builder()
                                            .SetMaxColumns(max_columns)
                                            .SetInitialTableSize(initial_table_size)
                                            .SetTxnLength(txn_length)
                                            .SetUpdateSelectRatio(update_select_ratio)
                                            .SetBlockStore(&block_store_)
                                            .SetBufferPool(&buffer_pool_)
                                            .SetGenerator(&generator_)
                                            .SetTimestampManager(&timestamp_manager_)
                                            .SetDeferredActionManager(&deferred_action_manager_)
                                            .SetVersionChainGC(&version_chain_gc_)
                                            .SetBookkeeping(true)
                                            .SetVarlenAllowed(true)
                                            .build();
    storage::GarbageCollectorThread gc_thread(&deferred_action_manager_, DISABLED, gc_period);
    for (uint32_t batch = 0; batch * batch_size < num_txns; batch++) {
      auto result = tested.SimulateOltp(batch_size, num_concurrent_txns);
      gc_thread.PauseGC();
      tested.CheckReadsCorrect(&result.first);
      for (auto w : result.first) delete w;
      for (auto w : result.second) delete w;
      gc_thread.ResumeGC();
    }
  }
}

// This test targets the scenario of low abort rate (~1% of num_txns) and high throughput of statements
// NOLINTNEXTLINE
TEST_F(LargeGCTests, LowAbortHighThroughputWithGC) {
  const uint32_t txn_length = 1;
  const std::vector<double> update_select_ratio = {0.5, 0.5};
  const uint32_t num_concurrent_txns = MultiThreadTestUtil::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested = LargeTransactionTestObject::Builder()
                                            .SetMaxColumns(max_columns)
                                            .SetInitialTableSize(initial_table_size)
                                            .SetTxnLength(txn_length)
                                            .SetUpdateSelectRatio(update_select_ratio)
                                            .SetBlockStore(&block_store_)
                                            .SetBufferPool(&buffer_pool_)
                                            .SetGenerator(&generator_)
                                            .SetTimestampManager(&timestamp_manager_)
                                            .SetDeferredActionManager(&deferred_action_manager_)
                                            .SetVersionChainGC(&version_chain_gc_)
                                            .SetBookkeeping(true)
                                            .SetVarlenAllowed(true)
                                            .build();
    storage::GarbageCollectorThread gc_thread(&deferred_action_manager_, DISABLED, gc_period);
    for (uint32_t batch = 0; batch * batch_size < num_txns; batch++) {
      auto result = tested.SimulateOltp(batch_size, num_concurrent_txns);
      gc_thread.PauseGC();
      tested.CheckReadsCorrect(&result.first);
      for (auto w : result.first) delete w;
      for (auto w : result.second) delete w;
      gc_thread.ResumeGC();
    }
  }
}

// This test is a duplicate of LowAbortHighThroughputWithGC but with higher number of thread swapouts
// NOLINTNEXTLINE
TEST_F(LargeGCTests, LowAbortHighThroughputHighThreadWithGC) {
  const uint32_t txn_length = 1;
  const std::vector<double> update_select_ratio = {0.5, 0.5};
  const uint32_t num_concurrent_txns = 2 * MultiThreadTestUtil::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested = LargeTransactionTestObject::Builder()
                                            .SetMaxColumns(max_columns)
                                            .SetInitialTableSize(initial_table_size)
                                            .SetTxnLength(txn_length)
                                            .SetUpdateSelectRatio(update_select_ratio)
                                            .SetBlockStore(&block_store_)
                                            .SetBufferPool(&buffer_pool_)
                                            .SetGenerator(&generator_)
                                            .SetTimestampManager(&timestamp_manager_)
                                            .SetDeferredActionManager(&deferred_action_manager_)
                                            .SetVersionChainGC(&version_chain_gc_)
                                            .SetBookkeeping(true)
                                            .SetVarlenAllowed(true)
                                            .build();
    storage::GarbageCollectorThread gc_thread(&deferred_action_manager_, DISABLED, gc_period);
    for (uint32_t batch = 0; batch * batch_size < num_txns; batch++) {
      auto result = tested.SimulateOltp(batch_size, num_concurrent_txns);
      gc_thread.PauseGC();
      tested.CheckReadsCorrect(&result.first);
      for (auto w : result.first) delete w;
      for (auto w : result.second) delete w;
      gc_thread.ResumeGC();
    }
  }
}

// This test is similar to the previous one, but with a higher ratio of updates
// and longer transactions leading to more aborts.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, HighAbortRateWithGC) {
  const uint32_t txn_length = 40;
  const std::vector<double> update_select_ratio = {0.8, 0.2};
  const uint32_t num_concurrent_txns = MultiThreadTestUtil::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested = LargeTransactionTestObject::Builder()
                                            .SetMaxColumns(max_columns)
                                            .SetInitialTableSize(initial_table_size)
                                            .SetTxnLength(txn_length)
                                            .SetUpdateSelectRatio(update_select_ratio)
                                            .SetBlockStore(&block_store_)
                                            .SetBufferPool(&buffer_pool_)
                                            .SetGenerator(&generator_)
                                            .SetTimestampManager(&timestamp_manager_)
                                            .SetDeferredActionManager(&deferred_action_manager_)
                                            .SetVersionChainGC(&version_chain_gc_)
                                            .SetBookkeeping(true)
                                            .SetVarlenAllowed(true)
                                            .build();
    storage::GarbageCollectorThread gc_thread(&deferred_action_manager_, DISABLED, gc_period);
    for (uint32_t batch = 0; batch * batch_size < num_txns; batch++) {
      auto result = tested.SimulateOltp(batch_size, num_concurrent_txns);
      gc_thread.PauseGC();
      tested.CheckReadsCorrect(&result.first);
      for (auto w : result.first) delete w;
      for (auto w : result.second) delete w;
      gc_thread.ResumeGC();
    }
  }
}

// This test duplicates the previous one with a higher number of thread swapouts.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, HighAbortRateHighThreadWithGC) {
  const uint32_t txn_length = 40;
  const std::vector<double> update_select_ratio = {0.8, 0.2};
  const uint32_t num_concurrent_txns = 2 * MultiThreadTestUtil::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested = LargeTransactionTestObject::Builder()
                                            .SetMaxColumns(max_columns)
                                            .SetInitialTableSize(initial_table_size)
                                            .SetTxnLength(txn_length)
                                            .SetUpdateSelectRatio(update_select_ratio)
                                            .SetBlockStore(&block_store_)
                                            .SetBufferPool(&buffer_pool_)
                                            .SetGenerator(&generator_)
                                            .SetTimestampManager(&timestamp_manager_)
                                            .SetDeferredActionManager(&deferred_action_manager_)
                                            .SetVersionChainGC(&version_chain_gc_)
                                            .SetBookkeeping(true)
                                            .SetVarlenAllowed(true)
                                            .build();
    storage::GarbageCollectorThread gc_thread(&deferred_action_manager_, DISABLED, gc_period);
    for (uint32_t batch = 0; batch * batch_size < num_txns; batch++) {
      auto result = tested.SimulateOltp(batch_size, num_concurrent_txns);
      gc_thread.PauseGC();
      tested.CheckReadsCorrect(&result.first);
      for (auto w : result.first) delete w;
      for (auto w : result.second) delete w;
      gc_thread.ResumeGC();
    }
  }
}

// This test attempts to simulate a TPC-C-like scenario.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, TPCCishWithGC) {
  const uint32_t txn_length = 5;
  const std::vector<double> update_select_ratio = {0.4, 0.6};
  const uint32_t num_concurrent_txns = MultiThreadTestUtil::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested = LargeTransactionTestObject::Builder()
                                            .SetMaxColumns(max_columns)
                                            .SetInitialTableSize(initial_table_size)
                                            .SetTxnLength(txn_length)
                                            .SetUpdateSelectRatio(update_select_ratio)
                                            .SetBlockStore(&block_store_)
                                            .SetBufferPool(&buffer_pool_)
                                            .SetGenerator(&generator_)
                                            .SetTimestampManager(&timestamp_manager_)
                                            .SetDeferredActionManager(&deferred_action_manager_)
                                            .SetVersionChainGC(&version_chain_gc_)
                                            .SetBookkeeping(true)
                                            .SetVarlenAllowed(true)
                                            .build();
    storage::GarbageCollectorThread gc_thread(&deferred_action_manager_, DISABLED, gc_period);
    for (uint32_t batch = 0; batch * batch_size < num_txns; batch++) {
      auto result = tested.SimulateOltp(batch_size, num_concurrent_txns);
      gc_thread.PauseGC();
      tested.CheckReadsCorrect(&result.first);
      for (auto w : result.first) delete w;
      for (auto w : result.second) delete w;
      gc_thread.ResumeGC();
    }
  }
}

// This test duplicates the previous one with a higher number of thread swapouts.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, TPCCishHighThreadWithGC) {
  const uint32_t txn_length = 5;
  const std::vector<double> update_select_ratio = {0.4, 0.6};
  const uint32_t num_concurrent_txns = 2 * MultiThreadTestUtil::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested = LargeTransactionTestObject::Builder()
                                            .SetMaxColumns(max_columns)
                                            .SetInitialTableSize(initial_table_size)
                                            .SetTxnLength(txn_length)
                                            .SetUpdateSelectRatio(update_select_ratio)
                                            .SetBlockStore(&block_store_)
                                            .SetBufferPool(&buffer_pool_)
                                            .SetGenerator(&generator_)
                                            .SetTimestampManager(&timestamp_manager_)
                                            .SetDeferredActionManager(&deferred_action_manager_)
                                            .SetVersionChainGC(&version_chain_gc_)
                                            .SetBookkeeping(true)
                                            .SetVarlenAllowed(true)
                                            .build();
    storage::GarbageCollectorThread gc_thread(&deferred_action_manager_, DISABLED, gc_period);
    for (uint32_t batch = 0; batch * batch_size < num_txns; batch++) {
      auto result = tested.SimulateOltp(batch_size, num_concurrent_txns);
      gc_thread.PauseGC();
      tested.CheckReadsCorrect(&result.first);
      for (auto w : result.first) delete w;
      for (auto w : result.second) delete w;
      gc_thread.ResumeGC();
    }
  }
}
}  // namespace terrier
