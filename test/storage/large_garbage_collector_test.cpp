#include <random>
#include "gtest/gtest.h"
#include "storage/garbage_collector_thread.h"
#include "test_util/data_table_test_util.h"
#include "transaction/deferred_action_manager.h"

namespace terrier {
class LargeGCTests : public TerrierTest {
 public:
  void RunTest(const LargeDataTableTestConfiguration &config) {
    for (uint32_t iteration = 0; iteration < config.NumIterations(); iteration++) {
      std::default_random_engine generator;
      storage::BlockStore store(10000, 1000);
      storage::RecordBufferSegmentPool buffer_pool(10000, 10000);
      transaction::TimestampManager timestamp_manager;
      transaction::DeferredActionManager deferred_action_manager(&timestamp_manager);
      transaction::TransactionManager txn_manager(&timestamp_manager, &deferred_action_manager, &buffer_pool, true,
                                                  DISABLED);
      storage::GarbageCollector gc(&timestamp_manager, &deferred_action_manager, &txn_manager, DISABLED);
      LargeDataTableTestObject tested(config, &store, &txn_manager, &generator, DISABLED);
      storage::GarbageCollectorThread gc_thread(&gc, std::chrono::milliseconds(10));
      for (uint32_t batch = 0; batch * config.BatchSize() < config.NumTxns(); batch++) {
        auto result = tested.SimulateOltp(config.BatchSize(), config.NumConcurrentTxns());
        gc_thread.PauseGC();
        tested.CheckReadsCorrect(&result.first);
        for (auto w : result.first) delete w;
        for (auto w : result.second) delete w;
        gc_thread.ResumeGC();
      }
    }
  }
};

// These test cases generates random update-selects in concurrent transactions on a pre-populated database.
// Each transaction logs their operations locally. At the end of the run, we can reconstruct the snapshot of
// a database using the updates at every timestamp, and compares the reads with the reconstructed snapshot versions
// to make sure they are the same.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, MixedReadWriteWithGC) {
  auto config = LargeDataTableTestConfiguration::Builder()
                    .SetNumIterations(10)
                    .SetNumTxns(1000)
                    .SetBatchSize(100)
                    .SetNumConcurrentTxns(MultiThreadTestUtil::HardwareConcurrency())
                    .SetUpdateSelectRatio({0.5, 0.5})
                    .SetTxnLength(10)
                    .SetInitialTableSize(1000)
                    .SetMaxColumns(20)
                    .SetVarlenAllowed(true)
                    .Build();
  RunTest(config);
}

// Double the thread count to force more thread swapping and try to capture unexpected races
// NOLINTNEXTLINE
TEST_F(LargeGCTests, MixedReadWriteHighThreadWithGC) {
  auto config = LargeDataTableTestConfiguration::Builder()
                    .SetNumIterations(10)
                    .SetNumTxns(1000)
                    .SetBatchSize(100)
                    .SetNumConcurrentTxns(2 * MultiThreadTestUtil::HardwareConcurrency())
                    .SetUpdateSelectRatio({0.5, 0.5})
                    .SetTxnLength(10)
                    .SetInitialTableSize(1000)
                    .SetMaxColumns(20)
                    .SetVarlenAllowed(true)
                    .Build();
  RunTest(config);
}

// This test targets the scenario of low abort rate (~1% of num_txns) and high throughput of statements
// NOLINTNEXTLINE
TEST_F(LargeGCTests, LowAbortHighThroughputWithGC) {
  auto config = LargeDataTableTestConfiguration::Builder()
                    .SetNumIterations(10)
                    .SetNumTxns(1000)
                    .SetBatchSize(100)
                    .SetNumConcurrentTxns(MultiThreadTestUtil::HardwareConcurrency())
                    .SetUpdateSelectRatio({0.5, 0.5})
                    .SetTxnLength(1)
                    .SetInitialTableSize(1000)
                    .SetMaxColumns(20)
                    .SetVarlenAllowed(true)
                    .Build();
  RunTest(config);
}

// This test is a duplicate of LowAbortHighThroughputWithGC but with higher number of thread swapouts
// NOLINTNEXTLINE
TEST_F(LargeGCTests, LowAbortHighThroughputHighThreadWithGC) {
  auto config = LargeDataTableTestConfiguration::Builder()
                    .SetNumIterations(10)
                    .SetNumTxns(1000)
                    .SetBatchSize(100)
                    .SetNumConcurrentTxns(2 * MultiThreadTestUtil::HardwareConcurrency())
                    .SetUpdateSelectRatio({0.5, 0.5})
                    .SetTxnLength(1)
                    .SetInitialTableSize(1000)
                    .SetMaxColumns(20)
                    .SetVarlenAllowed(true)
                    .Build();
  RunTest(config);
}

// This test is similar to the previous one, but with a higher ratio of updates
// and longer transactions leading to more aborts.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, HighAbortRateWithGC) {
  auto config = LargeDataTableTestConfiguration::Builder()
                    .SetNumIterations(10)
                    .SetNumTxns(1000)
                    .SetBatchSize(100)
                    .SetNumConcurrentTxns(MultiThreadTestUtil::HardwareConcurrency())
                    .SetUpdateSelectRatio({0.8, 0.2})
                    .SetTxnLength(40)
                    .SetInitialTableSize(1000)
                    .SetMaxColumns(20)
                    .SetVarlenAllowed(true)
                    .Build();
  RunTest(config);
}

// This test duplicates the previous one with a higher number of thread swapouts.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, HighAbortRateHighThreadWithGC) {
  auto config = LargeDataTableTestConfiguration::Builder()
                    .SetNumIterations(10)
                    .SetNumTxns(1000)
                    .SetBatchSize(100)
                    .SetNumConcurrentTxns(2 * MultiThreadTestUtil::HardwareConcurrency())
                    .SetUpdateSelectRatio({0.8, 0.2})
                    .SetTxnLength(40)
                    .SetInitialTableSize(1000)
                    .SetMaxColumns(20)
                    .SetVarlenAllowed(true)
                    .Build();
  RunTest(config);
}

// This test attempts to simulate a TPC-C-like scenario.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, TPCCishWithGC) {
  auto config = LargeDataTableTestConfiguration::Builder()
                    .SetNumIterations(10)
                    .SetNumTxns(1000)
                    .SetBatchSize(100)
                    .SetNumConcurrentTxns(MultiThreadTestUtil::HardwareConcurrency())
                    .SetUpdateSelectRatio({0.4, 0.6})
                    .SetTxnLength(5)
                    .SetInitialTableSize(1000)
                    .SetMaxColumns(20)
                    .SetVarlenAllowed(true)
                    .Build();
  RunTest(config);
}

// This test duplicates the previous one with a higher number of thread swapouts.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, TPCCishHighThreadWithGC) {
  auto config = LargeDataTableTestConfiguration::Builder()
                    .SetNumIterations(10)
                    .SetNumTxns(1000)
                    .SetBatchSize(100)
                    .SetNumConcurrentTxns(2 * MultiThreadTestUtil::HardwareConcurrency())
                    .SetUpdateSelectRatio({0.4, 0.6})
                    .SetTxnLength(5)
                    .SetInitialTableSize(1000)
                    .SetMaxColumns(20)
                    .SetVarlenAllowed(true)
                    .Build();
  RunTest(config);
}
}  // namespace terrier
