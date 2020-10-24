#include <random>

#include "main/db_main.h"
#include "test_util/data_table_test_util.h"
#include "transaction/deferred_action_manager.h"

namespace noisepage {
class LargeTransactionTests : public TerrierTest {
 public:
  void RunTest(const LargeDataTableTestConfiguration &config) {
    for (uint32_t iteration = 0; iteration < config.NumIterations(); iteration++) {
      std::default_random_engine generator;
      auto db_main = DBMain::Builder().Build();
      LargeDataTableTestObject tested(config, db_main->GetStorageLayer()->GetBlockStore().Get(),
                                      db_main->GetTransactionLayer()->GetTransactionManager().Get(), &generator,
                                      DISABLED);

      auto result = tested.SimulateOltp(config.NumTxns(), config.NumConcurrentTxns());
      tested.CheckReadsCorrect(&result.first);
      for (auto w : result.first) delete w;
      for (auto w : result.second) delete w;
    }
  }
};

// These test cases generates random update-selects in concurrent transactions on a pre-populated database.
// Each transaction logs their operations locally. At the end of the run, we can reconstruct the snapshot of
// a database using the updates at every timestamp, and compares the reads with the reconstructed snapshot versions
// to make sure they are the same.
// NOLINTNEXTLINE
TEST_F(LargeTransactionTests, MixedReadWrite) {
  auto config = LargeDataTableTestConfiguration::Builder()
                    .SetNumIterations(10)
                    .SetNumTxns(1000)
                    .SetNumConcurrentTxns(MultiThreadTestUtil::HardwareConcurrency())
                    .SetUpdateSelectRatio({0.5, 0.5})
                    .SetTxnLength(10)
                    .SetInitialTableSize(1000)
                    .SetMaxColumns(20)
                    .SetVarlenAllowed(false)
                    .Build();
  RunTest(config);
}

// Double the thread count to force more thread swapping and try to capture unexpected races
// NOLINTNEXTLINE
TEST_F(LargeTransactionTests, MixedReadWriteHighThread) {
  auto config = LargeDataTableTestConfiguration::Builder()
                    .SetNumIterations(10)
                    .SetNumTxns(1000)
                    .SetNumConcurrentTxns(2 * MultiThreadTestUtil::HardwareConcurrency())
                    .SetUpdateSelectRatio({0.5, 0.5})
                    .SetTxnLength(10)
                    .SetInitialTableSize(1000)
                    .SetMaxColumns(20)
                    .SetVarlenAllowed(false)
                    .Build();
  RunTest(config);
}

// This test targets the scenario of low abort rate (~1% of num_txns) and high throughput of statements
// NOLINTNEXTLINE
TEST_F(LargeTransactionTests, LowAbortHighThroughput) {
  auto config = LargeDataTableTestConfiguration::Builder()
                    .SetNumIterations(10)
                    .SetNumTxns(1000)
                    .SetNumConcurrentTxns(MultiThreadTestUtil::HardwareConcurrency())
                    .SetUpdateSelectRatio({0.5, 0.5})
                    .SetTxnLength(1)
                    .SetInitialTableSize(1000)
                    .SetMaxColumns(20)
                    .SetVarlenAllowed(false)
                    .Build();
  RunTest(config);
}

// This test is a duplicate of LowAbortHighThroughput but with higher number of thread swapouts
// NOLINTNEXTLINE
TEST_F(LargeTransactionTests, LowAbortHighThroughputHighThread) {
  auto config = LargeDataTableTestConfiguration::Builder()
                    .SetNumIterations(10)
                    .SetNumTxns(1000)
                    .SetNumConcurrentTxns(2 * MultiThreadTestUtil::HardwareConcurrency())
                    .SetUpdateSelectRatio({0.5, 0.5})
                    .SetTxnLength(1)
                    .SetInitialTableSize(1000)
                    .SetMaxColumns(20)
                    .SetVarlenAllowed(false)
                    .Build();
  RunTest(config);
}

// This test is similar to the previous one, but with a higher ratio of updates
// and longer transactions leading to more aborts.
// NOLINTNEXTLINE
TEST_F(LargeTransactionTests, HighAbortRate) {
  auto config = LargeDataTableTestConfiguration::Builder()
                    .SetNumIterations(10)
                    .SetNumTxns(1000)
                    .SetNumConcurrentTxns(MultiThreadTestUtil::HardwareConcurrency())
                    .SetUpdateSelectRatio({0.8, 0.2})
                    .SetTxnLength(40)
                    .SetInitialTableSize(1000)
                    .SetMaxColumns(20)
                    .SetVarlenAllowed(false)
                    .Build();
  RunTest(config);
}

// This test duplicates the previous one with a higher number of thread swapouts.
// NOLINTNEXTLINE
TEST_F(LargeTransactionTests, HighAbortRateHighThread) {
  auto config = LargeDataTableTestConfiguration::Builder()
                    .SetNumIterations(10)
                    .SetNumTxns(1000)
                    .SetNumConcurrentTxns(2 * MultiThreadTestUtil::HardwareConcurrency())
                    .SetUpdateSelectRatio({0.8, 0.2})
                    .SetTxnLength(40)
                    .SetInitialTableSize(1000)
                    .SetMaxColumns(20)
                    .SetVarlenAllowed(false)
                    .Build();
  RunTest(config);
}

// This test aims to behave like a TPC-C benchmark
// NOLINTNEXTLINE
TEST_F(LargeTransactionTests, TPCCish) {
  auto config = LargeDataTableTestConfiguration::Builder()
                    .SetNumIterations(10)
                    .SetNumTxns(1000)
                    .SetNumConcurrentTxns(MultiThreadTestUtil::HardwareConcurrency())
                    .SetUpdateSelectRatio({0.4, 0.6})
                    .SetTxnLength(5)
                    .SetInitialTableSize(1000)
                    .SetMaxColumns(20)
                    .SetVarlenAllowed(false)
                    .Build();
  RunTest(config);
}

// This test is a duplicate of TPCC but with higher number of thread swapouts
// NOLINTNEXTLINE
TEST_F(LargeTransactionTests, TPCCishHighThread) {
  auto config = LargeDataTableTestConfiguration::Builder()
                    .SetNumIterations(10)
                    .SetNumTxns(1000)
                    .SetNumConcurrentTxns(2 * MultiThreadTestUtil::HardwareConcurrency())
                    .SetUpdateSelectRatio({0.4, 0.6})
                    .SetTxnLength(5)
                    .SetInitialTableSize(1000)
                    .SetMaxColumns(20)
                    .SetVarlenAllowed(false)
                    .Build();
  RunTest(config);
}
}  // namespace noisepage
