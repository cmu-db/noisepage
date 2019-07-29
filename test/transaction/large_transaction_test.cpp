#include <memory>
#include <vector>
#include "di/di_help.h"
#include "di/injectors.h"
#include "gtest/gtest.h"
#include "util/data_table_test_util.h"

namespace terrier {
class LargeTransactionTests : public TerrierTest {
 public:
  void RunTest(const LargeDataTableTestConfiguration &config) {
    for (uint32_t iteration = 0; iteration < config.NumIterations(); iteration++) {
      auto injector = di::make_injector<di::TestBindingPolicy>(
          di::storage_injector(), di::bind<storage::AccessObserver>().in(di::disabled),
          di::bind<common::DedicatedThreadRegistry>().in(
              di::disabled)[di::override],                                 // no need for thread registry in this test
          di::bind<storage::LogManager>().in(di::disabled)[di::override],  // no need for logging in this test
          di::bind<LargeDataTableTestConfiguration>().to(config),
          di::bind<std::default_random_engine>().in(di::terrier_singleton),  // need to be universal across injectors
          di::bind<uint64_t>().named(storage::BlockStore::SIZE_LIMIT).to(static_cast<uint64_t>(1000)),
          di::bind<uint64_t>().named(storage::BlockStore::REUSE_LIMIT).to(static_cast<uint64_t>(1000)),
          di::bind<uint64_t>().named(storage::RecordBufferSegmentPool::SIZE_LIMIT).to(static_cast<uint64_t>(20000)),
          di::bind<uint64_t>().named(storage::RecordBufferSegmentPool::REUSE_LIMIT).to(static_cast<uint64_t>(20000)),
          di::bind<bool>().named(transaction::TransactionManager::GC_ENABLED).to(false));
      auto tested = injector.create<std::unique_ptr<LargeDataTableTestObject>>();
      auto result = tested->SimulateOltp(config.NumTxns(), config.NumConcurrentTxns());
      tested->CheckReadsCorrect(&result.first);
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
}  // namespace terrier
