#include <vector>
#include "gtest/gtest.h"
#include "util/transaction_test_util.h"

namespace terrier {
class LargeTransactionTests : public TerrierTest {
 public:
  const uint32_t num_iterations = 100;
  const uint16_t max_columns = 20;
  const uint32_t initial_table_size = 1000;
  const uint32_t num_txns = 100;
  storage::BlockStore block_store_{1000, 1000};
  common::ObjectPool<storage::BufferSegment> buffer_pool_{1000, 1000};
  std::default_random_engine generator_;
};

// This test case generates random update-selects in concurrent transactions on a pre-populated database.
// Each transaction logs their operations locally. At the end of the run, we can reconstruct the snapshot of
// a database using the updates at every timestamp, and compares the reads with the reconstructed snapshot versions
// to make sure they are the same.
// NOLINTNEXTLINE
TEST_F(LargeTransactionTests, MixedReadWrite) {
  const uint32_t txn_length = 20;
  const std::vector<double> update_select_ratio = {0.4, 0.6};
  const uint32_t num_concurrent_txns = TestThreadPool::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested(max_columns, initial_table_size, txn_length, update_select_ratio, &block_store_,
                                      &buffer_pool_, &generator_, false, true);
    auto result = tested.SimulateOltp(num_txns, num_concurrent_txns);
    tested.CheckReadsCorrect(&result.first);
    for (auto w : result.first) delete w;
    for (auto w : result.second) delete w;
  }
}

// This test targets the scenario of low abort rate (~1% of num_txns) and high throughput of statements
// NOLINTNEXTLINE
TEST_F(LargeTransactionTests, LowAbortHighThroughput) {
  const uint32_t txn_length = 1;
  const std::vector<double> update_select_ratio = {0.5, 0.5};
  const uint32_t num_concurrent_txns = TestThreadPool::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested(max_columns, initial_table_size, txn_length, update_select_ratio, &block_store_,
                                      &buffer_pool_, &generator_, false, true);
    auto result = tested.SimulateOltp(num_txns, num_concurrent_txns);
    tested.CheckReadsCorrect(&result.first);
    for (auto w : result.first) delete w;
    for (auto w : result.second) delete w;
  }
}

// This test is a duplicate of LowAbortHighThroughput but with higher number of thread swapouts
// NOLINTNEXTLINE
TEST_F(LargeTransactionTests, LowAbortHighThroughputHighThread) {
  const uint32_t txn_length = 1;
  const std::vector<double> update_select_ratio = {0.5, 0.5};
  const uint32_t num_concurrent_txns = 2 * TestThreadPool::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested(max_columns, initial_table_size, txn_length, update_select_ratio, &block_store_,
                                      &buffer_pool_, &generator_, false, true);
    auto result = tested.SimulateOltp(num_txns, num_concurrent_txns);
    tested.CheckReadsCorrect(&result.first);
    for (auto w : result.first) delete w;
    for (auto w : result.second) delete w;
  }
}

// This test aims to behave like a TPC-C benchmark
// NOLINTNEXTLINE
TEST_F(LargeTransactionTests, TPCC) {
  const uint32_t txn_length = 5;
  const std::vector<double> update_select_ratio = {0.4, 0.6};
  const uint32_t num_concurrent_txns = TestThreadPool::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested(max_columns, initial_table_size, txn_length, update_select_ratio, &block_store_,
                                      &buffer_pool_, &generator_, false, true);
    auto result = tested.SimulateOltp(num_txns, num_concurrent_txns);
    tested.CheckReadsCorrect(&result.first);
    for (auto w : result.first) delete w;
    for (auto w : result.second) delete w;
  }
}

// This test is a duplicate of TPCC but with higher number of thread swapouts
// NOLINTNEXTLINE
TEST_F(LargeTransactionTests, TPCCHighThread) {
  const uint32_t txn_length = 5;
  const std::vector<double> update_select_ratio = {0.4, 0.6};
  const uint32_t num_concurrent_txns = 2 * TestThreadPool::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LargeTransactionTestObject tested(max_columns, initial_table_size, txn_length, update_select_ratio, &block_store_,
                                      &buffer_pool_, &generator_, false, true);
    auto result = tested.SimulateOltp(num_txns, num_concurrent_txns);
    tested.CheckReadsCorrect(&result.first);
    for (auto w : result.first) delete w;
    for (auto w : result.second) delete w;
  }
}
}  // namespace terrier
