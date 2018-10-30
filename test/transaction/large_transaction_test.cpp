#include <vector>
#include "gtest/gtest.h"
#include "util/transaction_test_util.h"

namespace terrier {
class LargeTransactionTests : public TerrierTest {
 public:
  const uint32_t num_iterations = 10;
  const uint16_t max_columns = 20;
  const uint32_t initial_table_size = 1000;
  const uint32_t num_txns = 1000;
  storage::BlockStore block_store_{1000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{20000, 20000};
  std::default_random_engine generator_;
};

// These test cases generates random update-selects in concurrent transactions on a pre-populated database.
// Each transaction logs their operations locally. At the end of the run, we can reconstruct the snapshot of
// a database using the updates at every timestamp, and compares the reads with the reconstructed snapshot versions
// to make sure they are the same.
// NOLINTNEXTLINE
TEST_F(LargeTransactionTests, MixedReadWrite) {
  const uint32_t txn_length = 10;
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

// Double the thread count to force more thread swapping and try to capture unexpected races
// NOLINTNEXTLINE
TEST_F(LargeTransactionTests, MixedReadWriteHighThread) {
  const uint32_t txn_length = 10;
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

// This test is similar to the previous one, but with a higher ratio of updates
// and longer transactions leading to more aborts.
// NOLINTNEXTLINE
TEST_F(LargeTransactionTests, HighAbortRate) {
  const uint32_t txn_length = 40;
  const std::vector<double> update_select_ratio = {0.8, 0.2};
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

// This test duplicates the previous one with a higher number of thread swapouts.
// NOLINTNEXTLINE
TEST_F(LargeTransactionTests, HighAbortRateHighThread) {
  const uint32_t txn_length = 40;
  const std::vector<double> update_select_ratio = {0.8, 0.2};
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
TEST_F(LargeTransactionTests, TPCCish) {
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

<<<<<<< HEAD
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

=======
>>>>>>> upstream/master
// This test is a duplicate of TPCC but with higher number of thread swapouts
// NOLINTNEXTLINE
TEST_F(LargeTransactionTests, TPCCishHighThread) {
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
