#include <vector>
#include <random>
#include "gtest/gtest.h"
#include "storage/garbage_collector_thread.h"
#include "util/transaction_test_util.h"
#include "di/di_help.h"
#include "di/injectors.h"

namespace terrier {
class LargeGCTests : public TerrierTest {
 public:
  void RunTest(const LargeTransactionTestConfiguration &config) {
    for (uint32_t iteration = 0; iteration < config.num_iterations_; iteration++) {
      auto injector = di::make_injector<di::TestBindingPolicy>(
          di::storage_injector(),
          di::bind<storage::LogManager>().in(di::disabled)[di::override],  // no need for logging in this test
          di::bind<LargeTransactionTestConfiguration>().to(config),
          di::bind<std::default_random_engine>().in(di::terrier_singleton),  // need to be universal across injectors
          di::bind<uint64_t>().named(storage::BlockStore::SIZE_LIMIT).to(static_cast<uint64_t>(1000)),
          di::bind<uint64_t>().named(storage::BlockStore::REUSE_LIMIT).to(static_cast<uint64_t>(1000)),
          di::bind<uint64_t>().named(storage::RecordBufferSegmentPool::SIZE_LIMIT).to(static_cast<uint64_t>(10000)),
          di::bind<uint64_t>().named(storage::RecordBufferSegmentPool::REUSE_LIMIT).to(static_cast<uint64_t>(10000)),
          di::bind<bool>().named(transaction::TransactionManager::GC_ENABLED).to(true),
          di::bind<std::chrono::milliseconds>().named(storage::GarbageCollectorThread::GC_PERIOD)
              .to(std::chrono::milliseconds(10))
      );
      auto tested = injector.create<std::unique_ptr<LargeTransactionTestObject>>();
      auto gc_thread = injector.create<std::unique_ptr<storage::GarbageCollectorThread>>();
      for (uint32_t batch = 0; batch * config.batch_size_ < config.num_txns_; batch++) {
        auto result = tested->SimulateOltp(config.batch_size_, config.num_concurrent_txns_);
        gc_thread->PauseGC();
        tested->CheckReadsCorrect(&result.first);
        for (auto w : result.first) delete w;
        for (auto w : result.second) delete w;
        gc_thread->ResumeGC();
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
  RunTest({
              .num_iterations_ = 10,
              .num_txns_ = 1000,
              .batch_size_ = 100,
              .num_concurrent_txns_ = MultiThreadTestUtil::HardwareConcurrency(),
              .update_select_ratio_ = {0.5, 0.5},
              .txn_length_ = 10,
              .initial_table_size_ = 1000,
              .max_columns_ = 20,
              .varlen_allowed_ = true});
}

// Double the thread count to force more thread swapping and try to capture unexpected races
// NOLINTNEXTLINE
TEST_F(LargeGCTests, MixedReadWriteHighThreadWithGC) {
  RunTest({
              .num_iterations_ = 10,
              .num_txns_ = 1000,
              .batch_size_ = 100,
              .num_concurrent_txns_ = 2 * MultiThreadTestUtil::HardwareConcurrency(),
              .update_select_ratio_ = {0.5, 0.5},
              .txn_length_ = 10,
              .initial_table_size_ = 1000,
              .max_columns_ = 20,
              .varlen_allowed_ = true});
}

// This test targets the scenario of low abort rate (~1% of num_txns) and high throughput of statements
// NOLINTNEXTLINE
TEST_F(LargeGCTests, LowAbortHighThroughputWithGC) {
  RunTest({
              .num_iterations_ = 10,
              .num_txns_ = 1000,
              .batch_size_ = 100,
              .num_concurrent_txns_ = MultiThreadTestUtil::HardwareConcurrency(),
              .update_select_ratio_ = {0.5, 0.5},
              .txn_length_ = 1,
              .initial_table_size_ = 1000,
              .max_columns_ = 20,
              .varlen_allowed_ = true});
}

// This test is a duplicate of LowAbortHighThroughputWithGC but with higher number of thread swapouts
// NOLINTNEXTLINE
TEST_F(LargeGCTests, LowAbortHighThroughputHighThreadWithGC) {
  RunTest({
              .num_iterations_ = 10,
              .num_txns_ = 1000,
              .batch_size_ = 100,
              .num_concurrent_txns_ = 2 * MultiThreadTestUtil::HardwareConcurrency(),
              .update_select_ratio_ = {0.5, 0.5},
              .txn_length_ = 1,
              .initial_table_size_ = 1000,
              .max_columns_ = 20,
              .varlen_allowed_ = true});
}

// This test is similar to the previous one, but with a higher ratio of updates
// and longer transactions leading to more aborts.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, HighAbortRateWithGC) {
  RunTest({
              .num_iterations_ = 10,
              .num_txns_ = 1000,
              .batch_size_ = 100,
              .num_concurrent_txns_ = MultiThreadTestUtil::HardwareConcurrency(),
              .update_select_ratio_ = {0.8, 0.2},
              .txn_length_ = 40,
              .initial_table_size_ = 1000,
              .max_columns_ = 20,
              .varlen_allowed_ = true});
}

// This test duplicates the previous one with a higher number of thread swapouts.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, HighAbortRateHighThreadWithGC) {
  RunTest({
              .num_iterations_ = 10,
              .num_txns_ = 1000,
              .batch_size_ = 100,
              .num_concurrent_txns_ = 2 * MultiThreadTestUtil::HardwareConcurrency(),
              .update_select_ratio_ = {0.8, 0.2},
              .txn_length_ = 40,
              .initial_table_size_ = 1000,
              .max_columns_ = 20,
              .varlen_allowed_ = true});
}

// This test attempts to simulate a TPC-C-like scenario.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, TPCCishWithGC) {
  RunTest({
              .num_iterations_ = 10,
              .num_txns_ = 1000,
              .batch_size_ = 100,
              .num_concurrent_txns_ = MultiThreadTestUtil::HardwareConcurrency(),
              .update_select_ratio_ = {0.4, 0.6},
              .txn_length_ = 5,
              .initial_table_size_ = 1000,
              .max_columns_ = 20,
              .varlen_allowed_ = true});
}

// This test duplicates the previous one with a higher number of thread swapouts.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, TPCCishHighThreadWithGC) {
  RunTest({
              .num_iterations_ = 10,
              .num_txns_ = 1000,
              .batch_size_ = 100,
              .num_concurrent_txns_ = 2 * MultiThreadTestUtil::HardwareConcurrency(),
              .update_select_ratio_ = {0.4, 0.6},
              .txn_length_ = 5,
              .initial_table_size_ = 1000,
              .max_columns_ = 20,
              .varlen_allowed_ = true});
}
}  // namespace terrier
