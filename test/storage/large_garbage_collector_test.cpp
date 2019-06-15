#include <vector>
#include "gtest/gtest.h"
#include "storage/garbage_collector_thread.h"
#include "util/transaction_test_util.h"
#include "di/di_help.h"
#include "di/injectors.h"

namespace terrier {
class LargeGCTests : public TerrierTest {
 public:
  static auto Injector(const LargeTransactionTestConfiguration &config) {
    return di::make_injector(
        di::storage_injector(),
        di::bind<storage::LogManager>().in(di::disabled_module) [di::override],
        di::bind<LargeTransactionTestConfiguration>().to(config),
        di::bind<std::default_random_engine>().in(di::terrier_module),
        di::bind<uint64_t>().named(storage::BlockStore::SIZE_LIMIT).to(static_cast<uint64_t>(1000)),
        di::bind<uint64_t>().named(storage::BlockStore::REUSE_LIMIT).to(static_cast<uint64_t>(1000)),
        di::bind<uint64_t>().named(storage::RecordBufferSegmentPool::SIZE_LIMIT).to(static_cast<uint64_t>(10000)),
        di::bind<uint64_t>().named(storage::RecordBufferSegmentPool::REUSE_LIMIT).to(static_cast<uint64_t>(10000)),
        di::bind<bool>().named(transaction::TransactionManager::GC_ENABLED).to(true),
        di::bind<std::chrono::milliseconds>().named(storage::GarbageCollectorThread::GC_PERIOD)
                                             .to(std::chrono::milliseconds(10))
    );
  }

  const uint32_t num_iterations = 10;
  const uint32_t num_txns = 1000;
  const uint32_t batch_size = 100;
};

// These test cases generates random update-selects in concurrent transactions on a pre-populated database.
// Each transaction logs their operations locally. At the end of the run, we can reconstruct the snapshot of
// a database using the updates at every timestamp, and compares the reads with the reconstructed snapshot versions
// to make sure they are the same.
// NOLINTNEXTLINE
TEST_F(LargeGCTests, MixedReadWriteWithGC) {
  LargeTransactionTestConfiguration config = {
      .update_select_ratio_ = {0.5, 0.5},
      .txn_length_ = 10,
      .initial_table_size_ = 1000,
      .max_columns_ = 20,
      .varlen_allowed_ = true,
      .gc_on_ = true,
      .bookkeeping_ = true
  };

  const uint32_t num_concurrent_txns = MultiThreadTestUtil::HardwareConcurrency();
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    auto injector = Injector(config);
    auto tested = injector.create<std::unique_ptr<LargeTransactionTestObject>>();
    auto gc_thread = injector.create<std::unique_ptr<storage::GarbageCollectorThread>>();
    for (uint32_t batch = 0; batch * batch_size < num_txns; batch++) {
      auto result = tested->SimulateOltp(batch_size, num_concurrent_txns);
      gc_thread->PauseGC();
      tested->CheckReadsCorrect(&result.first);
      for (auto w : result.first) delete w;
      for (auto w : result.second) delete w;
      gc_thread->ResumeGC();
    }
    printf("it\n");
  }
}
/*
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
                                            .SetGcOn(true)
                                            .SetBookkeeping(true)
                                            .SetVarlenAllowed(true)
                                            .build();
    storage::GarbageCollectorThread gc_thread(tested.GetTxnManager(), gc_period);
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
                                            .SetGcOn(true)
                                            .SetBookkeeping(true)
                                            .SetVarlenAllowed(true)
                                            .build();
    storage::GarbageCollectorThread gc_thread(tested.GetTxnManager(), gc_period);
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
                                            .SetGcOn(true)
                                            .SetBookkeeping(true)
                                            .SetVarlenAllowed(true)
                                            .build();
    storage::GarbageCollectorThread gc_thread(tested.GetTxnManager(), gc_period);
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
                                            .SetGcOn(true)
                                            .SetBookkeeping(true)
                                            .SetVarlenAllowed(true)
                                            .build();
    storage::GarbageCollectorThread gc_thread(tested.GetTxnManager(), gc_period);
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
                                            .SetGcOn(true)
                                            .SetBookkeeping(true)
                                            .SetVarlenAllowed(true)
                                            .build();
    storage::GarbageCollectorThread gc_thread(tested.GetTxnManager(), gc_period);
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
                                            .SetGcOn(true)
                                            .SetBookkeeping(true)
                                            .SetVarlenAllowed(true)
                                            .build();
    storage::GarbageCollectorThread gc_thread(tested.GetTxnManager(), gc_period);
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
                                            .SetGcOn(true)
                                            .SetBookkeeping(true)
                                            .SetVarlenAllowed(true)
                                            .build();
    storage::GarbageCollectorThread gc_thread(tested.GetTxnManager(), gc_period);
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
 */
}  // namespace terrier
