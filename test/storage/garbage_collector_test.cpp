#include "storage/garbage_collector.h"
#include <cstring>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/object_pool.h"
#include "storage/data_table.h"
#include "storage/storage_util.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"

namespace terrier {
// Not thread-safe
class GarbageCollectorDataTableTestObject {
 public:
  template <class Random>
  GarbageCollectorDataTableTestObject(storage::BlockStore *block_store, const uint16_t max_col, Random *generator)
      : layout_(StorageTestUtil::RandomLayoutNoVarlen(max_col, generator)),
        table_(block_store, layout_, storage::layout_version_t(0)) {}

  ~GarbageCollectorDataTableTestObject() {
    for (auto ptr : loose_pointers_) delete[] ptr;
    delete[] select_buffer_;
  }

  const storage::BlockLayout &Layout() const { return layout_; }

  template <class Random>
  storage::ProjectedRow *GenerateRandomTuple(Random *generator) {
    auto *buffer = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
    loose_pointers_.push_back(buffer);
    storage::ProjectedRow *redo = initializer_.InitializeRow(buffer);
    StorageTestUtil::PopulateRandomRow(redo, layout_, null_bias_, generator);
    return redo;
  }

  template <class Random>
  storage::ProjectedRow *GenerateRandomUpdate(Random *generator) {
    std::vector<storage::col_id_t> update_col_ids = StorageTestUtil::ProjectionListRandomColumns(layout_, generator);
    storage::ProjectedRowInitializer update_initializer =
        storage::ProjectedRowInitializer::CreateProjectedRowInitializer(layout_, update_col_ids);
    auto *buffer = common::AllocationUtil::AllocateAligned(update_initializer.ProjectedRowSize());
    loose_pointers_.push_back(buffer);
    storage::ProjectedRow *update = update_initializer.InitializeRow(buffer);
    StorageTestUtil::PopulateRandomRow(update, layout_, null_bias_, generator);
    return update;
  }

  storage::ProjectedRow *GenerateVersionFromUpdate(const storage::ProjectedRow &delta,
                                                   const storage::ProjectedRow &previous) {
    auto *buffer = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
    loose_pointers_.push_back(buffer);
    // Copy previous version
    std::memcpy(buffer, &previous, initializer_.ProjectedRowSize());
    auto *version = reinterpret_cast<storage::ProjectedRow *>(buffer);
    storage::StorageUtil::ApplyDelta(layout_, delta, version);
    return version;
  }

  storage::ProjectedRow *SelectIntoBuffer(transaction::TransactionContext *const txn, const storage::TupleSlot slot) {
    // generate a redo ProjectedRow for Select
    storage::ProjectedRow *select_row = initializer_.InitializeRow(select_buffer_);
    select_result_ = table_.Select(txn, slot, select_row);
    return select_row;
  }

  storage::BlockLayout layout_;
  storage::DataTable table_;
  // We want null_bias_ to be zero when testing CC. We already evaluate null correctness in other directed tests, and
  // we don't want the logically deleted field to end up set NULL.
  const double null_bias_ = 0;
  std::vector<byte *> loose_pointers_;
  storage::ProjectedRowInitializer initializer_ = storage::ProjectedRowInitializer::CreateProjectedRowInitializer(
      layout_, StorageTestUtil::ProjectionListAllColumns(layout_));
  byte *select_buffer_ = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
  bool select_result_;
};

struct GarbageCollectorTests : public ::terrier::TerrierTest {
  storage::BlockStore block_store_{100, 100};
  storage::RecordBufferSegmentPool buffer_pool_{10000, 10000};
  std::default_random_engine generator_;
  const uint32_t num_iterations_ = 100;
  const uint16_t max_columns_ = 100;
};

// Run a single txn that performs an Insert. Confirm that it takes 2 GC cycles to process this tuple.
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, SingleInsert) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *txn0 = txn_manager.BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn0, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    // Nothing should be able to be GC'd yet because txn0 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn0, TestCallbacks::EmptyCallback, nullptr);

    // Unlink the Insert's UndoRecord, then deallocate it on the next run
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());
  }
}

// Run a single read-only txn (empty UndoBuffer). Confirm that it takes 1 GC cycles to process this tuple.
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, ReadOnly) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *txn0 = txn_manager.BeginTransaction();
    txn_manager.Commit(txn0, TestCallbacks::EmptyCallback, nullptr);

    // Unlink the txn and deallocate immediately because it's read-only
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

// Corresponds to (MVCCTests, CommitInsert1)
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, CommitInsert1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *txn0 = txn_manager.BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn0, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    // Nothing should be able to be GC'd yet because txn0 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    auto *txn1 = txn_manager.BeginTransaction();

    tested.SelectIntoBuffer(txn1, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager.Commit(txn0, TestCallbacks::EmptyCallback, nullptr);

    // Nothing should be able to be GC'd yet because txn1 started before txn0's commit
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    tested.SelectIntoBuffer(txn1, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager.Commit(txn1, TestCallbacks::EmptyCallback, nullptr);

    // Unlink the two transactions and then deallocate the single non-read-only transaction
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());

    auto *txn2 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));
    txn_manager.Commit(txn2, TestCallbacks::EmptyCallback, nullptr);

    // Unlink the read-only transaction, and it shouldn't make it to the second invocation because it's read-only
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

// Corresponds to (MVCCTests, CommitInsert2)
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, CommitInsert2) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *txn0 = txn_manager.BeginTransaction();

    auto *txn1 = txn_manager.BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn1, *insert_tuple);

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    // Nothing should be able to be GC'd yet because txn0 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn1, TestCallbacks::EmptyCallback, nullptr);

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    // Nothing should be able to be GC'd yet because txn0 started before txn1's commit
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn0, TestCallbacks::EmptyCallback, nullptr);

    // Unlink the two transactions and then deallocate the single non-read-only transaction
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());

    auto *txn2 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));
    txn_manager.Commit(txn2, TestCallbacks::EmptyCallback, nullptr);

    // Unlink the read-only transaction
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    // It shouldn't make it to the second invocation because it's read-only
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

// Corresponds to (MVCCTests, AbortInsert1)
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, AbortInsert1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *txn0 = txn_manager.BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn0, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    // Nothing should be able to be GC'd yet because txn0 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    auto *txn1 = txn_manager.BeginTransaction();

    tested.SelectIntoBuffer(txn1, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager.Abort(txn0);

    // Aborted transactions can be removed from the unlink queue immediately
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    // But it's not safe to deallocate it yet because txn #1 is still running and may hold a reference to it
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    tested.SelectIntoBuffer(txn1, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager.Commit(txn1, TestCallbacks::EmptyCallback, nullptr);

    // Deallocate the aborted txn, and process the read-only transaction
    EXPECT_EQ(std::make_pair(1u, 1u), gc.PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    auto *txn2 = txn_manager.BeginTransaction();

    tested.SelectIntoBuffer(txn2, slot);
    EXPECT_FALSE(tested.select_result_);
    txn_manager.Commit(txn2, TestCallbacks::EmptyCallback, nullptr);

    // Unlink the read-only transaction
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    // It shouldn't make it to the second invocation because it's read-only
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

// Corresponds to (MVCCTests, AbortInsert2)
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, AbortInsert2) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *txn0 = txn_manager.BeginTransaction();

    auto *txn1 = txn_manager.BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn1, *insert_tuple);

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    // Nothing should be able to be GC'd yet because txn1 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    txn_manager.Abort(txn1);

    // Aborted transactions can be removed from the unlink queue immediately
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    // But it's not safe to deallocate it yet because txn #0 is still running and may hold a reference to it
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager.Commit(txn0, TestCallbacks::EmptyCallback, nullptr);

    // Deallocate the aborted txn, and process the read-only transaction
    EXPECT_EQ(std::make_pair(1u, 1u), gc.PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    auto *txn2 = txn_manager.BeginTransaction();

    tested.SelectIntoBuffer(txn2, slot);
    EXPECT_FALSE(tested.select_result_);
    txn_manager.Commit(txn2, TestCallbacks::EmptyCallback, nullptr);

    // Unlink the read-only transaction
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    // It shouldn't make it to the second invocation because it's read-only
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

// Corresponds to (MVCCTests, CommitUpdate1)
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, CommitUpdate1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager.BeginTransaction();
    storage::TupleSlot slot = tested.table_.Insert(txn, *insert_tuple);
    txn_manager.Commit(txn, TestCallbacks::EmptyCallback, nullptr);

    // Unlink and reclaim the Insert
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager.BeginTransaction();

    EXPECT_TRUE(tested.table_.Update(txn0, slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    // Nothing should be able to be GC'd yet because txn0 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    auto *txn1 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn0, TestCallbacks::EmptyCallback, nullptr);

    // Nothing should be able to be GC'd yet because txn1 started before txn0's commit
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn1, TestCallbacks::EmptyCallback, nullptr);

    // Unlink the update and read-only txns, then deallocate the update txn
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());

    auto *txn2 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));
    txn_manager.Commit(txn2, TestCallbacks::EmptyCallback, nullptr);

    // Unlink the read-only transaction
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    // It shouldn't make it to the second invocation because it's read-only
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

// Corresponds to (MVCCTests, CommitUpdate2)
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, CommitUpdate2) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager.BeginTransaction();
    storage::TupleSlot slot = tested.table_.Insert(txn, *insert_tuple);
    txn_manager.Commit(txn, TestCallbacks::EmptyCallback, nullptr);

    // Unlink and reclaim the Insert
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager.BeginTransaction();

    auto *txn1 = txn_manager.BeginTransaction();

    EXPECT_TRUE(tested.table_.Update(txn1, slot, *update));

    // Nothing should be able to be GC'd yet because txn1 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    txn_manager.Commit(txn1, TestCallbacks::EmptyCallback, nullptr);

    // Nothing should be able to be GC'd yet because txn0 started before txn1's commit
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn0, TestCallbacks::EmptyCallback, nullptr);

    // Unlink the update and read-only txns, then deallocate the update txn
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());

    auto *txn2 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));
    txn_manager.Commit(txn2, TestCallbacks::EmptyCallback, nullptr);

    // Unlink the read-only transaction
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    // It shouldn't make it to the second invocation because it's read-only
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

// Corresponds to (MVCCTests, AbortUpdate1)
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, AbortUpdate1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager.BeginTransaction();
    storage::TupleSlot slot = tested.table_.Insert(txn, *insert_tuple);
    txn_manager.Commit(txn, TestCallbacks::EmptyCallback, nullptr);

    // Unlink and reclaim the Insert
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager.BeginTransaction();

    EXPECT_TRUE(tested.table_.Update(txn0, slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    auto *txn1 = txn_manager.BeginTransaction();

    // Nothing should be able to be GC'd yet because txn0 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Abort(txn0);

    // Aborted transactions can be removed from the unlink queue immediately
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    // But it's not safe to deallocate it yet because txn #1 is still running and may hold a reference to it
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn1, TestCallbacks::EmptyCallback, nullptr);

    // Deallocate the aborted txn, and process the read-only transaction
    EXPECT_EQ(std::make_pair(1u, 1u), gc.PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    auto *txn2 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));
    txn_manager.Commit(txn2, TestCallbacks::EmptyCallback, nullptr);

    // Unlink the read-only transaction
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    // It shouldn't make it to the second invocation because it's read-only
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

// Corresponds to (MVCCTests, AbortUpdate2)
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, AbortUpdate2) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager.BeginTransaction();
    storage::TupleSlot slot = tested.table_.Insert(txn, *insert_tuple);
    txn_manager.Commit(txn, TestCallbacks::EmptyCallback, nullptr);

    // Unlink and reclaim the Insert
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager.BeginTransaction();

    auto *txn1 = txn_manager.BeginTransaction();

    EXPECT_TRUE(tested.table_.Update(txn1, slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    // Nothing should be able to be GC'd yet because txn1 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    txn_manager.Abort(txn1);

    select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    // Aborted transactions can be removed from the unlink queue immediately
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    // But it's not safe to deallocate it yet because txn #0 is still running and may hold a reference to it
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn0, TestCallbacks::EmptyCallback, nullptr);

    // Deallocate the aborted txn, and process the read-only transaction
    EXPECT_EQ(std::make_pair(1u, 1u), gc.PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    auto *txn2 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));
    txn_manager.Commit(txn2, TestCallbacks::EmptyCallback, nullptr);

    // Unlink the read-only transaction
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    // It shouldn't make it to the second invocation because it's read-only
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

// Corresponds to (MVCCTests, InsertUpdate1)
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, InsertUpdate1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *txn0 = txn_manager.BeginTransaction();

    auto *txn1 = txn_manager.BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn1, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    // Nothing should be able to be GC'd yet because txn1 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn1, TestCallbacks::EmptyCallback, nullptr);

    // Nothing should be able to be GC'd yet because txn0 started before txn1's commit
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);
    EXPECT_FALSE(tested.table_.Update(txn0, slot, *update));

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager.Abort(txn0);

    // Process the insert and aborted txns. Both should make it to the unlink phase
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(2u, 0u), gc.PerformGarbageCollection());
  }
}

// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, SingleOLAP) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *txn0 = txn_manager.BeginTransaction();

    auto *txn1 = txn_manager.BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn1, *insert_tuple);
    txn_manager.Commit(txn1, TestCallbacks::EmptyCallback, nullptr);

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    auto *txn2 = txn_manager.BeginTransaction();

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);
    tested.table_.Update(txn2, slot, *update);
    txn_manager.Commit(txn2, TestCallbacks::EmptyCallback, nullptr);

    auto *txn3 = txn_manager.BeginTransaction();

    update = tested.GenerateRandomUpdate(&generator_);
    tested.table_.Update(txn3, slot, *update);
    txn_manager.Commit(txn3, TestCallbacks::EmptyCallback, nullptr);

    auto *txn4 = txn_manager.BeginTransaction();

    update = tested.GenerateRandomUpdate(&generator_);
    tested.table_.Update(txn4, slot, *update);
    txn_manager.Commit(txn4, TestCallbacks::EmptyCallback, nullptr);

    // Txn 2, 3 will be unlinked. Can't unlink 4 as it installed version chain head and 0 is still active
    // Can't unlink txn 1 as it is an Insert Undo Record
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager.Commit(txn0, TestCallbacks::EmptyCallback, nullptr);
    // Unlink txn 4 as txn 0 committed and unlink read-only txn 0 and unlink txn 1 as no active txn
    // Deallocate txn 2, 3
    EXPECT_EQ(std::make_pair(2u, 3u), gc.PerformGarbageCollection());
    // Deallocate txn 4 and txn 1
    EXPECT_EQ(std::make_pair(2u, 0u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

/*
 * This tests the case where there exists multiple OLAP transactions
 * and thus multiple compactions are required.
 *
 * This is the version chain:
 * U5
 * U4
 * U3
 * T1 <- active txn
 * U2
 * U1
 * INSERT
 * T0 <- active txn
 */
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, InterleavedOLAP) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    // T0
    auto *txn0 = txn_manager.BeginTransaction();

    // INSERT
    auto *txn1 = txn_manager.BeginTransaction();
    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn1, *insert_tuple);
    txn_manager.Commit(txn1, TestCallbacks::EmptyCallback, nullptr);

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    // U1
    auto *txn2 = txn_manager.BeginTransaction();
    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);
    tested.table_.Update(txn2, slot, *update);
    txn_manager.Commit(txn2, TestCallbacks::EmptyCallback, nullptr);

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    // U2
    auto *txn3 = txn_manager.BeginTransaction();
    update = tested.GenerateRandomUpdate(&generator_);
    tested.table_.Update(txn3, slot, *update);
    txn_manager.Commit(txn3, TestCallbacks::EmptyCallback, nullptr);

    update_tuple = tested.GenerateVersionFromUpdate(*update, *update_tuple);

    // T1
    auto *txn4 = txn_manager.BeginTransaction();

    // U3
    auto *txn5 = txn_manager.BeginTransaction();
    update = tested.GenerateRandomUpdate(&generator_);
    tested.table_.Update(txn5, slot, *update);
    txn_manager.Commit(txn5, TestCallbacks::EmptyCallback, nullptr);

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn4, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    // U4
    auto *txn6 = txn_manager.BeginTransaction();
    update = tested.GenerateRandomUpdate(&generator_);
    tested.table_.Update(txn6, slot, *update);
    txn_manager.Commit(txn6, TestCallbacks::EmptyCallback, nullptr);

    // U5
    auto *txn7 = txn_manager.BeginTransaction();
    update = tested.GenerateRandomUpdate(&generator_);
    tested.table_.Update(txn7, slot, *update);
    txn_manager.Commit(txn7, TestCallbacks::EmptyCallback, nullptr);
    // U1, U2, U3, U4 will be unlinked. Can't unlink U5 as it installed version chain head and T0, T1 are still active
    // Can't unlink INSERT as it inserted tuple
    EXPECT_EQ(std::make_pair(0u, 4u), gc.PerformGarbageCollection());
    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    select_tuple = tested.SelectIntoBuffer(txn4, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    txn_manager.Commit(txn4, TestCallbacks::EmptyCallback, nullptr);
    // Unlink T1
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager.Commit(txn0, TestCallbacks::EmptyCallback, nullptr);
    // Unlink read-only T0 and U5, INSERT as T0 committed and internal transaction
    // Deallocate  U1, U2, U3, U4
    EXPECT_EQ(std::make_pair(4u, 3u), gc.PerformGarbageCollection());
    // Deallocate U5 and INSERT
    EXPECT_EQ(std::make_pair(2u, 0u), gc.PerformGarbageCollection());
    // Nothing should get unlinked or deallocated
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

/*
 * This tests multiple tuple version chains.
 *
 * This is the version chain:
 * Tuple 1:            Tuple 2:
 *                     U2'
 *                     U1'
 *                     INSERT'
 *        T1 <- active txn
 * U2
 * U1
 * INSERT
 *        T0 <- active txn
 */
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, TwoTupleOLAP) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    // T0
    auto *txn0 = txn_manager.BeginTransaction();

    // INSERT
    auto *txn1 = txn_manager.BeginTransaction();
    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn1, *insert_tuple);
    txn_manager.Commit(txn1, TestCallbacks::EmptyCallback, nullptr);

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    // U1
    auto *txn2 = txn_manager.BeginTransaction();
    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);
    tested.table_.Update(txn2, slot, *update);
    txn_manager.Commit(txn2, TestCallbacks::EmptyCallback, nullptr);

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    // U2
    auto *txn3 = txn_manager.BeginTransaction();
    update = tested.GenerateRandomUpdate(&generator_);
    tested.table_.Update(txn3, slot, *update);
    txn_manager.Commit(txn3, TestCallbacks::EmptyCallback, nullptr);

    update_tuple = tested.GenerateVersionFromUpdate(*update, *update_tuple);

    // T1
    auto *txn4 = txn_manager.BeginTransaction();

    // INSERT'
    auto *txn5 = txn_manager.BeginTransaction();
    insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot2 = tested.table_.Insert(txn5, *insert_tuple);
    txn_manager.Commit(txn5, TestCallbacks::EmptyCallback, nullptr);

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);
    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn4, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));
    tested.SelectIntoBuffer(txn0, slot2);
    EXPECT_FALSE(tested.select_result_);
    tested.SelectIntoBuffer(txn4, slot2);
    EXPECT_FALSE(tested.select_result_);

    // U1'
    auto *txn6 = txn_manager.BeginTransaction();
    storage::ProjectedRow *update2 = tested.GenerateRandomUpdate(&generator_);
    tested.table_.Update(txn6, slot2, *update2);
    txn_manager.Commit(txn6, TestCallbacks::EmptyCallback, nullptr);

    // U2'
    auto *txn7 = txn_manager.BeginTransaction();
    update2 = tested.GenerateRandomUpdate(&generator_);
    tested.table_.Update(txn7, slot2, *update2);
    txn_manager.Commit(txn7, TestCallbacks::EmptyCallback, nullptr);
    // U1, U1' will be unlinked. Can't unlink U2', U2 as they installed version chain head and T0, T1 are still active
    // Can't unlink INSERT, INSERT' as they inserted tuple
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    select_tuple = tested.SelectIntoBuffer(txn4, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    tested.SelectIntoBuffer(txn0, slot2);
    EXPECT_FALSE(tested.select_result_);

    tested.SelectIntoBuffer(txn4, slot2);
    EXPECT_FALSE(tested.select_result_);

    txn_manager.Commit(txn4, TestCallbacks::EmptyCallback, nullptr);
    // Unlink T1
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    tested.SelectIntoBuffer(txn0, slot2);
    EXPECT_FALSE(tested.select_result_);

    txn_manager.Commit(txn0, TestCallbacks::EmptyCallback, nullptr);
    // Unlink read-only T0 and INSERT, U2, INSERT', U2' as T0 committed
    // Deallocate U1, U1'
    EXPECT_EQ(std::make_pair(2u, 5u), gc.PerformGarbageCollection());
    // Deallocate INSERT, U2, INSERT', U2'
    EXPECT_EQ(std::make_pair(4u, 0u), gc.PerformGarbageCollection());
    // Nothing should get unlinked or deallocated
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, MultipleIntervalTest) {
  /*
   * This tests that:
   * - All versions belonging compacted intervals get collected
   * - Insert and Header Records shouldn't get collected if there is an older txn
   * - Single Record Versions shouldn't get collected if there is an older txn
   *
   * This is how the version chain and the txn commit times should look like:
   * HEADER
   * A
   * B
   * C
   * T1 <- active
   * D
   * T2 <- active
   * E
   * INSERT
   * T3 <- active
   */
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    // T3
    auto *txn3 = txn_manager.BeginTransaction();

    // Insert
    auto *txn_insert = txn_manager.BeginTransaction();
    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn_insert, *insert_tuple);
    txn_manager.Commit(txn_insert, TestCallbacks::EmptyCallback, nullptr);

    tested.SelectIntoBuffer(txn3, slot);
    EXPECT_FALSE(tested.select_result_);

    // E
    auto *txn_e = txn_manager.BeginTransaction();
    auto *update_tuple_e = tested.GenerateRandomTuple(&generator_);
    tested.table_.Update(txn_e, slot, *update_tuple_e);
    txn_manager.Commit(txn_e, TestCallbacks::EmptyCallback, nullptr);

    tested.SelectIntoBuffer(txn3, slot);
    EXPECT_FALSE(tested.select_result_);

    // T2
    auto *txn2 = txn_manager.BeginTransaction();

    // D
    auto *txn_d = txn_manager.BeginTransaction();
    auto *update_tuple_d = tested.GenerateRandomTuple(&generator_);
    tested.table_.Update(txn_d, slot, *update_tuple_d);
    txn_manager.Commit(txn_d, TestCallbacks::EmptyCallback, nullptr);

    auto *select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple_e));

    // T1
    auto *txn1 = txn_manager.BeginTransaction();

    // C
    auto *txn_c = txn_manager.BeginTransaction();
    auto *update_tuple = tested.GenerateRandomTuple(&generator_);
    tested.table_.Update(txn_c, slot, *update_tuple);
    txn_manager.Commit(txn_c, TestCallbacks::EmptyCallback, nullptr);

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple_d));

    // B
    auto *txn_b = txn_manager.BeginTransaction();
    update_tuple = tested.GenerateRandomTuple(&generator_);
    tested.table_.Update(txn_b, slot, *update_tuple);
    txn_manager.Commit(txn_b, TestCallbacks::EmptyCallback, nullptr);

    // A
    auto *txn_a = txn_manager.BeginTransaction();
    update_tuple = tested.GenerateRandomTuple(&generator_);
    tested.table_.Update(txn_a, slot, *update_tuple);
    txn_manager.Commit(txn_a, TestCallbacks::EmptyCallback, nullptr);

    // Header
    auto *txn_header = txn_manager.BeginTransaction();
    update_tuple = tested.GenerateRandomTuple(&generator_);
    tested.table_.Update(txn_header, slot, *update_tuple);
    txn_manager.Commit(txn_header, TestCallbacks::EmptyCallback, nullptr);

    // Currently running txns T1, T2 and T3 should see the correct versions
    tested.SelectIntoBuffer(txn3, slot);
    EXPECT_FALSE(tested.select_result_);

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple_e));

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple_d));

    // A, B, C, E should be unlinked
    EXPECT_EQ(std::make_pair(0u, 4u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn3, TestCallbacks::EmptyCallback, nullptr);
    // T3 (read only) should be unlinked and deallocated
    // INSERT should be unlinked
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn2, TestCallbacks::EmptyCallback, nullptr);
    // T2 (read only) should be unlinked and deallocated
    // D should be unlinked
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn1, TestCallbacks::EmptyCallback, nullptr);
    // T1 (read only) should be unlinked and deallocated
    // HEADER should be unlinked
    // A, B, C, D, E, INSERT should be deallocated
    EXPECT_EQ(std::make_pair(6u, 2u), gc.PerformGarbageCollection());

    // HEADER should be deallocated
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());

    // Nothing left to collect
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, UncommittedIntervalTest) {
  /*
   * This tests that the uncommitted versions at the head of the chain are skipped,
   * but collected once the corresponding txn is committed
   *
   * This is the version chain:
   * U3 (by T1)
   * U2 (by T1)
   * U1 (by T1)
   * T1 <- active txn
   * E
   * D
   * INSERT
   * T2 <- active txn
   */
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    // T2
    auto *txn2 = txn_manager.BeginTransaction();

    // Insert
    auto *txn_insert = txn_manager.BeginTransaction();
    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn_insert, *insert_tuple);
    txn_manager.Commit(txn_insert, TestCallbacks::EmptyCallback, nullptr);

    tested.SelectIntoBuffer(txn2, slot);
    EXPECT_FALSE(tested.select_result_);

    // E
    auto *txn_e = txn_manager.BeginTransaction();
    auto *update_tuple_e = tested.GenerateRandomTuple(&generator_);
    tested.table_.Update(txn_e, slot, *update_tuple_e);
    txn_manager.Commit(txn_e, TestCallbacks::EmptyCallback, nullptr);

    // D
    auto *txn_d = txn_manager.BeginTransaction();
    auto *update_tuple_d = tested.GenerateRandomTuple(&generator_);
    tested.table_.Update(txn_d, slot, *update_tuple_d);
    txn_manager.Commit(txn_d, TestCallbacks::EmptyCallback, nullptr);

    tested.SelectIntoBuffer(txn2, slot);
    EXPECT_FALSE(tested.select_result_);

    // T1
    auto *txn1 = txn_manager.BeginTransaction();

    auto *update_tuple = tested.GenerateRandomTuple(&generator_);
    tested.table_.Update(txn1, slot, *update_tuple);

    update_tuple = tested.GenerateRandomTuple(&generator_);
    tested.table_.Update(txn1, slot, *update_tuple);

    update_tuple = tested.GenerateRandomTuple(&generator_);
    tested.table_.Update(txn1, slot, *update_tuple);

    // D should be unlinked
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn2, TestCallbacks::EmptyCallback, nullptr);
    // T2 (read only) should be unlinked and deallocated
    // INSERT should be unlinked
    // E should not be unlinked as it is the first committed undo record
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());

    // T1 should be able to read the correct version
    auto *select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    txn_manager.Commit(txn1, TestCallbacks::EmptyCallback, nullptr);
    // T1, E should be unlinked
    // D, INSERT should be deallocated
    EXPECT_EQ(std::make_pair(2u, 2u), gc.PerformGarbageCollection());

    // T1, E should be deallocated
    EXPECT_EQ(std::make_pair(2u, 0u), gc.PerformGarbageCollection());

    // Nothing left to collect
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, DeleteTest1) {
  /*
   * Consider performing GC on the version chain sorted from newest to oldest:
   * DELETE
   * INSERT
   * txn1 <- active txn
   */
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    // TXN
    auto *txn = txn_manager.BeginTransaction();

    // INSERT
    auto *txn_insert = txn_manager.BeginTransaction();
    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn_insert, *insert_tuple);
    txn_manager.Commit(txn_insert, TestCallbacks::EmptyCallback, nullptr);

    // DELETE
    auto *txn_delete = txn_manager.BeginTransaction();
    tested.table_.Delete(txn_delete, slot);
    txn_manager.Commit(txn_delete, TestCallbacks::EmptyCallback, nullptr);

    // Active txn should not see any version
    tested.SelectIntoBuffer(txn, slot);
    EXPECT_FALSE(tested.select_result_);

    // Nothing should be collected
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    // Both DELETE and INSERT should be unlinked
    // TXN (read only) should be unlinked and deallocated
    EXPECT_EQ(std::make_pair(0u, 3u), gc.PerformGarbageCollection());

    // DELETE and INSERT should be deallocated
    EXPECT_EQ(std::make_pair(2u, 0u), gc.PerformGarbageCollection());

    // Nothing should be unlinked or deallocated
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, DeleteTest2) {
  /*
   * Consider performing GC on the version chain sorted from newest to oldest:
   * DELETE
   * txn1 <- active txn
   * INSERT
   */
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    // INSERT
    auto *txn_insert = txn_manager.BeginTransaction();
    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn_insert, *insert_tuple);
    txn_manager.Commit(txn_insert, TestCallbacks::EmptyCallback, nullptr);

    // TXN
    auto *txn = txn_manager.BeginTransaction();

    // DELETE
    auto *txn_delete = txn_manager.BeginTransaction();
    tested.table_.Delete(txn_delete, slot);
    txn_manager.Commit(txn_delete, TestCallbacks::EmptyCallback, nullptr);

    // Active txn should see correct version
    auto *select_tuple = tested.SelectIntoBuffer(txn, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    // INSERT should be unlinked
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    // DELETE should be unlinked
    // TXN (read only) should be unlinked and deallocated
    // INSERT should be deallocated
    EXPECT_EQ(std::make_pair(1u, 2u), gc.PerformGarbageCollection());

    // DELETE should be deallocated
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());

    // Nothing should be unlinked or deallocated
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, DeleteTest3) {
  /*
   * Consider performing GC on the version chain sorted from newest to oldest:
   * txn1 <- active txn
   * DELETE
   * INSERT
   */
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    // INSERT
    auto *txn_insert = txn_manager.BeginTransaction();
    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn_insert, *insert_tuple);
    txn_manager.Commit(txn_insert, TestCallbacks::EmptyCallback, nullptr);

    // DELETE
    auto *txn_delete = txn_manager.BeginTransaction();
    tested.table_.Delete(txn_delete, slot);
    txn_manager.Commit(txn_delete, TestCallbacks::EmptyCallback, nullptr);

    // TXN
    auto *txn = txn_manager.BeginTransaction();

    // Active txn should not see any version
    tested.SelectIntoBuffer(txn, slot);
    EXPECT_FALSE(tested.select_result_);

    // Both DELETE and INSERT should be unlinked
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    // TXN (read only) should be unlinked and deallocated
    // DELETE and INSERT should be deallocated
    EXPECT_EQ(std::make_pair(2u, 1u), gc.PerformGarbageCollection());

    // Nothing should be unlinked or deallocated
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, DeleteUpdateTest1) {
  /*
   * Consider performing GC on the version chain sorted from newest to oldest:
   * DELETE
   * U2
   * U1
   * INSERT
   * txn1 <- active txn
   */
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    // TXN
    auto *txn = txn_manager.BeginTransaction();

    // INSERT
    auto *txn_insert = txn_manager.BeginTransaction();
    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn_insert, *insert_tuple);
    txn_manager.Commit(txn_insert, TestCallbacks::EmptyCallback, nullptr);

    // U1
    auto *txn_u1 = txn_manager.BeginTransaction();
    auto *update_tuple_1 = tested.GenerateRandomTuple(&generator_);
    tested.table_.Update(txn_u1, slot, *update_tuple_1);
    txn_manager.Commit(txn_u1, TestCallbacks::EmptyCallback, nullptr);

    // U2
    auto *txn_u2 = txn_manager.BeginTransaction();
    auto *update_tuple_2 = tested.GenerateRandomTuple(&generator_);
    tested.table_.Update(txn_u2, slot, *update_tuple_2);
    txn_manager.Commit(txn_u2, TestCallbacks::EmptyCallback, nullptr);

    // DELETE
    auto *txn_delete = txn_manager.BeginTransaction();
    tested.table_.Delete(txn_delete, slot);
    txn_manager.Commit(txn_delete, TestCallbacks::EmptyCallback, nullptr);

    // Active txn should not see any version
    tested.SelectIntoBuffer(txn, slot);
    EXPECT_FALSE(tested.select_result_);

    // U1, U2 should be compacted and unlinked
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    // DELETE, INSERT should be unlinked
    // TXN (read only) should be unlinked and deallocated
    // U1, U2 should be deallocated
    EXPECT_EQ(std::make_pair(2u, 3u), gc.PerformGarbageCollection());

    // DELETE, INSERT should be deallocated
    EXPECT_EQ(std::make_pair(2u, 0u), gc.PerformGarbageCollection());

    // Nothing should be unlinked or deallocated
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, DeleteUpdateTest2) {
  /*
   * Consider performing GC on the version chain sorted from newest to oldest:
   * DELETE
   * U2
   * txn1 <- active txn
   * U1
   * INSERT
   */
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    // INSERT
    auto *txn_insert = txn_manager.BeginTransaction();
    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn_insert, *insert_tuple);
    txn_manager.Commit(txn_insert, TestCallbacks::EmptyCallback, nullptr);

    // U1
    auto *txn_u1 = txn_manager.BeginTransaction();
    auto *update_tuple_1 = tested.GenerateRandomTuple(&generator_);
    tested.table_.Update(txn_u1, slot, *update_tuple_1);
    txn_manager.Commit(txn_u1, TestCallbacks::EmptyCallback, nullptr);

    // TXN
    auto *txn = txn_manager.BeginTransaction();

    // U2
    auto *txn_u2 = txn_manager.BeginTransaction();
    auto *update_tuple_2 = tested.GenerateRandomTuple(&generator_);
    tested.table_.Update(txn_u2, slot, *update_tuple_2);
    txn_manager.Commit(txn_u2, TestCallbacks::EmptyCallback, nullptr);

    // DELETE
    auto *txn_delete = txn_manager.BeginTransaction();
    tested.table_.Delete(txn_delete, slot);
    txn_manager.Commit(txn_delete, TestCallbacks::EmptyCallback, nullptr);

    // Active txn should see correct version
    auto *select_tuple = tested.SelectIntoBuffer(txn, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple_1));

    // INSERT, U1 should be unlinked
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    // DELETE, U2 should be unlinked
    // TXN (read only) should be unlinked and deallocated
    // INSERT, U1 should be deallocated
    EXPECT_EQ(std::make_pair(2u, 3u), gc.PerformGarbageCollection());

    // DELETE, U2 should be deallocated
    EXPECT_EQ(std::make_pair(2u, 0u), gc.PerformGarbageCollection());

    // Nothing should be unlinked or deallocated
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, DeleteUpdateTest3) {
  /*
   * Consider performing GC on the version chain sorted from newest to oldest:
   * DELETE
   * txn1 <- active txn
   * U2
   * U1
   * INSERT
   */
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    // INSERT
    auto *txn_insert = txn_manager.BeginTransaction();
    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn_insert, *insert_tuple);
    txn_manager.Commit(txn_insert, TestCallbacks::EmptyCallback, nullptr);

    // U1
    auto *txn_u1 = txn_manager.BeginTransaction();
    auto *update_tuple_1 = tested.GenerateRandomTuple(&generator_);
    tested.table_.Update(txn_u1, slot, *update_tuple_1);
    txn_manager.Commit(txn_u1, TestCallbacks::EmptyCallback, nullptr);

    // U2
    auto *txn_u2 = txn_manager.BeginTransaction();
    auto *update_tuple_2 = tested.GenerateRandomTuple(&generator_);
    tested.table_.Update(txn_u2, slot, *update_tuple_2);
    txn_manager.Commit(txn_u2, TestCallbacks::EmptyCallback, nullptr);

    // TXN
    auto *txn = txn_manager.BeginTransaction();

    // DELETE
    auto *txn_delete = txn_manager.BeginTransaction();
    tested.table_.Delete(txn_delete, slot);
    txn_manager.Commit(txn_delete, TestCallbacks::EmptyCallback, nullptr);

    // Active txn should see correct version
    auto *select_tuple = tested.SelectIntoBuffer(txn, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple_2));

    // INSERT, U1, U2 should be unlinked
    EXPECT_EQ(std::make_pair(0u, 3u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    // DELETE should be unlinked
    // TXN (read only) should be unlinked and deallocated
    // INSERT, U1, U2 should be deallocated
    EXPECT_EQ(std::make_pair(3u, 2u), gc.PerformGarbageCollection());

    // DELETE should be deallocated
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());

    // Nothing should be unlinked or deallocated
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, DeleteUpdateTest4) {
  /*
   * Consider performing GC on the version chain sorted from newest to oldest:
   * txn1 <- active txn
   * DELETE
   * U2
   * U1
   * INSERT
   */
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    // INSERT
    auto *txn_insert = txn_manager.BeginTransaction();
    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn_insert, *insert_tuple);
    txn_manager.Commit(txn_insert, TestCallbacks::EmptyCallback, nullptr);

    // U1
    auto *txn_u1 = txn_manager.BeginTransaction();
    auto *update_tuple_1 = tested.GenerateRandomTuple(&generator_);
    tested.table_.Update(txn_u1, slot, *update_tuple_1);
    txn_manager.Commit(txn_u1, TestCallbacks::EmptyCallback, nullptr);

    // U2
    auto *txn_u2 = txn_manager.BeginTransaction();
    auto *update_tuple_2 = tested.GenerateRandomTuple(&generator_);
    tested.table_.Update(txn_u2, slot, *update_tuple_2);
    txn_manager.Commit(txn_u2, TestCallbacks::EmptyCallback, nullptr);

    // DELETE
    auto *txn_delete = txn_manager.BeginTransaction();
    tested.table_.Delete(txn_delete, slot);
    txn_manager.Commit(txn_delete, TestCallbacks::EmptyCallback, nullptr);

    // TXN
    auto *txn = txn_manager.BeginTransaction();

    // Active txn should not see any version
    tested.SelectIntoBuffer(txn, slot);
    EXPECT_FALSE(tested.select_result_);

    // DELETE, INSERT, U1, U2 should be unlinked
    EXPECT_EQ(std::make_pair(0u, 4u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
    // TXN (read only) should be unlinked and deallocated
    // DELETE, INSERT, U1, U2 should be deallocated
    EXPECT_EQ(std::make_pair(4u, 1u), gc.PerformGarbageCollection());

    // Nothing should be unlinked or deallocated
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}
}  // namespace terrier
