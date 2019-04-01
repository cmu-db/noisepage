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
    storage::ProjectedRowInitializer update_initializer(layout_, update_col_ids);
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
  storage::ProjectedRowInitializer initializer_{layout_, StorageTestUtil::ProjectionListAllColumns(layout_)};
  byte *select_buffer_ = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
  bool select_result_;
};

struct GarbageCollectorTests : public ::terrier::TerrierTest {
  storage::BlockStore block_store_{100, 100};
  storage::RecordBufferSegmentPool buffer_pool_{10000, 10000};
  std::default_random_engine generator_;
  const uint32_t num_iterations_ = 1;
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
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

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

// Insert a tuple and commit. Next transaction tries to update that tuple (takes write lock), verify that GC doesn't
// unlink or free the Insert txn's context until safe to do so
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, WriteWriteConflictRequeue) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager{&buffer_pool_, true, LOGGING_DISABLED};
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager.BeginTransaction();
    storage::TupleSlot slot = tested.table_.Insert(txn, *insert_tuple);
    txn_manager.Commit(txn, TestCallbacks::EmptyCallback, nullptr);

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager.BeginTransaction();

    EXPECT_TRUE(tested.table_.Update(txn0, slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));

    // Verify that Insert txn doesn't get fully unlinked or reclaimed until txn0 finishes
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    txn_manager.Abort(txn0);

    // Aborted transactions can be removed from the unlink queue immediately
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());
    // Safe to deallocate both transactions
    EXPECT_EQ(std::make_pair(2u, 0u), gc.PerformGarbageCollection());
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
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

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
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));
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
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

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
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));
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
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

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
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

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
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));

    // Nothing should be able to be GC'd yet because txn0 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    auto *txn1 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn0, TestCallbacks::EmptyCallback, nullptr);

    // Nothing should be able to be GC'd yet because txn1 started before txn0's commit
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn1, TestCallbacks::EmptyCallback, nullptr);

    // Unlink the update and read-only txns, then deallocate the update txn
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());

    auto *txn2 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));
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
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));

    txn_manager.Commit(txn1, TestCallbacks::EmptyCallback, nullptr);

    // Nothing should be able to be GC'd yet because txn0 started before txn1's commit
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn0, TestCallbacks::EmptyCallback, nullptr);

    // Unlink the update and read-only txns, then deallocate the update txn
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());

    auto *txn2 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));
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
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));

    auto *txn1 = txn_manager.BeginTransaction();

    // Nothing should be able to be GC'd yet because txn0 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Abort(txn0);

    // Aborted transactions can be removed from the unlink queue immediately
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    // But it's not safe to deallocate it yet because txn #1 is still running and may hold a reference to it
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn1, TestCallbacks::EmptyCallback, nullptr);

    // Deallocate the aborted txn, and process the read-only transaction
    EXPECT_EQ(std::make_pair(1u, 1u), gc.PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    auto *txn2 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));
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
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));

    // Nothing should be able to be GC'd yet because txn1 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    txn_manager.Abort(txn1);

    select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

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
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));
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
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

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
    // Also collect internal transaction which is read only
    EXPECT_EQ(std::make_pair(2u, 4u), gc.PerformGarbageCollection());
    // Deallocate txn 4
    // Also collect internal transaction which is read only
    EXPECT_EQ(std::make_pair(2u, 1u), gc.PerformGarbageCollection());
  }
}

TEST_F(GarbageCollectorTests, InterleavedOLAP) {
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

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    auto *txn3 = txn_manager.BeginTransaction();

    update = tested.GenerateRandomUpdate(&generator_);
    tested.table_.Update(txn3, slot, *update);
    txn_manager.Commit(txn3, TestCallbacks::EmptyCallback, nullptr);

    update_tuple = tested.GenerateVersionFromUpdate(*update, *update_tuple);

    auto *txn4 = txn_manager.BeginTransaction();

    auto *txn5 = txn_manager.BeginTransaction();

    update = tested.GenerateRandomUpdate(&generator_);
    tested.table_.Update(txn5, slot, *update);
    txn_manager.Commit(txn5, TestCallbacks::EmptyCallback, nullptr);

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn4, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));

    auto *txn6 = txn_manager.BeginTransaction();

    update = tested.GenerateRandomUpdate(&generator_);
    tested.table_.Update(txn6, slot, *update);
    txn_manager.Commit(txn6, TestCallbacks::EmptyCallback, nullptr);

    auto *txn7 = txn_manager.BeginTransaction();

    update = tested.GenerateRandomUpdate(&generator_);
    tested.table_.Update(txn7, slot, *update);
    txn_manager.Commit(txn7, TestCallbacks::EmptyCallback, nullptr);
    // Txn 2, 3, 5, 6 will be unlinked. Can't unlink 7 as it installed version chain head and 0, 4 are still active
    // Can't unlink txn 1 as it inserted tuple
    EXPECT_EQ(std::make_pair(0u, 4u), gc.PerformGarbageCollection());
    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    select_tuple = tested.SelectIntoBuffer(txn4, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));

    txn_manager.Commit(txn4, TestCallbacks::EmptyCallback, nullptr);
    // Unlink txn 4 and unlink internal transaction
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager.Commit(txn0, TestCallbacks::EmptyCallback, nullptr);
    // Unlink read-only txn 0 and txn 7, txn 1  as txn 0 committed and internal transaction
    // Deallocate txn 2, 3, 5, 6 and internal transaction
    EXPECT_EQ(std::make_pair(5u, 4u), gc.PerformGarbageCollection());
    // Unlink internal transaction
    // Deallocate txn 7 and 1
    EXPECT_EQ(std::make_pair(2u, 1u), gc.PerformGarbageCollection());
  }
}

TEST_F(GarbageCollectorTests, TwoTupleOLAP) {
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

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    auto *txn3 = txn_manager.BeginTransaction();

    update = tested.GenerateRandomUpdate(&generator_);
    tested.table_.Update(txn3, slot, *update);
    txn_manager.Commit(txn3, TestCallbacks::EmptyCallback, nullptr);

    update_tuple = tested.GenerateVersionFromUpdate(*update, *update_tuple);

    auto *txn4 = txn_manager.BeginTransaction();

    auto *txn5 = txn_manager.BeginTransaction();

    insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot2 = tested.table_.Insert(txn5, *insert_tuple);
    txn_manager.Commit(txn5, TestCallbacks::EmptyCallback, nullptr);

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn4, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));

    tested.SelectIntoBuffer(txn0, slot2);
    EXPECT_FALSE(tested.select_result_);

    tested.SelectIntoBuffer(txn4, slot2);
    EXPECT_FALSE(tested.select_result_);

    auto *txn6 = txn_manager.BeginTransaction();

    storage::ProjectedRow *update2 = tested.GenerateRandomUpdate(&generator_);
    tested.table_.Update(txn6, slot2, *update2);
    txn_manager.Commit(txn6, TestCallbacks::EmptyCallback, nullptr);

    auto *txn7 = txn_manager.BeginTransaction();

    update2 = tested.GenerateRandomUpdate(&generator_);
    tested.table_.Update(txn7, slot2, *update2);
    txn_manager.Commit(txn7, TestCallbacks::EmptyCallback, nullptr);
    // Txn 2, 6 will be unlinked. Can't unlink 7, 3 as they installed version chain head and 0, 4 are still active
    // Can't unlink txn 1, 5 as they inserted tuple
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    select_tuple = tested.SelectIntoBuffer(txn4, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));

    tested.SelectIntoBuffer(txn0, slot2);
    EXPECT_FALSE(tested.select_result_);

    tested.SelectIntoBuffer(txn4, slot2);
    EXPECT_FALSE(tested.select_result_);

    txn_manager.Commit(txn4, TestCallbacks::EmptyCallback, nullptr);
    // Unlink txn 4 and internal txn
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    tested.SelectIntoBuffer(txn0, slot2);
    EXPECT_FALSE(tested.select_result_);

    txn_manager.Commit(txn0, TestCallbacks::EmptyCallback, nullptr);
    // Unlink read-only txn 0 and txn 1, 3, 5, 7 as txn 0 committed and internal txn
    // Deallocate txn 2, 6
    EXPECT_EQ(std::make_pair(2u, 6u), gc.PerformGarbageCollection());
    // Unlink internal txn
    // Deallocate txn 1, 3, 5, 7
    EXPECT_EQ(std::make_pair(4u, 1u), gc.PerformGarbageCollection());
  }
}
}  // namespace terrier
