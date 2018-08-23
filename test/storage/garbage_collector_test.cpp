#include <unordered_map>
#include <utility>
#include <vector>
#include "common/object_pool.h"
#include "storage/data_table.h"
#include "storage/garbage_collector.h"
#include "storage/storage_util.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace terrier {
// Not thread-safe
class GarbageCollectorDataTableTestObject {
 public:
  template<class Random>
  GarbageCollectorDataTableTestObject(storage::BlockStore *block_store,
                                      const uint16_t max_col,
                                      Random *generator)
      : layout_(StorageTestUtil::RandomLayout(max_col, generator)),
        table_(block_store, layout_) {}

  ~GarbageCollectorDataTableTestObject() {
    for (auto ptr : loose_pointers_)
      delete[] ptr;
    delete[] select_buffer_;
  }

  const storage::BlockLayout &Layout() const { return layout_; }

  template<class Random>
  storage::ProjectedRow *GenerateRandomTuple(Random *generator) {
    auto *buffer = new byte[redo_size_];
    loose_pointers_.push_back(buffer);
    storage::ProjectedRow
        *redo = storage::ProjectedRow::InitializeProjectedRow(buffer, all_col_ids_, layout_);
    StorageTestUtil::PopulateRandomRow(redo, layout_, null_bias_, generator);
    return redo;
  }

  template<class Random>
  storage::ProjectedRow *GenerateRandomUpdate(Random *generator) {
    std::vector<uint16_t> update_col_ids = StorageTestUtil::ProjectionListRandomColumns(layout_, generator);
    auto *buffer = new byte[storage::ProjectedRow::Size(layout_, update_col_ids)];
    loose_pointers_.push_back(buffer);
    storage::ProjectedRow *update =
        storage::ProjectedRow::InitializeProjectedRow(buffer, update_col_ids, layout_);
    StorageTestUtil::PopulateRandomRow(update, layout_, null_bias_, generator);
    return update;
  }

  storage::ProjectedRow *GenerateVersionFromUpdate(const storage::ProjectedRow &delta,
                                                   const storage::ProjectedRow &previous) {
    auto *buffer = new byte[redo_size_];
    loose_pointers_.push_back(buffer);
    // Copy previous version
    TERRIER_MEMCPY(buffer, &previous, redo_size_);
    auto *version = reinterpret_cast<storage::ProjectedRow *>(buffer);
    std::unordered_map<uint16_t, uint16_t> col_to_projection_list_index;
    for (uint16_t i = 0; i < version->NumColumns(); i++)
      col_to_projection_list_index.emplace(version->ColumnIds()[i], i);
    storage::StorageUtil::ApplyDelta(layout_, delta, version, col_to_projection_list_index);
    return version;
  }

  storage::ProjectedRow *SelectIntoBuffer(transaction::TransactionContext *const txn,
                                          const storage::TupleSlot slot,
                                          const std::vector<uint16_t> &col_ids) {
    // generate a redo ProjectedRow for Select
    storage::ProjectedRow *select_row = storage::ProjectedRow::InitializeProjectedRow(select_buffer_, col_ids, layout_);
    table_.Select(txn, slot, select_row);
    return select_row;
  }

  storage::BlockLayout layout_;
  storage::DataTable table_;
  // We want null_bias_ to be zero when testing CC. We already evaluate null correctness in other directed tests, and
  // we don't want the logically deleted field to end up set NULL.
  const double null_bias_ = 0;
  std::vector<byte *> loose_pointers_;
  std::vector<uint16_t> all_col_ids_{StorageTestUtil::ProjectionListAllColumns(layout_)};
  // These always over-provision in the case of partial selects or deltas, which is fine.
  uint32_t redo_size_ = storage::ProjectedRow::Size(layout_, all_col_ids_);
  byte *select_buffer_ = new byte[redo_size_];
};

struct GarbageCollectorTests : public ::terrier::TerrierTest {
  storage::BlockStore block_store_{100};
  common::ObjectPool<storage::BufferSegment> buffer_pool_{10000};
  std::default_random_engine generator_;
  const uint32_t num_iterations_ = 5;
  const uint16_t max_columns_ = 100;
};

// Run a single txn that performs an Insert. Confirm that it takes 2 GC cycles to process this tuple.
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, SingleInsert) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager{&buffer_pool_, true};
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *txn0 = txn_manager.BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn0, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    // Nothing should be able to be GC'd yet because txn0 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn0);

    // Unlink the Insert's UndoRecord, then deallocate it on the next run
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());
  }
}

// Run a single read-only txn (empty UndoBuffer). Confirm that it takes 1 GC cycles to process this tuple.
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, ReadOnly) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager{&buffer_pool_, true};
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *txn0 = txn_manager.BeginTransaction();
    txn_manager.Commit(txn0);

    // Unlink the txn and deallocate immediately because it's read-only
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

// Corresponds to (MVCCTests, CommitInsert1)
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, CommitInsert1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager{&buffer_pool_, true};
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *txn0 = txn_manager.BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn0, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    // Nothing should be able to be GC'd yet because txn0 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    auto *txn1 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn0);

    // Nothing should be able to be GC'd yet because txn1 started before txn0's commit
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn1);

    // Unlink the two transactions and then deallocate the single non-read-only transaction
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());

    auto *txn2 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));
    txn_manager.Commit(txn2);

    // Unlink the read-only transaction, and it shouldn't make it to the second invocation because it's read-only
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());
  }
}

// Corresponds to (MVCCTests, CommitInsert2)
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, CommitInsert2) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager{&buffer_pool_, true};
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *txn0 = txn_manager.BeginTransaction();

    auto *txn1 = txn_manager.BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn1, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    // Nothing should be able to be GC'd yet because txn0 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn1);

    select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    // Nothing should be able to be GC'd yet because txn0 started before txn1's commit
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn0);

    // Unlink the two transactions and then deallocate the single non-read-only transaction
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());

    auto *txn2 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));
    txn_manager.Commit(txn2);

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
    transaction::TransactionManager txn_manager{&buffer_pool_, true};
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *txn0 = txn_manager.BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn0, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    // Nothing should be able to be GC'd yet because txn0 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    auto *txn1 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Abort(txn0);

    // Aborted transactions can be removed from the unlink queue immediately
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    // But it's not safe to deallocate it yet because txn #1 is still running and may hold a reference to it
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn1);

    // Deallocate the aborted txn, and process the read-only transaction
    EXPECT_EQ(std::make_pair(1u, 1u), gc.PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    auto *txn2 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));
    txn_manager.Commit(txn2);

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
    transaction::TransactionManager txn_manager{&buffer_pool_, true};
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *txn0 = txn_manager.BeginTransaction();

    auto *txn1 = txn_manager.BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn1, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    // Nothing should be able to be GC'd yet because txn1 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    txn_manager.Abort(txn1);

    // Aborted transactions can be removed from the unlink queue immediately
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    // But it's not safe to deallocate it yet because txn #0 is still running and may hold a reference to it
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn0);

    // Deallocate the aborted txn, and process the read-only transaction
    EXPECT_EQ(std::make_pair(1u, 1u), gc.PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    auto *txn2 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));
    txn_manager.Commit(txn2);

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
    transaction::TransactionManager txn_manager{&buffer_pool_, true};
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager.BeginTransaction();
    storage::TupleSlot slot = tested.table_.Insert(txn, *insert_tuple);
    txn_manager.Commit(txn);

    // Unlink and reclaim the Insert
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager.BeginTransaction();

    EXPECT_TRUE(tested.table_.Update(txn0, slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));

    // Nothing should be able to be GC'd yet because txn0 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    auto *txn1 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn0);

    // Nothing should be able to be GC'd yet because txn1 started before txn0's commit
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn1);

    // Unlink the update and read-only txns, then deallocate the update txn
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());

    auto *txn2 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));
    txn_manager.Commit(txn2);

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
    transaction::TransactionManager txn_manager{&buffer_pool_, true};
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager.BeginTransaction();
    storage::TupleSlot slot = tested.table_.Insert(txn, *insert_tuple);
    txn_manager.Commit(txn);

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

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));

    txn_manager.Commit(txn1);

    // Nothing should be able to be GC'd yet because txn0 started before txn1's commit
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn0);

    // Unlink the update and read-only txns, then deallocate the update txn
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());

    auto *txn2 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));
    txn_manager.Commit(txn2);

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
    transaction::TransactionManager txn_manager{&buffer_pool_, true};
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager.BeginTransaction();
    storage::TupleSlot slot = tested.table_.Insert(txn, *insert_tuple);
    txn_manager.Commit(txn);

    // Unlink and reclaim the Insert
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager.BeginTransaction();

    EXPECT_TRUE(tested.table_.Update(txn0, slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));

    auto *txn1 = txn_manager.BeginTransaction();

    // Nothing should be able to be GC'd yet because txn0 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Abort(txn0);

    // Aborted transactions can be removed from the unlink queue immediately
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    // But it's not safe to deallocate it yet because txn #1 is still running and may hold a reference to it
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn1);

    // Deallocate the aborted txn, and process the read-only transaction
    EXPECT_EQ(std::make_pair(1u, 1u), gc.PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    auto *txn2 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));
    txn_manager.Commit(txn2);

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
    transaction::TransactionManager txn_manager{&buffer_pool_, true};
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager.BeginTransaction();
    storage::TupleSlot slot = tested.table_.Insert(txn, *insert_tuple);
    txn_manager.Commit(txn);

    // Unlink and reclaim the Insert
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(1u, 0u), gc.PerformGarbageCollection());

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager.BeginTransaction();

    auto *txn1 = txn_manager.BeginTransaction();

    EXPECT_TRUE(tested.table_.Update(txn1, slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));

    // Nothing should be able to be GC'd yet because txn1 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    txn_manager.Abort(txn1);

    select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    // Aborted transactions can be removed from the unlink queue immediately
    EXPECT_EQ(std::make_pair(0u, 1u), gc.PerformGarbageCollection());
    // But it's not safe to deallocate it yet because txn #0 is still running and may hold a reference to it
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn0);

    // Deallocate the aborted txn, and process the read-only transaction
    EXPECT_EQ(std::make_pair(1u, 1u), gc.PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    auto *txn2 = txn_manager.BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));
    txn_manager.Commit(txn2);

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
    transaction::TransactionManager txn_manager{&buffer_pool_, true};
    GarbageCollectorDataTableTestObject tested(&block_store_, max_columns_, &generator_);
    storage::GarbageCollector gc(&txn_manager);

    auto *txn0 = txn_manager.BeginTransaction();

    auto *txn1 = txn_manager.BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn1, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    // Nothing should be able to be GC'd yet because txn1 has not committed yet
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    txn_manager.Commit(txn1);

    // Nothing should be able to be GC'd yet because txn0 started before txn1's commit
    EXPECT_EQ(std::make_pair(0u, 0u), gc.PerformGarbageCollection());

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);
    EXPECT_FALSE(tested.table_.Update(txn0, slot, *update));

    select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Abort(txn0);

    // Process the insert and aborted txns. Both should make it to the unlink phase
    EXPECT_EQ(std::make_pair(0u, 2u), gc.PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(2u, 0u), gc.PerformGarbageCollection());
  }
}

}  // namespace terrier
