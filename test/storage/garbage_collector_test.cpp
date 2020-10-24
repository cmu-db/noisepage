#include "storage/garbage_collector.h"

#include <cstring>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "main/db_main.h"
#include "storage/data_table.h"
#include "storage/storage_util.h"
#include "test_util/data_table_test_util.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace noisepage {
// Not thread-safe
class GarbageCollectorDataTableTestObject {
 public:
  template <class Random>
  GarbageCollectorDataTableTestObject(storage::BlockStore *block_store, const uint16_t max_col, Random *generator)
      : layout_(StorageTestUtil::RandomLayoutNoVarlen(max_col, generator)),
        table_(common::ManagedPointer(block_store), layout_, storage::layout_version_t(0)) {}

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
        storage::ProjectedRowInitializer::Create(layout_, update_col_ids);
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
    select_result_ = table_.Select(common::ManagedPointer(txn), slot, select_row);
    return select_row;
  }

  storage::BlockLayout layout_;
  storage::DataTable table_;
  // We want null_bias_ to be zero when testing CC. We already evaluate null correctness in other directed tests, and
  // we don't want the logically deleted field to end up set NULL.
  const double null_bias_ = 0;
  std::vector<byte *> loose_pointers_;
  storage::ProjectedRowInitializer initializer_ =
      storage::ProjectedRowInitializer::Create(layout_, StorageTestUtil::ProjectionListAllColumns(layout_));
  byte *select_buffer_ = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
  bool select_result_;
};

struct GarbageCollectorTests : public ::noisepage::TerrierTest {
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
    auto db_main = DBMain::Builder().SetUseGC(true).Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    auto gc = db_main->GetStorageLayer()->GetGarbageCollector();

    GarbageCollectorDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_,
                                               &generator_);

    auto *txn0 = txn_manager->BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn0), *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    // Nothing should be able to be GC'd yet because txn0 has not committed yet
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());

    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Unlink the Insert's UndoRecord, then deallocate it on the next run
    EXPECT_EQ(std::make_pair(0U, 1U), gc->PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(1U, 0U), gc->PerformGarbageCollection());
  }
}

// Run a single read-only txn (empty UndoBuffer). Confirm that it takes 1 GC cycles to process this tuple.
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, ReadOnly) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    auto db_main = DBMain::Builder().SetUseGC(true).Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    auto gc = db_main->GetStorageLayer()->GetGarbageCollector();

    GarbageCollectorDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_,
                                               &generator_);

    auto *txn0 = txn_manager->BeginTransaction();
    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Unlink the txn and deallocate immediately because it's read-only
    EXPECT_EQ(std::make_pair(0U, 1U), gc->PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());
  }
}

// Corresponds to (MVCCTests, CommitInsert1)
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, CommitInsert1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    auto db_main = DBMain::Builder().SetUseGC(true).Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    auto gc = db_main->GetStorageLayer()->GetGarbageCollector();

    GarbageCollectorDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_,
                                               &generator_);

    auto *txn0 = txn_manager->BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn0), *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    // Nothing should be able to be GC'd yet because txn0 has not committed yet
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());

    auto *txn1 = txn_manager->BeginTransaction();

    tested.SelectIntoBuffer(txn1, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Nothing should be able to be GC'd yet because txn1 started before txn0's commit
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());

    tested.SelectIntoBuffer(txn1, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Unlink the two transactions and then deallocate the single non-read-only transaction
    EXPECT_EQ(std::make_pair(0U, 2U), gc->PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(1U, 0U), gc->PerformGarbageCollection());

    auto *txn2 = txn_manager->BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Unlink the read-only transaction, and it shouldn't make it to the second invocation because it's read-only
    EXPECT_EQ(std::make_pair(0U, 1U), gc->PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());
  }
}

// Corresponds to (MVCCTests, CommitInsert2)
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, CommitInsert2) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    auto db_main = DBMain::Builder().SetUseGC(true).Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    auto gc = db_main->GetStorageLayer()->GetGarbageCollector();

    GarbageCollectorDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_,
                                               &generator_);

    auto *txn0 = txn_manager->BeginTransaction();

    auto *txn1 = txn_manager->BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn1), *insert_tuple);

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    // Nothing should be able to be GC'd yet because txn0 has not committed yet
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());

    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    // Nothing should be able to be GC'd yet because txn0 started before txn1's commit
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());

    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Unlink the two transactions and then deallocate the single non-read-only transaction
    EXPECT_EQ(std::make_pair(0U, 2U), gc->PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(1U, 0U), gc->PerformGarbageCollection());

    auto *txn2 = txn_manager->BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Unlink the read-only transaction
    EXPECT_EQ(std::make_pair(0U, 1U), gc->PerformGarbageCollection());
    // It shouldn't make it to the second invocation because it's read-only
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());
  }
}

// Corresponds to (MVCCTests, AbortInsert1)
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, AbortInsert1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    auto db_main = DBMain::Builder().SetUseGC(true).Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    auto gc = db_main->GetStorageLayer()->GetGarbageCollector();

    GarbageCollectorDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_,
                                               &generator_);

    auto *txn0 = txn_manager->BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn0), *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    // Nothing should be able to be GC'd yet because txn0 has not committed yet
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());

    auto *txn1 = txn_manager->BeginTransaction();

    tested.SelectIntoBuffer(txn1, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager->Abort(txn0);

    // Aborted transactions cannot be removed from the unlink queue immediately
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());

    tested.SelectIntoBuffer(txn1, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

    //  process the read-only transaction and aborted transaction
    EXPECT_EQ(std::make_pair(0U, 2U), gc->PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue, aborted transaction can be deallocated
    EXPECT_EQ(std::make_pair(1U, 0U), gc->PerformGarbageCollection());

    auto *txn2 = txn_manager->BeginTransaction();

    tested.SelectIntoBuffer(txn2, slot);
    EXPECT_FALSE(tested.select_result_);
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Unlink the read-only transaction
    EXPECT_EQ(std::make_pair(0U, 1U), gc->PerformGarbageCollection());
    // It shouldn't make it to the second invocation because it's read-only
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());
  }
}

// Corresponds to (MVCCTests, AbortInsert2)
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, AbortInsert2) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    auto db_main = DBMain::Builder().SetUseGC(true).Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    auto gc = db_main->GetStorageLayer()->GetGarbageCollector();

    GarbageCollectorDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_,
                                               &generator_);

    auto *txn0 = txn_manager->BeginTransaction();

    auto *txn1 = txn_manager->BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn1), *insert_tuple);

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    // Nothing should be able to be GC'd yet because txn1 has not committed yet
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());

    txn_manager->Abort(txn1);

    // Aborted transactions cannot be removed from the unlink queue immediately
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    // process the read-only transaction and aborted transcation
    EXPECT_EQ(std::make_pair(0U, 2U), gc->PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue, aborted transaction is deallocated
    EXPECT_EQ(std::make_pair(1U, 0U), gc->PerformGarbageCollection());

    auto *txn2 = txn_manager->BeginTransaction();

    tested.SelectIntoBuffer(txn2, slot);
    EXPECT_FALSE(tested.select_result_);
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Unlink the read-only transaction
    EXPECT_EQ(std::make_pair(0U, 1U), gc->PerformGarbageCollection());
    // It shouldn't make it to the second invocation because it's read-only
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());
  }
}

// Corresponds to (MVCCTests, CommitUpdate1)
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, CommitUpdate1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    auto db_main = DBMain::Builder().SetUseGC(true).Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    auto gc = db_main->GetStorageLayer()->GetGarbageCollector();

    GarbageCollectorDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_,
                                               &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager->BeginTransaction();
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn), *insert_tuple);
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Unlink and reclaim the Insert
    EXPECT_EQ(std::make_pair(0U, 1U), gc->PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(1U, 0U), gc->PerformGarbageCollection());

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager->BeginTransaction();

    EXPECT_TRUE(tested.table_.Update(common::ManagedPointer(txn0), slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    // Nothing should be able to be GC'd yet because txn0 has not committed yet
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());

    auto *txn1 = txn_manager->BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Nothing should be able to be GC'd yet because txn1 started before txn0's commit
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Unlink the update and read-only txns, then deallocate the update txn
    EXPECT_EQ(std::make_pair(0U, 2U), gc->PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue
    EXPECT_EQ(std::make_pair(1U, 0U), gc->PerformGarbageCollection());

    auto *txn2 = txn_manager->BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Unlink the read-only transaction
    EXPECT_EQ(std::make_pair(0U, 1U), gc->PerformGarbageCollection());
    // It shouldn't make it to the second invocation because it's read-only
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());
  }
}

// Corresponds to (MVCCTests, CommitUpdate2)
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, CommitUpdate2) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    auto db_main = DBMain::Builder().SetUseGC(true).Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    auto gc = db_main->GetStorageLayer()->GetGarbageCollector();

    GarbageCollectorDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_,
                                               &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager->BeginTransaction();
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn), *insert_tuple);
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Unlink and reclaim the Insert
    EXPECT_EQ(std::make_pair(0U, 1U), gc->PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(1U, 0U), gc->PerformGarbageCollection());

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager->BeginTransaction();

    auto *txn1 = txn_manager->BeginTransaction();

    EXPECT_TRUE(tested.table_.Update(common::ManagedPointer(txn1), slot, *update));

    // Nothing should be able to be GC'd yet because txn1 has not committed yet
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Nothing should be able to be GC'd yet because txn0 started before txn1's commit
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());

    select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Unlink the update and read-only txns, then deallocate the update txn
    EXPECT_EQ(std::make_pair(0U, 2U), gc->PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue
    EXPECT_EQ(std::make_pair(1U, 0U), gc->PerformGarbageCollection());

    auto *txn2 = txn_manager->BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Unlink the read-only transaction
    EXPECT_EQ(std::make_pair(0U, 1U), gc->PerformGarbageCollection());
    // It shouldn't make it to the second invocation because it's read-only
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());
  }
}

// Corresponds to (MVCCTests, AbortUpdate1)
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, AbortUpdate1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    auto db_main = DBMain::Builder().SetUseGC(true).Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    auto gc = db_main->GetStorageLayer()->GetGarbageCollector();

    GarbageCollectorDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_,
                                               &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager->BeginTransaction();
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn), *insert_tuple);
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Unlink and reclaim the Insert
    EXPECT_EQ(std::make_pair(0U, 1U), gc->PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(1U, 0U), gc->PerformGarbageCollection());

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager->BeginTransaction();

    EXPECT_TRUE(tested.table_.Update(common::ManagedPointer(txn0), slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    auto *txn1 = txn_manager->BeginTransaction();

    // Nothing should be able to be GC'd yet because txn0 has not committed yet
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Abort(txn0);

    // Aborted transactions cannot be removed from the unlink queue immediately
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

    // process the read-only transaction and aborted transcation
    EXPECT_EQ(std::make_pair(0U, 2U), gc->PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue, aborted transaction is deallocated
    EXPECT_EQ(std::make_pair(1U, 0U), gc->PerformGarbageCollection());

    auto *txn2 = txn_manager->BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Unlink the read-only transaction
    EXPECT_EQ(std::make_pair(0U, 1U), gc->PerformGarbageCollection());
    // It shouldn't make it to the second invocation because it's read-only
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());
  }
}

// Corresponds to (MVCCTests, AbortUpdate2)
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, AbortUpdate2) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    auto db_main = DBMain::Builder().SetUseGC(true).Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    auto gc = db_main->GetStorageLayer()->GetGarbageCollector();

    GarbageCollectorDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_,
                                               &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager->BeginTransaction();
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn), *insert_tuple);
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Unlink and reclaim the Insert
    EXPECT_EQ(std::make_pair(0U, 1U), gc->PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(1U, 0U), gc->PerformGarbageCollection());

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager->BeginTransaction();

    auto *txn1 = txn_manager->BeginTransaction();

    EXPECT_TRUE(tested.table_.Update(common::ManagedPointer(txn1), slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    // Nothing should be able to be GC'd yet because txn1 has not committed yet
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());

    txn_manager->Abort(txn1);

    select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    // Aborted transactions cannot be removed from the unlink queue immediately
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());

    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    // process the read-only transaction and aborted transcation
    EXPECT_EQ(std::make_pair(0U, 2U), gc->PerformGarbageCollection());
    // Read-only transaction shouldn't have made it to the deallocate queue, aborted transaction is deallocated
    EXPECT_EQ(std::make_pair(1U, 0U), gc->PerformGarbageCollection());

    auto *txn2 = txn_manager->BeginTransaction();

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Unlink the read-only transaction
    EXPECT_EQ(std::make_pair(0U, 1U), gc->PerformGarbageCollection());
    // It shouldn't make it to the second invocation because it's read-only
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());
  }
}

// Corresponds to (MVCCTests, InsertUpdate1)
// NOLINTNEXTLINE
TEST_F(GarbageCollectorTests, InsertUpdate1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    auto db_main = DBMain::Builder().SetUseGC(true).Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    auto gc = db_main->GetStorageLayer()->GetGarbageCollector();

    GarbageCollectorDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_,
                                               &generator_);

    auto *txn0 = txn_manager->BeginTransaction();

    auto *txn1 = txn_manager->BeginTransaction();

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn1), *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    // Nothing should be able to be GC'd yet because txn1 has not committed yet
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());

    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Nothing should be able to be GC'd yet because txn0 started before txn1's commit
    EXPECT_EQ(std::make_pair(0U, 0U), gc->PerformGarbageCollection());

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);
    EXPECT_FALSE(tested.table_.Update(common::ManagedPointer(txn0), slot, *update));

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager->Abort(txn0);

    // Process the insert and aborted txns. Both should make it to the unlink phase
    EXPECT_EQ(std::make_pair(0U, 2U), gc->PerformGarbageCollection());
    EXPECT_EQ(std::make_pair(2U, 0U), gc->PerformGarbageCollection());
  }
}
}  // namespace noisepage
