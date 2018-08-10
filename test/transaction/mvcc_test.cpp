#include <unordered_map>
#include <utility>
#include <vector>
#include "common/object_pool.h"
#include "storage/data_table.h"
#include "storage/storage_util.h"
#include "util/storage_test_util.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace terrier {
// Not thread-safe
class MVCCDataTableTestObject {
 public:
  template<class Random>
  MVCCDataTableTestObject(storage::BlockStore *block_store,
                                 const uint16_t max_col,
                                 Random *generator)
      : layout_(StorageTestUtil::RandomLayout(max_col, generator)),
        table_(block_store, layout_) {}

  ~MVCCDataTableTestObject() {
    for (auto ptr : loose_pointers_)
      delete[] ptr;
    for (auto ptr : loose_txns_)
      delete ptr;
    delete[] select_buffer_;
  }

  const storage::BlockLayout &Layout() const { return layout_; }

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
  std::vector<byte *> loose_pointers_;
  std::vector<transaction::TransactionContext *> loose_txns_;
  std::vector<uint16_t> all_col_ids_{StorageTestUtil::ProjectionListAllColumns(layout_)};
  // These always over-provision in the case of partial selects or deltas, which is fine.
  uint32_t redo_size_ = storage::ProjectedRow::Size(layout_, all_col_ids_);
  byte *select_buffer_ = new byte[redo_size_];
};

struct MVCCTests : public ::testing::Test {
  storage::BlockStore block_store_{100};
  common::ObjectPool<transaction::UndoBufferSegment> buffer_pool_{10000};
  std::default_random_engine generator_;
};

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//    W(X)   |        |        |
//    R(X)   |        |        |
//           | BEGIN  |        |
//           | R(X)   |        |
//    COMMIT |        |        |
//           | R(X)   |        |
//           | COMMIT |        |
//           |        | BEGIN  |
//           |        | R(X)   |
//           |        | COMMIT |
//
// Txn #0 should only read Txn #0's version of X
// Txn #1 should only read the previous version of X because its start time is before #0's commit
// Txn #2 should only read Txn #0's version of X
TEST_F(MVCCTests, CommitInsert1) {
  const uint32_t num_iterations = 1000;
  const uint16_t max_columns = 100;

  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    transaction::TransactionManager txn_manager{&buffer_pool_};
    MVCCDataTableTestObject tested(&block_store_, max_columns, &generator_);

    // generate a random redo ProjectedRow to Insert
    byte *redo_buffer = new byte[tested.redo_size_];
    tested.loose_pointers_.push_back(redo_buffer);
    storage::ProjectedRow
        *redo = storage::ProjectedRow::InitializeProjectedRow(redo_buffer, tested.all_col_ids_, tested.layout_);
    StorageTestUtil::PopulateRandomRow(redo, tested.layout_, 0, &generator_);

    auto *txn0 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    storage::TupleSlot slot = tested.table_.Insert(txn0, *redo);

    storage::ProjectedRow *stored = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), stored, redo));

    auto *txn1 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    stored = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), stored, redo));

    txn_manager.Commit(txn0);

    stored = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), stored, redo));

    txn_manager.Commit(txn1);

    auto *txn2 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    stored = tested.SelectIntoBuffer(txn2, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), stored, redo));
    txn_manager.Commit(txn2);
  }
}

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//           | BEGIN  |        |
//           | W(X)   |        |
//    R(X)   |        |        |
//           | R(X)   |        |
//           | COMMIT |        |
//    R(X)   |        |        |
//    COMMIT |        |        |
//           |        | BEGIN  |
//           |        | R(X)   |
//           |        | COMMIT |
//
// Txn #0 should only read the previous version of X because its start time is before #1's commit
// Txn #1 should only read Txn #1's version of X
// Txn #2 should only read Txn #1's version of X
TEST_F(MVCCTests, CommitInsert2) {
  const uint32_t num_iterations = 1000;
  const uint16_t max_columns = 100;

  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    transaction::TransactionManager txn_manager{&buffer_pool_};
    MVCCDataTableTestObject tested(&block_store_, max_columns, &generator_);

    // generate a random redo ProjectedRow to Insert
    byte *redo_buffer = new byte[tested.redo_size_];
    tested.loose_pointers_.push_back(redo_buffer);
    storage::ProjectedRow
        *redo = storage::ProjectedRow::InitializeProjectedRow(redo_buffer, tested.all_col_ids_, tested.layout_);
    StorageTestUtil::PopulateRandomRow(redo, tested.layout_, 0, &generator_);

    auto *txn0 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    auto *txn1 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    storage::TupleSlot slot = tested.table_.Insert(txn1, *redo);

    storage::ProjectedRow *stored = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), stored, redo));

    stored = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), stored, redo));

    txn_manager.Commit(txn1);

    stored = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), stored, redo));

    txn_manager.Commit(txn0);

    auto *txn2 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    stored = tested.SelectIntoBuffer(txn2, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), stored, redo));
    txn_manager.Commit(txn2);
  }
}

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//    W(X)   |        |        |
//    R(X)   |        |        |
//           | BEGIN  |        |
//           | R(X)   |        |
//    ABORT  |        |        |
//           | R(X)   |        |
//           | COMMIT |        |
//           |        | BEGIN  |
//           |        | R(X)   |
//           |        | COMMIT |
//
// Txn #0 should only read Txn #0's version of X
// Txn #1 should only read the previous version of X because Txn #0's is uncommitted
// Txn #2 should only read the previous version of X because Txn #0 aborted
TEST_F(MVCCTests, AbortInsert1) {
  const uint32_t num_iterations = 1000;
  const uint16_t max_columns = 100;

  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    transaction::TransactionManager txn_manager{&buffer_pool_};
    MVCCDataTableTestObject tested(&block_store_, max_columns, &generator_);

    // generate a random redo ProjectedRow to Insert
    byte *redo_buffer = new byte[tested.redo_size_];
    tested.loose_pointers_.push_back(redo_buffer);
    storage::ProjectedRow
        *redo = storage::ProjectedRow::InitializeProjectedRow(redo_buffer, tested.all_col_ids_, tested.layout_);
    StorageTestUtil::PopulateRandomRow(redo, tested.layout_, 0, &generator_);

    auto *txn0 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    storage::TupleSlot slot = tested.table_.Insert(txn0, *redo);

    storage::ProjectedRow *stored = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), stored, redo));

    auto *txn1 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    stored = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), stored, redo));

    txn_manager.Abort(txn0);

    stored = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), stored, redo));

    txn_manager.Commit(txn1);

    auto *txn2 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    stored = tested.SelectIntoBuffer(txn2, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), stored, redo));
    txn_manager.Commit(txn2);
  }
}

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//           | BEGIN  |        |
//           | W(X)   |        |
//    R(X)   |        |        |
//           | R(X)   |        |
//           | ABORT  |        |
//    R(X)   |        |        |
//    COMMIT |        |        |
//           |        | BEGIN  |
//           |        | R(X)   |
//           |        | COMMIT |
//
// Txn #0 should only read the previous version of X because Txn #1's is uncommitted
// Txn #1 should only read Txn #1's version of X
// Txn #2 should only read the previous version of X because Txn #1 aborted
TEST_F(MVCCTests, AbortInsert2) {
  const uint32_t num_iterations = 1000;
  const uint16_t max_columns = 100;

  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    transaction::TransactionManager txn_manager{&buffer_pool_};
    MVCCDataTableTestObject tested(&block_store_, max_columns, &generator_);

    // generate a random redo ProjectedRow to Insert
    byte *redo_buffer = new byte[tested.redo_size_];
    tested.loose_pointers_.push_back(redo_buffer);
    storage::ProjectedRow
        *redo = storage::ProjectedRow::InitializeProjectedRow(redo_buffer, tested.all_col_ids_, tested.layout_);
    StorageTestUtil::PopulateRandomRow(redo, tested.layout_, 0, &generator_);

    auto *txn0 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    auto *txn1 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    storage::TupleSlot slot = tested.table_.Insert(txn1, *redo);

    storage::ProjectedRow *stored = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), stored, redo));

    stored = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), stored, redo));

    txn_manager.Abort(txn1);

    stored = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), stored, redo));

    txn_manager.Commit(txn0);

    auto *txn2 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    stored = tested.SelectIntoBuffer(txn2, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), stored, redo));
    txn_manager.Commit(txn2);
  }
}

}  // namespace terrier
