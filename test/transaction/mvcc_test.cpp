#include <unordered_map>
#include <utility>
#include <vector>
#include "common/object_pool.h"
#include "storage/data_table.h"
#include "storage/storage_util.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"
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
    PELOTON_MEMCPY(buffer, &previous, redo_size_);
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
  std::vector<transaction::TransactionContext *> loose_txns_;
  std::vector<uint16_t> all_col_ids_{StorageTestUtil::ProjectionListAllColumns(layout_)};
  // These always over-provision in the case of partial selects or deltas, which is fine.
  uint32_t redo_size_ = storage::ProjectedRow::Size(layout_, all_col_ids_);
  byte *select_buffer_ = new byte[redo_size_];
};

class MVCCTests : public ::terrier::TerrierTest {
 public:
  storage::BlockStore block_store_{100};
  common::ObjectPool<transaction::UndoBufferSegment> buffer_pool_{10000};
  std::default_random_engine generator_;
  const uint32_t num_iterations_ = 1000;
  const uint16_t max_columns_ = 100;
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
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
// NOLINTNEXTLINE
TEST_F(MVCCTests, CommitInsert1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager{&buffer_pool_, false};
    MVCCDataTableTestObject tested(&block_store_, max_columns_, &generator_);

    auto *txn0 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn0, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    auto *txn1 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn0);

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn1);

    auto *txn2 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    select_tuple = tested.SelectIntoBuffer(txn2, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));
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
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
// NOLINTNEXTLINE
TEST_F(MVCCTests, CommitInsert2) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager{&buffer_pool_, false};
    MVCCDataTableTestObject tested(&block_store_, max_columns_, &generator_);

    auto *txn0 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    auto *txn1 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn1, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn1);

    select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn0);

    auto *txn2 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    select_tuple = tested.SelectIntoBuffer(txn2, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));
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
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
// NOLINTNEXTLINE
TEST_F(MVCCTests, AbortInsert1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager{&buffer_pool_, false};
    MVCCDataTableTestObject tested(&block_store_, max_columns_, &generator_);

    auto *txn0 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn0, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    auto *txn1 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Abort(txn0);

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn1);

    auto *txn2 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    select_tuple = tested.SelectIntoBuffer(txn2, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));
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
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
// NOLINTNEXTLINE
TEST_F(MVCCTests, AbortInsert2) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager{&buffer_pool_, false};
    MVCCDataTableTestObject tested(&block_store_, max_columns_, &generator_);

    auto *txn0 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    auto *txn1 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn1, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Abort(txn1);

    select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn0);

    auto *txn2 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    select_tuple = tested.SelectIntoBuffer(txn2, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));
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
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
// NOLINTNEXTLINE
TEST_F(MVCCTests, CommitUpdate1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager{&buffer_pool_, false};
    MVCCDataTableTestObject tested(&block_store_, max_columns_, &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn);
    storage::TupleSlot slot = tested.table_.Insert(txn, *insert_tuple);
    txn_manager.Commit(txn);

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    EXPECT_TRUE(tested.table_.Update(txn0, slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));

    auto *txn1 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn0);

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn1);

    auto *txn2 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    select_tuple = tested.SelectIntoBuffer(txn2, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));
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
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
// NOLINTNEXTLINE
TEST_F(MVCCTests, CommitUpdate2) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager{&buffer_pool_, false};
    MVCCDataTableTestObject tested(&block_store_, max_columns_, &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn);
    storage::TupleSlot slot = tested.table_.Insert(txn, *insert_tuple);
    txn_manager.Commit(txn);

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    auto *txn1 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    EXPECT_TRUE(tested.table_.Update(txn1, slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));

    txn_manager.Commit(txn1);

    select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn0);

    auto *txn2 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    select_tuple = tested.SelectIntoBuffer(txn2, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));
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
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
// NOLINTNEXTLINE
TEST_F(MVCCTests, AbortUpdate1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager{&buffer_pool_, false};
    MVCCDataTableTestObject tested(&block_store_, max_columns_, &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn);
    storage::TupleSlot slot = tested.table_.Insert(txn, *insert_tuple);
    txn_manager.Commit(txn);

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    EXPECT_TRUE(tested.table_.Update(txn0, slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));

    auto *txn1 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Abort(txn0);

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn1);

    auto *txn2 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    select_tuple = tested.SelectIntoBuffer(txn2, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));
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
//
// This test confirms that we are not susceptible to the DIRTY READS and UNREPEATABLE READS anomalies
// NOLINTNEXTLINE
TEST_F(MVCCTests, AbortUpdate2) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager{&buffer_pool_, false};
    MVCCDataTableTestObject tested(&block_store_, max_columns_, &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn);
    storage::TupleSlot slot = tested.table_.Insert(txn, *insert_tuple);
    txn_manager.Commit(txn);

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    auto *txn1 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    EXPECT_TRUE(tested.table_.Update(txn1, slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, update_tuple));

    txn_manager.Abort(txn1);

    select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn0);

    auto *txn2 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    select_tuple = tested.SelectIntoBuffer(txn2, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));
    txn_manager.Commit(txn2);
  }
}

//    Txn #0 | Txn #1
//    -----------------
//    BEGIN  |
//           | BEGIN
//           | W(X)
//           | R(X)
//           | COMMIT
//    W(X)   |
//    R(X)   |
//    COMMIT |
//
// Txn #0 should fail to update and only read the previous version of X because its start time is before #1's commit
// Txn #1 should only read Txn #1's version of X
//
// This test confirms that we are enforcing Snapshot Isolation. Txn #0 should never see Txn #'1 inserted tuple
// NOLINTNEXTLINE
TEST_F(MVCCTests, InsertUpdate1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    transaction::TransactionManager txn_manager{&buffer_pool_, false};
    MVCCDataTableTestObject tested(&block_store_, max_columns_, &generator_);

    auto *txn0 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    auto *txn1 = txn_manager.BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(txn1, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn1, slot, tested.all_col_ids_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));
    txn_manager.Commit(txn1);

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);
    EXPECT_FALSE(tested.table_.Update(txn0, slot, *update));

    select_tuple = tested.SelectIntoBuffer(txn0, slot, tested.all_col_ids_);
    EXPECT_FALSE(StorageTestUtil::ProjectionListEqual(tested.Layout(), select_tuple, insert_tuple));

    txn_manager.Commit(txn0);
  }
}

}  // namespace terrier
