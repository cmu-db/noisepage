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
class MVCCDataTableTestObject {
 public:
  template <class Random>
  MVCCDataTableTestObject(storage::BlockStore *block_store, const uint16_t max_col, Random *generator)
      : layout_(StorageTestUtil::RandomLayoutNoVarlen(max_col, generator)),
        table_(common::ManagedPointer(block_store), layout_, storage::layout_version_t(0)) {}

  ~MVCCDataTableTestObject() {
    for (auto ptr : loose_pointers_) delete[] ptr;
    for (auto ptr : loose_txns_) delete ptr;
    delete[] select_buffer_;
  }

  const storage::BlockLayout &Layout() const { return layout_; }

  template <class Random>
  storage::ProjectedRow *GenerateRandomTuple(Random *generator) {
    auto *buffer = common::AllocationUtil::AllocateAligned(redo_initializer_.ProjectedRowSize());
    loose_pointers_.push_back(buffer);
    storage::ProjectedRow *redo = redo_initializer_.InitializeRow(buffer);
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
    auto *buffer = common::AllocationUtil::AllocateAligned(redo_initializer_.ProjectedRowSize());
    loose_pointers_.push_back(buffer);
    // Copy previous version
    std::memcpy(buffer, &previous, redo_initializer_.ProjectedRowSize());
    auto *version = reinterpret_cast<storage::ProjectedRow *>(buffer);
    std::unordered_map<uint16_t, uint16_t> col_to_projection_list_index;
    storage::StorageUtil::ApplyDelta(layout_, delta, version);
    return version;
  }

  storage::ProjectedRow *SelectIntoBuffer(transaction::TransactionContext *const txn, const storage::TupleSlot slot) {
    // generate a redo ProjectedRow for Select
    storage::ProjectedRow *select_row = redo_initializer_.InitializeRow(select_buffer_);
    select_result_ = table_.Select(common::ManagedPointer(txn), slot, select_row);
    return select_row;
  }

  storage::BlockLayout layout_;
  storage::DataTable table_;
  // We want null_bias_ to be zero when testing CC. We already evaluate null correctness in other directed tests, and
  // we don't want the logically deleted field to end up set NULL.
  const double null_bias_ = 0;
  std::vector<byte *> loose_pointers_;
  std::vector<transaction::TransactionContext *> loose_txns_;
  storage::ProjectedRowInitializer redo_initializer_ =
      storage::ProjectedRowInitializer::Create(layout_, StorageTestUtil::ProjectionListAllColumns(layout_));
  byte *select_buffer_ = common::AllocationUtil::AllocateAligned(redo_initializer_.ProjectedRowSize());
  bool select_result_;
};

class MVCCTests : public ::noisepage::TerrierTest {
 public:
  storage::BlockStore block_store_{100, 100};
  storage::RecordBufferSegmentPool buffer_pool_{10000, 10000};
  std::default_random_engine generator_;
  const uint32_t num_iterations_ = 100;
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
    auto db_main = DBMain::Builder().Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    MVCCDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_, &generator_);

    auto *txn0 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn0), *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    auto *txn1 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    tested.SelectIntoBuffer(txn1, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    tested.SelectIntoBuffer(txn1, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn2 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
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
    auto db_main = DBMain::Builder().Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    MVCCDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_, &generator_);

    auto *txn0 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    auto *txn1 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn1), *insert_tuple);

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

    tested.SelectIntoBuffer(txn0, slot);

    EXPECT_FALSE(tested.select_result_);

    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn2 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
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
    auto db_main = DBMain::Builder().Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    MVCCDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_, &generator_);

    auto *txn0 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn0), *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    auto *txn1 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    tested.SelectIntoBuffer(txn1, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager->Abort(txn0);

    tested.SelectIntoBuffer(txn1, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn2 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    tested.SelectIntoBuffer(txn2, slot);
    EXPECT_FALSE(tested.select_result_);
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
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
    auto db_main = DBMain::Builder().Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    MVCCDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_, &generator_);

    auto *txn0 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    auto *txn1 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn1), *insert_tuple);

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Abort(txn1);

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn2 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    tested.SelectIntoBuffer(txn2, slot);
    EXPECT_FALSE(tested.select_result_);
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
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
    auto db_main = DBMain::Builder().Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    MVCCDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_, &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn), *insert_tuple);
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    EXPECT_TRUE(tested.table_.Update(common::ManagedPointer(txn0), slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    auto *txn1 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn2 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
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
    auto db_main = DBMain::Builder().Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    MVCCDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_, &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn), *insert_tuple);
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    auto *txn1 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    EXPECT_TRUE(tested.table_.Update(common::ManagedPointer(txn1), slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

    select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn2 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
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
    auto db_main = DBMain::Builder().Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    MVCCDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_, &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn), *insert_tuple);
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    EXPECT_TRUE(tested.table_.Update(common::ManagedPointer(txn0), slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    auto *txn1 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Abort(txn0);

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn2 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
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
    auto db_main = DBMain::Builder().Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    MVCCDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_, &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn), *insert_tuple);
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    auto *txn1 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    EXPECT_TRUE(tested.table_.Update(common::ManagedPointer(txn1), slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    txn_manager->Abort(txn1);

    select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn2 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
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
    auto db_main = DBMain::Builder().Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    MVCCDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_, &generator_);

    auto *txn0 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    auto *txn1 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn1), *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));
    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);
    EXPECT_FALSE(tested.table_.Update(common::ManagedPointer(txn0), slot, *update));

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager->Abort(txn0);
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
TEST_F(MVCCTests, CommitDelete1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    auto db_main = DBMain::Builder().Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    MVCCDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_, &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Deleted later
    auto *txn = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn), *insert_tuple);
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn0 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    EXPECT_TRUE(tested.table_.Delete(common::ManagedPointer(txn0), slot));

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    auto *txn1 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn2 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    tested.SelectIntoBuffer(txn2, slot);
    EXPECT_FALSE(tested.select_result_);
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
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
TEST_F(MVCCTests, CommitDelete2) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    auto db_main = DBMain::Builder().Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    MVCCDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_, &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Deleted later
    auto *txn = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn), *insert_tuple);
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn0 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    auto *txn1 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    EXPECT_TRUE(tested.table_.Delete(common::ManagedPointer(txn1), slot));

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    tested.SelectIntoBuffer(txn1, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

    select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn2 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    tested.SelectIntoBuffer(txn2, slot);
    EXPECT_FALSE(tested.select_result_);
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
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
TEST_F(MVCCTests, AbortDelete1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    auto db_main = DBMain::Builder().Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    MVCCDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_, &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn), *insert_tuple);
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn0 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    EXPECT_TRUE(tested.table_.Delete(common::ManagedPointer(txn0), slot));

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    auto *txn1 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Abort(txn0);

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn2 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
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
TEST_F(MVCCTests, AbortDelete2) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    auto db_main = DBMain::Builder().Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    MVCCDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_, &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn), *insert_tuple);
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn0 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    auto *txn1 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    EXPECT_TRUE(tested.table_.Delete(common::ManagedPointer(txn1), slot));

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    tested.SelectIntoBuffer(txn1, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager->Abort(txn1);

    select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn2 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
  }
}

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//    W(X)   |        |        |
//    R(X)   |        |        |
//           | BEGIN  |        |
//    W(X)   |        |        |
//    R(X)   |        |        |
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
TEST_F(MVCCTests, CommitUpdateDelete1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    auto db_main = DBMain::Builder().Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    MVCCDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_, &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn), *insert_tuple);
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    EXPECT_TRUE(tested.table_.Update(common::ManagedPointer(txn0), slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    auto *txn1 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    EXPECT_TRUE(tested.table_.Delete(common::ManagedPointer(txn0), slot));

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn2 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    tested.SelectIntoBuffer(txn2, slot);
    EXPECT_FALSE(tested.select_result_);
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
  }
}

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//           | BEGIN  |        |
//           | W(X)   |        |
//    R(X)   |        |        |
//           | R(X)   |        |
//           | W(X)   |        |
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
TEST_F(MVCCTests, CommitUpdateDelete2) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    auto db_main = DBMain::Builder().Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    MVCCDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_, &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn), *insert_tuple);
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    auto *txn1 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    EXPECT_TRUE(tested.table_.Update(common::ManagedPointer(txn1), slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    EXPECT_TRUE(tested.table_.Delete(common::ManagedPointer(txn1), slot));

    tested.SelectIntoBuffer(txn1, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

    select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn2 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    tested.SelectIntoBuffer(txn2, slot);
    EXPECT_FALSE(tested.select_result_);
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
  }
}

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//    W(X)   |        |        |
//    R(X)   |        |        |
//           | BEGIN  |        |
//           | R(X)   |        |
//    W(X)   |        |        |
//    R(X)   |        |        |
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
TEST_F(MVCCTests, AbortUpdateDelete1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    auto db_main = DBMain::Builder().Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    MVCCDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_, &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn), *insert_tuple);
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    EXPECT_TRUE(tested.table_.Update(common::ManagedPointer(txn0), slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    auto *txn1 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    EXPECT_TRUE(tested.table_.Delete(common::ManagedPointer(txn0), slot));

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager->Abort(txn0);

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn2 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
  }
}

//    Txn #0 | Txn #1 | Txn #2 |
//    --------------------------
//    BEGIN  |        |        |
//           | BEGIN  |        |
//           | W(X)   |        |
//    R(X)   |        |        |
//           | R(X)   |        |
//           | W(X)   |        |
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
TEST_F(MVCCTests, AbortUpdateDelete2) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    auto db_main = DBMain::Builder().Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    MVCCDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_, &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn), *insert_tuple);
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    auto *txn1 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    EXPECT_TRUE(tested.table_.Update(common::ManagedPointer(txn1), slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    EXPECT_TRUE(tested.table_.Delete(common::ManagedPointer(txn1), slot));

    tested.SelectIntoBuffer(txn1, slot);
    EXPECT_FALSE(tested.select_result_);

    txn_manager->Abort(txn1);

    select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn2 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn2);

    select_tuple = tested.SelectIntoBuffer(txn2, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));
    txn_manager->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);
  }
}

//    Txn #0 | Txn #1 |
//    -----------------
//    BEGIN  |        |
//    R(X)   |        |
//    W(X)   |        |
//    R(X)   |        |
//    W(X)   |        |
//    COMMIT |        |
//           | BEGIN  |
//           | R(X)   |
//           | COMMIT |
//
// Txn #0 should read the original version, then after delete fail to read or update the tuple
// Txn #1 should read the deleted version (fail to select)
// NOLINTNEXTLINE
TEST_F(MVCCTests, SimpleDelete1) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    auto db_main = DBMain::Builder().Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    MVCCDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_, &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn), *insert_tuple);
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    EXPECT_TRUE(tested.table_.Update(common::ManagedPointer(txn0), slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    EXPECT_TRUE(tested.table_.Delete(common::ManagedPointer(txn0), slot));

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    update = tested.GenerateRandomUpdate(&generator_);
    EXPECT_FALSE(tested.table_.Update(common::ManagedPointer(txn0), slot, *update));

    txn_manager->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

    auto *txn1 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    tested.SelectIntoBuffer(txn1, slot);
    EXPECT_FALSE(tested.select_result_);
    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);
  }
}

//    Txn #0 | Txn #1 |
//    -----------------
//    BEGIN  |        |
//    R(X)   |        |
//    W(X)   |        |
//    R(X)   |        |
//    W(X)   |        |
//    ABORT  |        |
//           | BEGIN  |
//           | R(X)   |
//           | COMMIT |
//
// Txn #0 should read the original version, then after delete fail to read or update the tuple
// Txn #1 should read the original version
// NOLINTNEXTLINE
TEST_F(MVCCTests, SimpleDelete2) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    auto db_main = DBMain::Builder().Build();
    auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
    MVCCDataTableTestObject tested(db_main->GetStorageLayer()->GetBlockStore().Get(), max_columns_, &generator_);

    auto *insert_tuple = tested.GenerateRandomTuple(&generator_);

    // insert the tuple to be Updated later
    auto *txn = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn);
    storage::TupleSlot slot = tested.table_.Insert(common::ManagedPointer(txn), *insert_tuple);
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    storage::ProjectedRow *update = tested.GenerateRandomUpdate(&generator_);

    auto *txn0 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn0);

    storage::ProjectedRow *select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));

    EXPECT_TRUE(tested.table_.Update(common::ManagedPointer(txn0), slot, *update));

    auto *update_tuple = tested.GenerateVersionFromUpdate(*update, *insert_tuple);

    select_tuple = tested.SelectIntoBuffer(txn0, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, update_tuple));

    EXPECT_TRUE(tested.table_.Delete(common::ManagedPointer(txn0), slot));

    tested.SelectIntoBuffer(txn0, slot);
    EXPECT_FALSE(tested.select_result_);

    update = tested.GenerateRandomUpdate(&generator_);
    EXPECT_FALSE(tested.table_.Update(common::ManagedPointer(txn0), slot, *update));

    txn_manager->Abort(txn0);

    auto *txn1 = txn_manager->BeginTransaction();
    tested.loose_txns_.push_back(txn1);

    select_tuple = tested.SelectIntoBuffer(txn1, slot);
    EXPECT_TRUE(tested.select_result_);
    EXPECT_TRUE(StorageTestUtil::ProjectionListEqualShallow(tested.Layout(), select_tuple, insert_tuple));
    txn_manager->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);
  }
}
}  // namespace noisepage
