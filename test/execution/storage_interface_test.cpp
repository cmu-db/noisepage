#include "execution/sql/storage_interface.h"

#include <array>
#include <memory>
#include <vector>

#include "catalog/catalog_defs.h"
#include "execution/sql/index_iterator.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/sql_test.h"
#include "execution/util/timer.h"

namespace noisepage::execution::sql::test {

class StorageInterfaceTest : public SqlBasedTest {
  void SetUp() override {
    // Create the test tables
    SqlBasedTest::SetUp();
    exec_ctx_ = MakeExecCtx();
    GenerateTestTables(exec_ctx_.get());
  }

 protected:
  /**
   * Execution context to use for the test
   */
  std::unique_ptr<exec::ExecutionContext> exec_ctx_;
};

// NOLINTNEXTLINE
TEST_F(StorageInterfaceTest, SimpleInsertTest) {
  // INSERT INTO empty_table SELECT colA FROM test_1 WHERE colA BETWEEN 495 and 505.
  auto table_oid0 = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "empty_table");
  auto index_oid0 = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_empty");
  auto table_oid1 = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  auto index_oid1 = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");
  // Select colA only
  std::array<uint32_t, 1> col_oids{1};

  // The index iterator gives us the slots to update.
  IndexIterator index_iter1{exec_ctx_.get(),
                            1,
                            table_oid1.UnderlyingValue(),
                            index_oid1.UnderlyingValue(),
                            col_oids.data(),
                            static_cast<uint32_t>(col_oids.size())};
  index_iter1.Init();

  // Inserter.
  StorageInterface inserter(exec_ctx_.get(), table_oid0, col_oids.data(), col_oids.size(), true);

  // Find the rows with colA BETWEEN 495 AND 505.
  int32_t lo_match = 495;
  int32_t hi_match = 505;
  auto *const lo_pr(index_iter1.LoPR());
  auto *const hi_pr(index_iter1.HiPR());
  lo_pr->Set<int32_t, false>(0, lo_match, false);
  hi_pr->Set<int32_t, false>(0, hi_match, false);
  index_iter1.ScanAscending(storage::index::ScanType::Closed, 0);
  std::vector<uint32_t> inserted_vals;
  int nt = 0;
  while (index_iter1.Advance()) {
    // Get tuple at the current slot
    auto *const table_pr(index_iter1.TablePR());
    auto *val_a = table_pr->Get<int32_t, false>(0, nullptr);
    inserted_vals.emplace_back(*val_a);
    // Insert into table
    auto *const insert_pr(inserter.GetTablePR());
    insert_pr->Set<int32_t, false>(0, *val_a, false);
    inserter.TableInsert();
    // Insert into index
    auto *const index_pr(inserter.GetIndexPR(index_oid0));
    index_pr->Set<int32_t, false>(0, *val_a, false);
    ASSERT_TRUE(inserter.IndexInsert());
    nt++;
  }

  // Try to fetch the inserted values.
  TableVectorIterator table_iter(exec_ctx_.get(), table_oid0.UnderlyingValue(), col_oids.data(),
                                 static_cast<uint32_t>(col_oids.size()));
  table_iter.Init();
  VectorProjectionIterator *vpi = table_iter.GetVectorProjectionIterator();
  uint32_t num_tuples = 0;
  while (table_iter.Advance()) {
    for (; vpi->HasNext(); vpi->Advance()) {
      auto *val_a = vpi->GetValue<int32_t, false>(0, nullptr);
      ASSERT_EQ(*val_a, inserted_vals[num_tuples]);
      num_tuples++;
    }
    vpi->Reset();
  }
  EXPECT_EQ(num_tuples, (hi_match - lo_match) + 1);
}

// NOLINTNEXTLINE
TEST_F(StorageInterfaceTest, SimpleDeleteTest) {
  // DELETE FROM test_1 where colA BETWEEN 495 and 505.
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");
  // Select colA only
  std::array<uint32_t, 1> col_oids{1};

  // The index iterator gives us the slots to update.
  IndexIterator index_iter{exec_ctx_.get(),
                           1,
                           table_oid.UnderlyingValue(),
                           index_oid.UnderlyingValue(),
                           col_oids.data(),
                           static_cast<uint32_t>(col_oids.size())};
  index_iter.Init();

  // Deleter.
  StorageInterface deleter(exec_ctx_.get(), table_oid, col_oids.data(), col_oids.size(), true);

  // Find the rows with colA BETWEEN 495 AND 505.
  int32_t lo_match = 495;
  int32_t hi_match = 505;
  auto *const lo_pr(index_iter.LoPR());
  auto *const hi_pr(index_iter.HiPR());
  lo_pr->Set<int32_t, false>(0, lo_match, false);
  hi_pr->Set<int32_t, false>(0, hi_match, false);
  index_iter.ScanAscending(storage::index::ScanType::Closed, 0);
  while (index_iter.Advance()) {
    // Get tuple at the current slot
    auto *const table_pr(index_iter.TablePR());
    storage::TupleSlot slot(index_iter.CurrentSlot());
    auto *val_a = table_pr->Get<int32_t, false>(0, nullptr);
    // Delete from Table
    deleter.TableDelete(slot);
    // Delete from Index
    auto *const index_pr(deleter.GetIndexPR(index_oid));
    index_pr->Set<int32_t, false>(0, *val_a, false);
    deleter.IndexDelete(slot);
  }

  // Now try reading the deleted rows from the index. They should not be found
  lo_pr->Set<int32_t, false>(0, lo_match, false);
  hi_pr->Set<int32_t, false>(0, hi_match, false);
  index_iter.ScanAscending(storage::index::ScanType::Closed, 0);
  ASSERT_FALSE(index_iter.Advance());

  // Try scanning through the table. There should be less elements.
  TableVectorIterator table_iter(exec_ctx_.get(), table_oid.UnderlyingValue(), col_oids.data(),
                                 static_cast<uint32_t>(col_oids.size()));
  table_iter.Init();
  VectorProjectionIterator *vpi = table_iter.GetVectorProjectionIterator();
  uint32_t num_tuples = 0;
  while (table_iter.Advance()) {
    for (; vpi->HasNext(); vpi->Advance()) {
      num_tuples++;
    }
    vpi->Reset();
  }
  EXPECT_EQ(num_tuples, TEST1_SIZE - ((hi_match - lo_match) + 1));
}

// NOLINTNEXTLINE
TEST_F(StorageInterfaceTest, SimpleNonIndexedUpdateTest) {
  // Add 10000 to colB where colA BETWEEN 495 and 505.
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");
  // Select colB only
  std::array<uint32_t, 1> col_oids{2};

  // The index iterator gives us the slots to update.
  IndexIterator index_iter{exec_ctx_.get(),
                           1,
                           table_oid.UnderlyingValue(),
                           index_oid.UnderlyingValue(),
                           col_oids.data(),
                           static_cast<uint32_t>(col_oids.size())};
  index_iter.Init();

  // Non indexed updater.
  StorageInterface updater(exec_ctx_.get(), table_oid, col_oids.data(), col_oids.size(), false);

  // Find the rows with colA BETWEEN 495 AND 505.
  int32_t lo_match = 495;
  int32_t hi_match = 505;
  auto *const lo_pr(index_iter.LoPR());
  auto *const hi_pr(index_iter.HiPR());
  lo_pr->Set<int32_t, false>(0, lo_match, false);
  hi_pr->Set<int32_t, false>(0, hi_match, false);
  index_iter.ScanAscending(storage::index::ScanType::Closed, 0);
  std::vector<uint32_t> old_vals;
  while (index_iter.Advance()) {
    // Get tuple at the current slot
    auto *const table_pr(index_iter.TablePR());
    storage::TupleSlot slot(index_iter.CurrentSlot());
    auto *curr_val = table_pr->Get<int32_t, false>(0, nullptr);
    old_vals.emplace_back(*curr_val);
    // Update Table
    auto *const update_pr(updater.GetTablePR());
    update_pr->Set<int32_t, false>(0, *curr_val + TEST1_SIZE, false);
    ASSERT_TRUE(updater.TableUpdate(slot));
  }

  // Now try reading the updated rows.
  // The updated values should be found.
  lo_pr->Set<int32_t, false>(0, lo_match, false);
  hi_pr->Set<int32_t, false>(0, hi_match, false);
  index_iter.ScanAscending(storage::index::ScanType::Closed, 0);
  uint32_t num_matches = 0;
  while (index_iter.Advance()) {
    auto *const table_pr(index_iter.TablePR());
    auto *val = table_pr->Get<int32_t, false>(0, nullptr);
    EXPECT_EQ(*val, old_vals[num_matches] + TEST1_SIZE);
    num_matches++;
  }
  ASSERT_EQ(num_matches, (hi_match - lo_match) + 1);
}

// NOLINTNEXTLINE
TEST_F(StorageInterfaceTest, SimpleIndexedUpdateTest) {
  // Add 10000 to colA where colA BETWEEN 495 and 505.
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");
  // Select all columns for insert
  std::array<uint32_t, 4> col_oids{1, 2, 3, 4};

  // The index iterator gives us the slots to update.
  IndexIterator index_iter{exec_ctx_.get(),
                           1,
                           table_oid.UnderlyingValue(),
                           index_oid.UnderlyingValue(),
                           col_oids.data(),
                           static_cast<uint32_t>(col_oids.size())};
  index_iter.Init();

  // Indexed updater.
  StorageInterface updater(exec_ctx_.get(), table_oid, col_oids.data(), col_oids.size(), true);

  // Find the rows with colA BETWEEN 495 AND 505.
  int32_t lo_match = 495;
  int32_t hi_match = 505;
  auto *const lo_pr(index_iter.LoPR());
  auto *const hi_pr(index_iter.HiPR());
  lo_pr->Set<int32_t, false>(0, lo_match, false);
  hi_pr->Set<int32_t, false>(0, hi_match, false);
  index_iter.ScanAscending(storage::index::ScanType::Closed, 0);
  std::vector<uint32_t> old_vals;
  while (index_iter.Advance()) {
    // Get tuple at the current slot
    auto *const table_pr(index_iter.TablePR());
    storage::TupleSlot slot(index_iter.CurrentSlot());
    auto *curr_val = table_pr->Get<int32_t, false>(0, nullptr);
    auto *val_b = table_pr->Get<int32_t, false>(1, nullptr);
    auto *val_c = table_pr->Get<int32_t, false>(2, nullptr);
    auto *val_d = table_pr->Get<int32_t, false>(3, nullptr);
    old_vals.emplace_back(*curr_val);
    // Delete + Insert in Table
    ASSERT_TRUE(updater.TableDelete(slot));
    auto *const update_pr(updater.GetTablePR());
    update_pr->Set<int32_t, false>(0, *curr_val + TEST1_SIZE, false);
    update_pr->Set<int32_t, false>(1, *val_b, false);
    update_pr->Set<int32_t, false>(2, *val_c, false);
    update_pr->Set<int32_t, false>(3, *val_d, false);
    auto new_slot UNUSED_ATTRIBUTE = updater.TableInsert();

    // Delete + Insert in Index
    auto *const index_pr(updater.GetIndexPR(index_oid));
    index_pr->Set<int32_t, false>(0, *curr_val, false);
    updater.IndexDelete(slot);
    index_pr->Set<int32_t, false>(0, *curr_val + TEST1_SIZE, false);
    updater.IndexInsert();
  }

  // Now try reading the updated rows.
  // The updated values should be found.
  lo_pr->Set<int32_t, false>(0, lo_match + TEST1_SIZE, false);
  hi_pr->Set<int32_t, false>(0, hi_match + TEST1_SIZE, false);
  index_iter.ScanAscending(storage::index::ScanType::Closed, 0);
  uint32_t num_matches = 0;
  while (index_iter.Advance()) {
    auto *const table_pr(index_iter.TablePR());
    auto *val = table_pr->Get<int32_t, false>(0, nullptr);
    EXPECT_EQ(*val, old_vals[num_matches] + TEST1_SIZE);
    num_matches++;
  }
  ASSERT_EQ(num_matches, (hi_match - lo_match) + 1);
}

// NOLINTNEXTLINE
TEST_F(StorageInterfaceTest, MultiIndexedUpdateTest) {
  // Set colA += 10000 and colB = 0 where colA BETWEEN 495 and 505.
  // Here there are two indexes to update.
  // One of the indexes contains a nullable value.
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_2");
  auto index_oid1 = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_2");
  auto index_oid2 = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_2_multi");
  // Select all columns for insert
  std::array<uint32_t, 4> col_oids{1, 2, 3, 4};
  uint16_t idx_a = 3;
  uint16_t idx_b = 1;
  uint16_t idx_c = 0;
  uint16_t idx_d = 2;

  // The index iterator gives us the slots to update.
  IndexIterator index_iter1{exec_ctx_.get(),
                            1,
                            table_oid.UnderlyingValue(),
                            index_oid1.UnderlyingValue(),
                            col_oids.data(),
                            static_cast<uint32_t>(col_oids.size())};

  IndexIterator index_iter2{exec_ctx_.get(),
                            1,
                            table_oid.UnderlyingValue(),
                            index_oid2.UnderlyingValue(),
                            col_oids.data(),
                            static_cast<uint32_t>(col_oids.size())};
  index_iter1.Init();
  index_iter2.Init();

  // Indexed updater.
  StorageInterface updater(exec_ctx_.get(), table_oid, col_oids.data(), col_oids.size(), true);

  // Find the rows with colA BETWEEN 495 AND 505.
  int16_t lo_match = 495;
  int16_t hi_match = 505;
  int16_t update_val = static_cast<uint16_t>(TEST2_SIZE);
  auto *lo_pr(index_iter1.LoPR());
  auto *hi_pr(index_iter1.HiPR());
  lo_pr->Set<int16_t, false>(0, lo_match, false);
  hi_pr->Set<int16_t, false>(0, hi_match, false);
  index_iter1.ScanAscending(storage::index::ScanType::Closed, 0);
  std::vector<uint16_t> old_vals;
  uint32_t num_updates = 0;
  while (index_iter1.Advance()) {
    // Get tuple at the current slot
    auto *const table_pr(index_iter1.TablePR());
    storage::TupleSlot slot(index_iter1.CurrentSlot());
    // Read table values
    auto *curr_val_a = table_pr->Get<int16_t, false>(idx_a, nullptr);
    bool null_b;
    auto *curr_val_b = table_pr->Get<int32_t, true>(idx_b, &null_b);
    auto val_c = table_pr->Get<int64_t, false>(idx_c, nullptr);
    bool null_d;
    auto val_d = table_pr->Get<int32_t, true>(idx_d, &null_d);

    old_vals.emplace_back(*curr_val_a);
    // Delete + Insert in Table
    ASSERT_TRUE(updater.TableDelete(slot));
    auto *const update_pr(updater.GetTablePR());
    update_pr->Set<int16_t, false>(idx_a, *curr_val_a + update_val, false);
    update_pr->Set<int32_t, true>(idx_b, 0, false);
    update_pr->Set<int64_t, false>(idx_c, *val_c, false);
    update_pr->Set<int32_t, true>(idx_d, null_d ? 0 : *val_d, null_d);
    auto new_slot UNUSED_ATTRIBUTE = updater.TableInsert();

    // Delete + Insert in Indexes
    // First index
    {
      auto *const index_pr(updater.GetIndexPR(index_oid1));
      index_pr->Set<int16_t, false>(0, *curr_val_a, false);
      updater.IndexDelete(slot);
      index_pr->Set<int16_t, false>(0, *curr_val_a + update_val, false);
      updater.IndexInsert();
    }
    // Second index
    {
      auto *const index_pr(updater.GetIndexPR(index_oid2));
      index_pr->Set<int16_t, false>(1, *curr_val_a, false);
      index_pr->Set<int32_t, true>(0, null_b ? 0 : *curr_val_b, null_b);
      updater.IndexDelete(slot);
      index_pr->Set<int16_t, false>(1, *curr_val_a + update_val, false);
      index_pr->Set<int32_t, true>(0, 0, false);
      updater.IndexInsert();
    }
    num_updates++;
  }

  // Now try reading the updated rows.
  // The updated values should be found in both index

  // Check first index
  {
    lo_pr->Set<int16_t, false>(0, lo_match + update_val, false);
    hi_pr->Set<int16_t, false>(0, hi_match + update_val, false);
    index_iter1.ScanAscending(storage::index::ScanType::Closed, 0);
    uint32_t num_matches = 0;
    while (index_iter1.Advance()) {
      auto *const table_pr(index_iter1.TablePR());
      auto *val_a = table_pr->Get<int16_t, false>(idx_a, nullptr);
      bool null_b = true;
      auto *val_b = table_pr->Get<int32_t, true>(idx_b, &null_b);
      EXPECT_EQ(*val_a, old_vals[num_matches] + update_val);
      ASSERT_FALSE(null_b);
      EXPECT_EQ(*val_b, 0);
      num_matches++;
    }
    ASSERT_EQ(num_matches, (hi_match - lo_match) + 1);
  }

  // Check second index
  {
    lo_pr = index_iter2.LoPR();
    hi_pr = index_iter2.HiPR();
    lo_pr->Set<int16_t, false>(1, lo_match + update_val, false);
    lo_pr->Set<int32_t, false>(0, 0, false);
    hi_pr->Set<int16_t, false>(1, hi_match + update_val, false);
    hi_pr->Set<int32_t, false>(0, 0, false);
    index_iter2.ScanAscending(storage::index::ScanType::Closed, 0);
    uint32_t num_matches = 0;
    while (index_iter2.Advance()) {
      auto *const table_pr(index_iter2.TablePR());
      auto *val_a = table_pr->Get<int16_t, false>(idx_a, nullptr);
      bool null_b = true;
      auto *val_b = table_pr->Get<int32_t, true>(idx_b, &null_b);
      EXPECT_EQ(*val_a, old_vals[num_matches] + update_val);
      ASSERT_FALSE(null_b);
      EXPECT_EQ(*val_b, 0);
      num_matches++;
    }
    ASSERT_EQ(num_matches, (hi_match - lo_match) + 1);
  }
}
}  // namespace noisepage::execution::sql::test
