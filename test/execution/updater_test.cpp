#include <array>
#include <memory>

#include "execution/sql_test.h"

#include "catalog/catalog_defs.h"
#include "execution/sql/inserter.h"
#include "execution/sql/updater.h"
#include "execution/util/timer.h"

#include "execution/sql/index_iterator.h"

namespace terrier::execution::sql::test {

class UpdaterTest : public SqlBasedTest {
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
TEST_F(UpdaterTest, SimpleUpdaterTest) {
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  Inserter inserter(exec_ctx_.get(), table_oid);
  uint32_t col_oid_array[] = {1, 2};
  auto is_index_key_update = false;  // The key is col oid 0, we don't modify that
  Updater updater(exec_ctx_.get(), table_oid, col_oid_array, 2, is_index_key_update);

  auto table_pr = inserter.GetTablePR();
  auto schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);

  int32_t value0 = 445;
  int32_t value1 = 721;
  int32_t value2 = 4256;
  int32_t value3 = 15;

  int32_t value1_changed = 720;
  int32_t value2_changed = 4250;

  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(0)) = value0;
  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(1)) = value1;
  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(2)) = value2;
  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(3)) = value3;

  // Insert into table, and verify that insert succeeds
  auto tuple_slot = inserter.TableInsert();
  auto table = exec_ctx_->GetAccessor()->GetTable(table_oid);
  size_t count = 0;
  for (auto iter = table->begin(); iter != table->end(); iter++) {
    count++;
  }
  EXPECT_EQ(TEST1_SIZE + 1, count);

  auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");
  auto index = exec_ctx_->GetAccessor()->GetIndex(index_oid);

  // Create Insert PR
  auto insert_pr = inserter.GetIndexPR(index_oid);
  *reinterpret_cast<int32_t *>(insert_pr->AccessForceNotNull(0)) = value0;

  // Insert into index
  std::vector<storage::TupleSlot> results_before_insertion, results_after_insertion;
  index->ScanKey(*exec_ctx_->GetTxn(), *insert_pr, &results_before_insertion);
  EXPECT_TRUE(inserter.IndexInsert(index_oid));

  // Verify that insertion succeeds
  index->ScanKey(*exec_ctx_->GetTxn(), *insert_pr, &results_after_insertion);
  EXPECT_EQ(results_before_insertion.size() + 1, results_after_insertion.size());

  // Create Update PR
  auto update_pr = updater.GetTablePR();
  *reinterpret_cast<int32_t *>(update_pr->AccessForceNotNull(0)) = value1_changed;
  *reinterpret_cast<int32_t *>(update_pr->AccessForceNotNull(1)) = value2_changed;
  updater.TableUpdate(tuple_slot);
  count = 0;
  for (auto iter = table->begin(); iter != table->end(); iter++) {
    count++;
  }
  EXPECT_EQ(TEST1_SIZE + 1, count);

  std::vector<storage::TupleSlot> results_after_update;
  index->ScanKey(*exec_ctx_->GetTxn(), *insert_pr, &results_after_update);
  EXPECT_EQ(results_after_insertion.size(), results_after_update.size());
}

// NOLINTNEXTLINE
TEST_F(UpdaterTest, MultiIndexTest) {
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_2");
  Inserter inserter(exec_ctx_.get(), table_oid);
  auto is_index_key_update = true;
  Updater updater(exec_ctx_.get(), table_oid, nullptr, 0, is_index_key_update);
  auto table_pr = inserter.GetTablePR();
  auto schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);

  int16_t value0 = 445;
  int32_t value1 = 721;
  int64_t value2 = 4256;
  int32_t value3 = 15;

  int32_t value1_changed = 720;
  int64_t value2_changed = 4250;

  *reinterpret_cast<int16_t *>(table_pr->AccessForceNotNull(0)) = value0;
  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(1)) = value1;
  *reinterpret_cast<int64_t *>(table_pr->AccessForceNotNull(2)) = value2;
  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(3)) = value3;

  auto tuple_slot = inserter.TableInsert();

  auto table = exec_ctx_->GetAccessor()->GetTable(table_oid);

  size_t count = 0;
  for (auto iter = table->begin(); iter != table->end(); iter++) {
    count++;
  }
  EXPECT_EQ(TEST2_SIZE + 1, count);

  auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_2");
  auto index_pr = inserter.GetIndexPR(index_oid);
  auto index = exec_ctx_->GetAccessor()->GetIndex(index_oid);
  *reinterpret_cast<int16_t *>(index_pr->AccessForceNotNull(0)) = value0;
  std::vector<storage::TupleSlot> results1;
  index->ScanKey(*exec_ctx_->GetTxn(), *index_pr, &results1);
  EXPECT_TRUE(inserter.IndexInsert(index_oid));
  {
    std::vector<storage::TupleSlot> results2;
    index->ScanKey(*exec_ctx_->GetTxn(), *index_pr, &results2);
    EXPECT_EQ(results1.size() + 1, results2.size());
  }

  auto index_oid2 = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_2_multi");
  auto index_pr2 = inserter.GetIndexPR(index_oid2);
  auto index2 = exec_ctx_->GetAccessor()->GetIndex(index_oid2);
  *reinterpret_cast<int16_t *>(index_pr2->AccessForceNotNull(0)) = value0;
  *reinterpret_cast<int32_t *>(index_pr2->AccessForceNotNull(1)) = value1;
  std::vector<storage::TupleSlot> results3;
  index2->ScanKey(*exec_ctx_->GetTxn(), *index_pr2, &results3);
  EXPECT_TRUE(inserter.IndexInsert(index_oid2));
  std::vector<storage::TupleSlot> results4;
  index2->ScanKey(*exec_ctx_->GetTxn(), *index_pr2, &results4);
  EXPECT_EQ(results3.size() + 1, results4.size());

  std::vector<storage::TupleSlot> results_before_update, results_after_update, results_before_update_2,
      results_after_update_2;

  // Create Update PR
  {
    auto update_index_pr = updater.GetIndexPR(index_oid);
    *reinterpret_cast<int16_t *>(update_index_pr->AccessForceNotNull(0)) = value0;
    index->ScanKey(*exec_ctx_->GetTxn(), *update_index_pr, &results_before_update);
  }

  // Create Multi Index Update PR
  {
    auto update_index_pr = updater.GetIndexPR(index_oid2);
    *reinterpret_cast<int16_t *>(update_index_pr->AccessForceNotNull(0)) = value0;
    *reinterpret_cast<int32_t *>(update_index_pr->AccessForceNotNull(1)) = value1;
    index2->ScanKey(*exec_ctx_->GetTxn(), *update_index_pr, &results_before_update_2);
  }

  // Create Update PR
  updater.TableDelete(tuple_slot);

  {
    // Delete from index 1
    auto update_index_pr = updater.GetIndexPR(index_oid);
    *reinterpret_cast<int16_t *>(update_index_pr->AccessForceNotNull(0)) = value0;
    updater.IndexDelete(index_oid, tuple_slot);
  }

  {
    // Test that multi index delete succeeds
    auto update_index_pr = updater.GetIndexPR(index_oid2);
    *reinterpret_cast<int16_t *>(update_index_pr->AccessForceNotNull(0)) = value0;
    *reinterpret_cast<int32_t *>(update_index_pr->AccessForceNotNull(1)) = value1;
    updater.IndexDelete(index_oid2, tuple_slot);
  }

  auto update_pr = updater.GetTablePR();
  *reinterpret_cast<int16_t *>(update_pr->AccessForceNotNull(0)) = value0;
  *reinterpret_cast<int32_t *>(update_pr->AccessForceNotNull(1)) = value1_changed;
  *reinterpret_cast<int64_t *>(update_pr->AccessForceNotNull(2)) = value2_changed;
  *reinterpret_cast<int32_t *>(update_pr->AccessForceNotNull(3)) = value3;
  updater.TableInsert();

  {
    // Update index1
    auto update_index_pr = updater.GetIndexPR(index_oid);
    *reinterpret_cast<int16_t *>(update_index_pr->AccessForceNotNull(0)) = value0;
    updater.IndexInsert(index_oid);
    index->ScanKey(*exec_ctx_->GetTxn(), *update_index_pr, &results_after_update);
    EXPECT_EQ(results_before_update.size(), results_after_update.size());
  }

  {
    // Update index2
    auto update_index_pr = updater.GetIndexPR(index_oid2);
    *reinterpret_cast<int16_t *>(update_index_pr->AccessForceNotNull(0)) = value0;
    *reinterpret_cast<int32_t *>(update_index_pr->AccessForceNotNull(1)) = value1_changed;
    updater.IndexInsert(index_oid2);

    index2->ScanKey(*exec_ctx_->GetTxn(), *update_index_pr, &results_after_update_2);
    EXPECT_EQ(results_before_update_2.size(), results_after_update_2.size());
  }
}
}  // namespace terrier::execution::sql::test