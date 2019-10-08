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
  Updater updater(exec_ctx_.get(), table_oid, col_oid_array, 2);

  auto table_pr = inserter.GetTablePR();
  auto schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);

  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(0)) = 445;
  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(1)) = 721;
  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(2)) = 4256;
  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(3)) = 15;

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
  *reinterpret_cast<int32_t *>(insert_pr->AccessForceNotNull(0)) = 1337;

  // Insert into index
  std::vector<storage::TupleSlot> results_before_insertion, results_after_insertion;
  index->ScanKey(*exec_ctx_->GetTxn(), *insert_pr, &results_before_insertion);
  EXPECT_TRUE(inserter.IndexInsert(index_oid));

  // Verify that insertion succeeds
  index->ScanKey(*exec_ctx_->GetTxn(), *insert_pr, &results_after_insertion);
  EXPECT_EQ(results_before_insertion.size() + 1, results_after_insertion.size());

  // Create Update PR
  auto update_pr = updater.GetTablePR();
  *reinterpret_cast<int32_t *>(update_pr->AccessForceNotNull(0)) = 720;
  *reinterpret_cast<int32_t *>(update_pr->AccessForceNotNull(1)) = 4250;

  EXPECT_TRUE(updater.TableUpdate(tuple_slot));
  count = 0;
  for (auto iter = table->begin(); iter != table->end(); iter++) {
    count++;
  }
  EXPECT_EQ(TEST1_SIZE + 1, count);

  std::vector<storage::TupleSlot> results_after_update;
  index->ScanKey(*exec_ctx_->GetTxn(), *insert_pr, &results_after_update);
  EXPECT_EQ(results_after_insertion.size(), results_after_update.size());
}
}  // namespace terrier::execution::sql::test