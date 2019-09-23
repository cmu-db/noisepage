#include <array>
#include <memory>

#include "execution/sql_test.h"

#include "catalog/catalog_defs.h"
#include "execution/sql/inserter.h"
#include "execution/util/timer.h"

namespace terrier::execution::sql::test {

class InserterTest : public SqlBasedTest {
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
TEST_F(InserterTest, SimpleInserterTest) {
  //
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  Inserter inserter(exec_ctx_.get(), table_oid);
  auto table_pr = inserter.GetTablePR();
  auto schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);

  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(0)) = 15;
  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(1)) = 721;
  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(2)) = 445;
  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(3)) = 4256;

  inserter.TableInsert();

  auto table = exec_ctx_->GetAccessor()->GetTable(table_oid);

  size_t count = 0;
  for (auto iter = table->begin(); iter != table->end(); iter++) {
    count++;
  }
  EXPECT_EQ(TEST1_SIZE + 1, count);

  auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");
  auto index_pr = inserter.GetIndexPR(index_oid);
  auto index = exec_ctx_->GetAccessor()->GetIndex(index_oid);
  *reinterpret_cast<int32_t *>(index_pr->AccessForceNotNull(0)) = 15;
  std::vector<storage::TupleSlot> results1;
  index->ScanKey(*exec_ctx_->GetTxn(), *index_pr, &results1);
  EXPECT_TRUE(inserter.IndexInsert(index_oid));
  std::vector<storage::TupleSlot> results2;
  index->ScanKey(*exec_ctx_->GetTxn(), *index_pr, &results2);
  EXPECT_EQ(results1.size() + 1, results2.size());
}


} // namespace terrier::execution::sql::test