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
  auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");

  Inserter inserter(exec_ctx_.get(), table_oid);
  auto table_pr = inserter.GetTablePR();
  auto schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);
  auto columns = schema.GetColumns();

  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(0)) = 15;
  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(1)) = 721;
  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(2)) = 445;
  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(3)) = 4256;


}
} // namespace terrier::execution::sql::test