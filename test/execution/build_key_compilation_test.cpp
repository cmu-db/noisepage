#include <execution/sql/table_vector_iterator.h>
#include <array>
#include <memory>
#include <vector>

#include "catalog/catalog_defs.h"
#include "execution/compiler/codegen.h"
#include "execution/sql/index_creator.h"
#include "execution/sql/index_iterator.h"
#include "execution/sql_test.h"
#include "execution/util/timer.h"
#include "storage/sql_table.h"

namespace terrier::execution::sql::test {

class IndexCreatorTest : public SqlBasedTest {
  void SetUp() override {
    InitTPL();
    // Create the test tables
    SqlBasedTest::SetUp();
    exec_ctx_ = MakeExecCtx();
    GenerateTestTables(exec_ctx_.get());
  }

 public:
  /**
   * Initialize all TPL subsystems
   */
  static void InitTPL() {
    execution::CpuInfo::Instance();
    execution::vm::LLVMEngine::Initialize();
  }

  /**
   * Shutdown all TPL subsystems
   */
  static void ShutdownTPL() {
    terrier::execution::vm::LLVMEngine::Shutdown();
    terrier::LoggersUtil::ShutDown();
  }

 protected:
  /**
   * Execution context to use for the test
   */
  std::unique_ptr<exec::ExecutionContext> exec_ctx_;
};

//// NOLINTNEXTLINE
TEST_F(IndexCreatorTest, CompileTest1) {
  // Get Table Info
  auto txn = exec_ctx_->GetTxn();
  auto accessor = exec_ctx_->GetAccessor();
  auto table_oid = accessor->GetTableOid("test_1");
  auto index_oid = accessor->GetIndexOid("empty_index_1");
  auto table = exec_ctx_->GetAccessor()->GetTable(table_oid);

  IndexCreator index_creator(exec_ctx_.get(), table_oid, index_oid);

  auto table_pr = index_creator.GetTablePR();
  auto index_pr = index_creator.GetIndexPR();

  auto filler_fn = index_creator.GetBuildKeyFn();

  for (auto it = table->begin(); it != table->end(); it++) {
    if (table->Select(txn, *it, table_pr)) {
      filler_fn(table_pr, index_pr);
      index_creator.IndexInsert(index_pr, *it);
    }
  }
}

}  // namespace terrier::execution::sql::test
