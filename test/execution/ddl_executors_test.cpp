#include <array>
#include <memory>
#include <vector>

#include "execution/sql_test.h"

#include "catalog/catalog_defs.h"
#include "execution/sql/ddl_executors.h"

namespace terrier::execution::sql::test {

class DDLUtilTests : public SqlBasedTest {
  void SetUp() override {
    // Create the test tables
    SqlBasedTest::SetUp();
    exec_ctx_ = MakeExecCtx();
    GenerateTestTables(exec_ctx_.get());
  }

 public:
  parser::ConstantValueExpression DummyExpr() {
    return parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(0));
  }

 protected:
  /**
   * Execution context to use for the test
   */
  std::unique_ptr<exec::ExecutionContext> exec_ctx_;
};

// NOLINTNEXTLINE
TEST_F(DDLUtilTests, BasicTest) {}

}  // namespace terrier::execution::sql::test
