#include <string>

#include "common/version.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/functions/system_functions.h"
#include "execution/sql/value.h"
#include "execution/tpl_test.h"

namespace terrier::execution::sql::test {

class SystemFunctionsTests : public TplTest {
 public:
  SystemFunctionsTests() : ctx_(catalog::db_oid_t(0), nullptr, nullptr, nullptr, nullptr) {}

  exec::ExecutionContext *Ctx() { return &ctx_; }

 private:
  exec::ExecutionContext ctx_;
};

// NOLINTNEXTLINE
TEST_F(SystemFunctionsTests, Version) {
  auto result = StringVal("");
  SystemFunctions::Version(Ctx(), &result);
  EXPECT_EQ(result, StringVal(common::NOISEPAGE_VERSION_STR.data()));
}

}  // namespace terrier::execution::sql::test
