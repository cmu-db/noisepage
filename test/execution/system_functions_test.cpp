#include "execution/sql/functions/system_functions.h"

#include <string>

#include "common/version.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/execution_settings.h"
#include "execution/sql/value.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::sql::test {

class SystemFunctionsTests : public TplTest {
 public:
  SystemFunctionsTests()
      : ctx_(catalog::db_oid_t(0), nullptr, nullptr, nullptr, nullptr, settings_, nullptr, DISABLED) {}

  exec::ExecutionContext *Ctx() { return &ctx_; }

 private:
  exec::ExecutionSettings settings_{};
  exec::ExecutionContext ctx_;
};

// NOLINTNEXTLINE
TEST_F(SystemFunctionsTests, Version) {
  auto result = StringVal("");
  SystemFunctions::Version(Ctx(), &result);
  EXPECT_TRUE(StringVal(common::NOISEPAGE_VERSION_STR.data()) == result);
}

}  // namespace noisepage::execution::sql::test
