#include "execution/sql/functions/system_functions.h"

#include <string>

#include "common/version.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/execution_context_builder.h"
#include "execution/exec/execution_settings.h"
#include "execution/sql/value.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::sql::test {

class SystemFunctionsTests : public TplTest {
 public:
  SystemFunctionsTests() {
    ctx_ = exec::ExecutionContextBuilder()
               .WithDatabaseOID(DATABASE_OID)
               .WithTxnContext(nullptr)
               .WithExecutionSettings(settings_)
               .WithOutputSchema(nullptr)
               .WithOutputCallback(nullptr)
               .WithCatalogAccessor(nullptr)
               .WithMetricsManager(DISABLED)
               .WithReplicationManager(DISABLED)
               .WithRecoveryManager(DISABLED)
               .Build();
  }

  /** @return A non-owning pointer to the execution context */
  exec::ExecutionContext *Ctx() { return ctx_.get(); }

 private:
  /** Dummy database OID */
  constexpr static catalog::db_oid_t DATABASE_OID{15721};

  /** The execution settings for the test */
  exec::ExecutionSettings settings_{};
  /** The execution context for the test */
  std::unique_ptr<exec::ExecutionContext> ctx_;
};

// NOLINTNEXTLINE
TEST_F(SystemFunctionsTests, Version) {
  auto result = StringVal("");
  SystemFunctions::Version(Ctx(), &result);
  EXPECT_TRUE(StringVal(common::NOISEPAGE_VERSION_STR.data()) == result);
}

}  // namespace noisepage::execution::sql::test
