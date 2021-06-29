#include "execution/exec/execution_context.h"

#include "execution/compiled_tpl_test.h"

namespace noisepage::execution::test {

class ExecutionContextTest : public TplTest {};

TEST_F(ExecutionContextTest, ItWorks) { EXPECT_TRUE(true); }

}  // namespace noisepage::execution::test
