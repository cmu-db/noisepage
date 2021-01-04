#include "execution/sql_test.h"
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "self_driving/pilot/action/abstract_action.h"
#include "self_driving/pilot/action/action_defs.h"
#include "test_util/test_harness.h"

namespace noisepage::selfdriving::pilot::test {

class GenerateIndexAction : public execution::SqlBasedTest {
  void SetUp() override {
    // Create the test tables
    SqlBasedTest::SetUp();
    auto exec_ctx = MakeExecCtx();
    GenerateTestTables(exec_ctx.get());
  }

 protected:
};

// NOLINTNEXTLINE
TEST_F(GenerateIndexAction, GenerateIndexAction) {
  std::map<action_id_t, std::unique_ptr<AbstractAction>> action_map;
  std::vector<action_id_t> candidate_actions;
}

}  // namespace noisepage::selfdriving::pilot::test
