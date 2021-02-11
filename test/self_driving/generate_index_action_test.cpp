#include "binder/bind_node_visitor.h"
#include "execution/sql_test.h"
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "self_driving/pilot/action/abstract_action.h"
#include "self_driving/pilot/action/action_defs.h"
#include "self_driving/pilot/action/generators/index_action_generator.h"
#include "self_driving/pilot/action/index_action_util.h"

namespace noisepage::selfdriving::pilot::test {

class GenerateIndexAction : public execution::SqlBasedTest {
  void SetUp() override {
    // Create the test tables
    SqlBasedTest::SetUp();
    auto exec_ctx = MakeExecCtx();
    GenerateTestTables(exec_ctx.get());
  }

 protected:
  std::unique_ptr<planner::AbstractPlanNode> GenerateQueryPlan(const std::string &sql_query) {
    auto stmt_list = parser::PostgresParser::BuildParseTree(sql_query);

    auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor_), test_db_oid_);
    binder.BindNameToNode(common::ManagedPointer(stmt_list), nullptr, nullptr);

    // Creating exec_ctx
    std::unique_ptr<optimizer::AbstractCostModel> cost_model = std::make_unique<optimizer::TrivialCostModel>();

    return trafficcop::TrafficCopUtil::Optimize(common::ManagedPointer(test_txn_), common::ManagedPointer(accessor_),
                                                common::ManagedPointer(stmt_list), test_db_oid_, stats_storage_,
                                                std::move(cost_model), optimizer_timeout_ms_)
        ->TakePlanNodeOwnership();
  }

 private:
  const uint64_t optimizer_timeout_ms_ = 10000;
};

// NOLINTNEXTLINE
TEST_F(GenerateIndexAction, GenerateEmptyIndexAction) {
  std::map<action_id_t, std::unique_ptr<AbstractAction>> action_map;
  std::vector<action_id_t> candidate_actions;
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> plans;
  std::string query = "select * from index_action_test_table where col1 = 1;";
  plans.emplace_back(GenerateQueryPlan(query));
  IndexActionGenerator().GenerateActions(plans, nullptr, &action_map, &candidate_actions);

  // There should not be any action when when predicates in the query are already indexed
  EXPECT_EQ(action_map.size(), 0);
  EXPECT_EQ(candidate_actions.size(), 0);
}

// NOLINTNEXTLINE
TEST_F(GenerateIndexAction, GenerateSingleColumnIndexAction) {
  std::map<action_id_t, std::unique_ptr<AbstractAction>> action_map;
  std::vector<action_id_t> candidate_actions;
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> plans;
  std::string table_name("index_action_test_table");
  std::string query = "select * from " + table_name + " where col2 = 1;";
  plans.emplace_back(GenerateQueryPlan(query));
  IndexActionGenerator().GenerateActions(plans, nullptr, &action_map, &candidate_actions);

  // There should be a candidate create index action on col2 and a corresponding drop index action
  EXPECT_EQ(action_map.size(), 2);
  EXPECT_EQ(candidate_actions.size(), 1);

  std::string create_index_command = action_map.at(candidate_actions[0])->GetSQLCommand();
  std::vector<IndexColumn> columns{IndexColumn("col2")};
  std::string index_name = IndexActionUtil::GenerateIndexName(table_name, columns);
  std::string expected_command = "create index " + index_name + " on " + table_name + "(col2, );";
  EXPECT_EQ(create_index_command, expected_command);

  // Check that the two actions are reverse actions to each other
  std::vector<action_id_t> action_ids;
  action_ids.reserve(action_map.size());
  for (auto &it : action_map) {
    action_ids.push_back(it.first);
  }
  action_id_t first_action_id = action_ids[0];
  action_id_t second_action_id = action_ids[1];
  EXPECT_EQ(action_map[first_action_id]->GetReverseActions()[0], second_action_id);
  EXPECT_EQ(action_map[second_action_id]->GetReverseActions()[0], first_action_id);
  EXPECT_EQ(action_map[first_action_id]->GetEnabledActions()[0], second_action_id);
  EXPECT_EQ(action_map[second_action_id]->GetEnabledActions()[0], first_action_id);
  EXPECT_EQ(action_map[first_action_id]->GetInvalidatedActions()[0], first_action_id);
  EXPECT_EQ(action_map[second_action_id]->GetInvalidatedActions()[0], second_action_id);
}

// NOLINTNEXTLINE
TEST_F(GenerateIndexAction, GenerateMultipleColumnIndexAction) {
  std::map<action_id_t, std::unique_ptr<AbstractAction>> action_map;
  std::vector<action_id_t> candidate_actions;
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> plans;
  std::string table_name("index_action_test_table");
  std::string query = "select * from " + table_name + " where col2 = 1 and col3 = 1;";
  plans.emplace_back(GenerateQueryPlan(query));
  IndexActionGenerator().GenerateActions(plans, nullptr, &action_map, &candidate_actions);

  // There should be a candidate create index action on col2 and col3 and a corresponding drop index action
  EXPECT_EQ(action_map.size(), 2);
  EXPECT_EQ(candidate_actions.size(), 1);

  std::string create_index_command = action_map.at(candidate_actions[0])->GetSQLCommand();
  std::vector<IndexColumn> columns{IndexColumn("col2"), IndexColumn("col3")};
  std::string index_name = IndexActionUtil::GenerateIndexName(table_name, columns);
  std::string expected_command = "create index " + index_name + " on " + table_name + "(col2, col3, );";
  EXPECT_EQ(create_index_command, expected_command);
}

// NOLINTNEXTLINE
TEST_F(GenerateIndexAction, GenerateUncoveredColumnIndexAction) {
  std::map<action_id_t, std::unique_ptr<AbstractAction>> action_map;
  std::vector<action_id_t> candidate_actions;
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> plans;
  std::string table_name("index_action_test_table");
  std::string query = "select * from " + table_name + " where col1 = 1 and col3 = 1;";
  plans.emplace_back(GenerateQueryPlan(query));
  IndexActionGenerator().GenerateActions(plans, nullptr, &action_map, &candidate_actions);

  // There should be a candidate create index action on col1 and col3 and a corresponding drop index action
  EXPECT_EQ(action_map.size(), 2);
  EXPECT_EQ(candidate_actions.size(), 1);

  std::string create_index_command = action_map.at(candidate_actions[0])->GetSQLCommand();
  std::vector<IndexColumn> columns{IndexColumn("col1"), IndexColumn("col3")};
  std::string index_name = IndexActionUtil::GenerateIndexName(table_name, columns);
  std::string expected_command = "create index " + index_name + " on " + table_name + "(col1, col3, );";
  EXPECT_EQ(create_index_command, expected_command);
}

// NOLINTNEXTLINE
TEST_F(GenerateIndexAction, GenerateInequalityColumnIndexAction) {
  std::map<action_id_t, std::unique_ptr<AbstractAction>> action_map;
  std::vector<action_id_t> candidate_actions;
  std::vector<std::unique_ptr<planner::AbstractPlanNode>> plans;
  std::string table_name("index_action_test_table");
  std::string query = "select * from " + table_name + " where col2 > 1 and col2 < 4 and col3 = 1;";
  plans.emplace_back(GenerateQueryPlan(query));
  IndexActionGenerator().GenerateActions(plans, nullptr, &action_map, &candidate_actions);

  // There should be a candidate create index action on col3 and col2 (equality followed by inequality) and a
  // corresponding drop index action
  EXPECT_EQ(action_map.size(), 2);
  EXPECT_EQ(candidate_actions.size(), 1);

  std::string create_index_command = action_map.at(candidate_actions[0])->GetSQLCommand();
  std::vector<IndexColumn> columns{IndexColumn("col3"), IndexColumn("col2")};
  std::string index_name = IndexActionUtil::GenerateIndexName(table_name, columns);
  std::string expected_command = "create index " + index_name + " on " + table_name + "(col3, col2, );";
  EXPECT_EQ(create_index_command, expected_command);
}

}  // namespace noisepage::selfdriving::pilot::test
