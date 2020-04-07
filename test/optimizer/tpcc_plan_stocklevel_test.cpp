#include <memory>
#include <string>

#include "parser/expression/derived_value_expression.h"
#include "planner/plannodes/aggregate_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "test_util/test_harness.h"
#include "test_util/tpcc/tpcc_plan_test.h"

namespace terrier {

struct TpccPlanStockLevelTests : public TpccPlanTest {};

// NOLINTNEXTLINE
TEST_F(TpccPlanStockLevelTests, GetDistrictOrderId) {
  std::string query = "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID=1 AND D_ID=2";
  OptimizeQuery(query, tbl_district_, TpccPlanTest::CheckIndexScan);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanStockLevelTests, GetCountStock) {
  auto check = [](TpccPlanTest *test, parser::SelectStatement *sel_stmt, catalog::table_oid_t tbl_oid,
                  std::unique_ptr<planner::AbstractPlanNode> plan) {
    // Validates the plan node's structure
    EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::AGGREGATE);
    auto agg = reinterpret_cast<planner::AggregatePlanNode *>(plan.get());
    EXPECT_EQ(agg->GetOutputSchema()->GetColumns().size(), 1);
    EXPECT_EQ(agg->GetAggregateTerms().size(), 1);
    EXPECT_EQ(agg->GetAggregateStrategyType(), planner::AggregateStrategyType::PLAIN);

    EXPECT_EQ(plan->GetChildrenSize(), 1);
    EXPECT_EQ(plan->GetChild(0)->GetPlanNodeType(), planner::PlanNodeType::NESTLOOP);
    auto nl = reinterpret_cast<const planner::NestedLoopJoinPlanNode *>(plan->GetChild(0));
    EXPECT_EQ(nl->GetLogicalJoinType(), planner::LogicalJoinType::INNER);
    EXPECT_NE(nl->GetJoinPredicate().Get(), nullptr);
    EXPECT_EQ(nl->GetOutputSchema()->GetColumns().size(), 1);
    EXPECT_EQ(nl->GetChildrenSize(), 2);

    auto *lchild = nl->GetChild(0);
    auto *rchild = nl->GetChild(1);
    EXPECT_EQ(lchild->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);
    EXPECT_EQ(rchild->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);
  };

  std::string query =
      "SELECT COUNT(DISTINCT S_I_ID) AS STOCK_COUNT FROM \"ORDER LINE\", STOCK "
      "WHERE OL_W_ID = 1 AND OL_D_ID = 2 AND OL_O_ID < 3 AND OL_O_ID >= 4 AND "
      "S_W_ID = 5 AND S_I_ID = OL_I_ID AND S_QUANTITY < 6";
  OptimizeQuery(query, tbl_stock_, check);
}

// NOLINTNEXTLINE
TEST_F(TpccPlanStockLevelTests, GetJoinStock) {
  auto check = [](TpccPlanTest *test, parser::SelectStatement *sel_stmt, catalog::table_oid_t tbl_oid,
                  std::unique_ptr<planner::AbstractPlanNode> plan) {
    EXPECT_EQ(plan->GetPlanNodeType(), planner::PlanNodeType::NESTLOOP);
    EXPECT_EQ(plan->GetChildrenSize(), 2);
    auto *join = reinterpret_cast<planner::NestedLoopJoinPlanNode *>(plan.get());
    EXPECT_EQ(join->GetLogicalJoinType(), planner::LogicalJoinType::INNER);

    // Output schema check
    auto j_schema = plan->GetOutputSchema();
    EXPECT_EQ(j_schema->GetColumns().size(), 1);
    auto j_col = j_schema->GetColumns()[0].GetExpr().CastManagedPointerTo<parser::DerivedValueExpression>();
    EXPECT_EQ(j_col->GetTupleIdx(), 1);
    EXPECT_EQ(j_col->GetValueIdx(), 1);

    // Join predicate check
    auto join_pred = join->GetJoinPredicate();
    EXPECT_EQ(join_pred->GetExpressionType(), parser::ExpressionType::COMPARE_EQUAL);
    EXPECT_EQ(join_pred->GetChildrenSize(), 2);
    auto jp_left = join_pred->GetChild(0).CastManagedPointerTo<parser::DerivedValueExpression>();
    auto jp_right = join_pred->GetChild(1).CastManagedPointerTo<parser::DerivedValueExpression>();
    EXPECT_EQ(jp_left->GetTupleIdx(), 0);
    EXPECT_EQ(jp_left->GetValueIdx(), 0);
    EXPECT_EQ(jp_right->GetTupleIdx(), 1);
    EXPECT_EQ(jp_right->GetValueIdx(), 0);

    auto *lchild = plan->GetChild(0);
    auto *rchild = plan->GetChild(1);
    EXPECT_EQ(lchild->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);
    EXPECT_EQ(rchild->GetPlanNodeType(), planner::PlanNodeType::INDEXSCAN);
  };

  std::string query = "SELECT S_I_ID FROM \"ORDER LINE\", STOCK WHERE OL_W_ID=1 AND S_W_ID=1 AND OL_W_ID=S_W_ID";
  OptimizeQuery(query, tbl_stock_, check);
}

}  // namespace terrier
