#include <vector>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/operator_node.h"
#include "optimizer/optimizer.h"
#include "optimizer/optimizer_task_pool.h"
#include "optimizer/query_to_operator_transformer.h"
#include "test_util/test_harness.h"

#include "gtest/gtest.h"

namespace terrier::optimizer {
class OptimizerRewriteTest : public TerrierTest {
  void SetUp() override { TerrierTest::SetUp(); }

  void TearDown() override { TerrierTest::TearDown(); }
};

// NOLINTNEXTLINE
TEST_F(OptimizerRewriteTest, SingleJoinGetToInnerJoinTest) {
  auto optimizer_context = new OptimizerContext(nullptr);
  auto optimization_context = new OptimizationContext(optimizer_context, nullptr);
  optimizer_context->AddOptimizationContext(optimization_context);

  // Setup the task stack
  auto task_stack = new OptimizerTaskStack();
  optimizer_context->SetTaskPool(task_stack);

  // Create OperatorNode of (GET A) SINGLE_JOIN (AGGREGATE (GET A))
  std::vector<std::unique_ptr<OperatorNode>> children;
  auto left_get = std::make_unique<OperatorNode>(
    LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), {}, "tbl", false),
    std::move(children));
  auto lg_copy = left_get->Copy();

  auto right_get = left_get->Copy();
  EXPECT_EQ(*left_get, *right_get);

  std::vector<std::unique_ptr<OperatorNode>> agg_children;
  agg_children.emplace_back(std::move(right_get));
  auto agg = std::make_unique<OperatorNode>(LogicalAggregateAndGroupBy::Make(), std::move(agg_children));

  std::vector<std::unique_ptr<OperatorNode>> join_children;
  join_children.emplace_back(std::move(left_get));
  join_children.emplace_back(std::move(agg));
  auto join = std::make_unique<OperatorNode>(LogicalSingleJoin::Make(), std::move(join_children));

  // RecordOperatorNodeIntoGroup
  GroupExpression *join_gexpr;
  EXPECT_TRUE(optimizer_context->RecordOperatorNodeIntoGroup(common::ManagedPointer(join), &join_gexpr));
  EXPECT_TRUE(join_gexpr != nullptr);

  // Add rewrite tasks
  group_id_t root_id = join_gexpr->GetGroupID();
  task_stack->Push(new TopDownRewrite(root_id, optimization_context, RuleSetName::PREDICATE_PUSH_DOWN));
  task_stack->Push(new BottomUpRewrite(root_id, optimization_context, RuleSetName::UNNEST_SUBQUERY, false));

  // Execute the tasks in stack
  while (!task_stack->Empty()) {
    auto task = task_stack->Pop();
    task->Execute();
    delete task;
  }

  // Expected OperatorNode: (GET A) INNER_JOIN (AGGREGATE (GET A))
  auto root_gexpr = optimizer_context->GetMemo().GetGroupByID(root_id)->GetLogicalExpression();
  EXPECT_EQ(root_gexpr->Op().GetType(), OpType::LOGICALINNERJOIN);
  EXPECT_EQ(root_gexpr->GetChildrenGroupsSize(), 2);

  auto left_child_gexpr =
    optimizer_context->GetMemo().GetGroupByID(root_gexpr->GetChildGroupId(0))->GetLogicalExpression();
  EXPECT_EQ(left_child_gexpr->Op().GetType(), OpType::LOGICALGET);

  auto right_child_gexpr =
    optimizer_context->GetMemo().GetGroupByID(root_gexpr->GetChildGroupId(1))->GetLogicalExpression();
  EXPECT_EQ(right_child_gexpr->Op().GetType(), OpType::LOGICALAGGREGATEANDGROUPBY);

  auto agg_child_expr =
    optimizer_context->GetMemo().GetGroupByID(right_child_gexpr->GetChildGroupId(0) )->GetLogicalExpression();
  EXPECT_EQ(agg_child_expr->Op().GetType(), OpType::LOGICALGET);

  delete(optimizer_context);
}

}