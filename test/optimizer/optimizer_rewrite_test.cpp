#include <iostream>
#include <set>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/operator_node.h"
#include "optimizer/optimizer.h"
#include "optimizer/optimizer_task_pool.h"
#include "optimizer/query_to_operator_transformer.h"
#include "parser/expression/comparison_expression.h"
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

  auto left_get_gexpr =
    optimizer_context->GetMemo().GetGroupByID(root_gexpr->GetChildGroupId(0))->GetLogicalExpression();
  EXPECT_EQ(left_get_gexpr->Op().GetType(), OpType::LOGICALGET);

  auto agg_gexpr =
    optimizer_context->GetMemo().GetGroupByID(root_gexpr->GetChildGroupId(1))->GetLogicalExpression();
  EXPECT_EQ(agg_gexpr->Op().GetType(), OpType::LOGICALAGGREGATEANDGROUPBY);
  EXPECT_EQ(root_gexpr->GetChildrenGroupsSize(), 2);

  auto right_get_gexpr =
    optimizer_context->GetMemo().GetGroupByID(agg_gexpr->GetChildGroupId(0) )->GetLogicalExpression();
  EXPECT_EQ(right_get_gexpr->Op().GetType(), OpType::LOGICALGET);

  delete(optimizer_context);
}

// NOLINTNEXTLINE
TEST_F(OptimizerRewriteTest, DependentJoinGetToInnerJoin) {
  auto optimizer_context = new OptimizerContext(nullptr);
  auto optimization_context = new OptimizationContext(optimizer_context, nullptr);
  optimizer_context->AddOptimizationContext(optimization_context);

  // Setup the task stack
  auto task_stack = new OptimizerTaskStack();
  optimizer_context->SetTaskPool(task_stack);

  // Create OperatorNode of (GET A) SINGLE_JOIN (FILTER (AGGREGATE (GET B)))
  std::vector<std::unique_ptr<OperatorNode>> left_children;
  auto left_get = std::make_unique<OperatorNode>(
    LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), {}, "tbl1", false),
    std::move(left_children));
  auto lg_copy = left_get->Copy();

  std::vector<std::unique_ptr<OperatorNode>> right_children;
  auto right_get = std::make_unique<OperatorNode>(
    LogicalGet::Make(catalog::db_oid_t(4), catalog::namespace_oid_t(5), catalog::table_oid_t(6), {}, "tbl2", false),
    std::move(right_children));

  std::vector<std::unique_ptr<OperatorNode>> agg_children;
  agg_children.emplace_back(std::move(right_get));
  auto agg = std::make_unique<OperatorNode>(LogicalAggregateAndGroupBy::Make(), std::move(agg_children));

  // Build a COMPARE_EQUAL expression for filter, which contains two children
  parser::AbstractExpression *expr_b_1 =
    new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  parser::AbstractExpression *expr_b_2 =
    new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));

  std::vector<std::unique_ptr<parser::AbstractExpression>> expr_cmp_children;
  expr_cmp_children.emplace_back(expr_b_1);
  expr_cmp_children.emplace_back(expr_b_2);
  parser::AbstractExpression *expr_cmp =
    new parser::ComparisonExpression(parser::ExpressionType::COMPARE_EQUAL, std::move(expr_cmp_children));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_cmp);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_cmp);

  // Two predicates refer to different tables
  auto outer_expression = AnnotatedExpression(x_1, std::unordered_set<std::string>{"tbl1"});
  auto inner_expression = AnnotatedExpression(x_2, std::unordered_set<std::string>{"tbl2"});
  std::vector<AnnotatedExpression> predicates {outer_expression, inner_expression};
  std::vector<std::unique_ptr<OperatorNode>> filter_children;
  filter_children.emplace_back(std::move(agg));
  auto filter = std::make_unique<OperatorNode>(LogicalFilter::Make(std::move(predicates)), std::move(filter_children));

  std::vector<std::unique_ptr<OperatorNode>> join_children;
  join_children.emplace_back(std::move(left_get));
  join_children.emplace_back(std::move(filter));
  auto join = std::make_unique<OperatorNode>(LogicalSingleJoin::Make(), std::move(join_children));

  // RecordOperatorNodeIntoGroup
  GroupExpression *original_gexpr;
  EXPECT_TRUE(optimizer_context->RecordOperatorNodeIntoGroup(common::ManagedPointer(join), &original_gexpr));
  EXPECT_TRUE(original_gexpr != nullptr);

  // Add rewrite tasks
  group_id_t root_id = original_gexpr->GetGroupID();
  task_stack->Push(new BottomUpRewrite(root_id, optimization_context, RuleSetName::UNNEST_SUBQUERY, false));

  // Execute the tasks in stack
  while (!task_stack->Empty()) {
    auto task = task_stack->Pop();
    task->Execute();
    delete task;
  }

  // Expected OperatorNode: FILTER((GET A) INNER_JOIN (FILTER (AGGREGATE (GET B))))
  auto root_gexpr = optimizer_context->GetMemo().GetGroupByID(root_id)->GetLogicalExpression();
  EXPECT_EQ(root_gexpr->Op().GetType(), OpType::LOGICALFILTER);
  EXPECT_EQ(root_gexpr->GetChildrenGroupsSize(), 1);

  auto join_gexpr = optimizer_context->GetMemo().GetGroupByID(root_gexpr->GetChildGroupId(0))->GetLogicalExpression();
  EXPECT_EQ(join_gexpr->Op().GetType(), OpType::LOGICALINNERJOIN);
  EXPECT_EQ(join_gexpr->GetChildrenGroupsSize(), 2);

  auto left_get_gexpr =
    optimizer_context->GetMemo().GetGroupByID(join_gexpr->GetChildGroupId(0))->GetLogicalExpression();
  EXPECT_EQ(left_get_gexpr->Op().GetType(), OpType::LOGICALGET);

  auto filter_gexpr =
    optimizer_context->GetMemo().GetGroupByID(join_gexpr->GetChildGroupId(1))->GetLogicalExpression();
  EXPECT_EQ(filter_gexpr->Op().GetType(), OpType::LOGICALFILTER);
  EXPECT_EQ(filter_gexpr->GetChildrenGroupsSize(), 1);

  auto agg_gexpr =
    optimizer_context->GetMemo().GetGroupByID(filter_gexpr->GetChildGroupId(0))->GetLogicalExpression();
  EXPECT_EQ(agg_gexpr->Op().GetType(), OpType::LOGICALAGGREGATEANDGROUPBY);
  EXPECT_EQ(agg_gexpr->GetChildrenGroupsSize(), 1);

  auto right_get_gexpr =
    optimizer_context->GetMemo().GetGroupByID(agg_gexpr->GetChildGroupId(0) )->GetLogicalExpression();
  EXPECT_EQ(right_get_gexpr->Op().GetType(), OpType::LOGICALGET);

  // The predicates of upper filter should not refer to tables contained in agg
  for (auto &predicate : root_gexpr->Op().As<LogicalFilter>()->GetPredicates()) {
    auto table_alias_set = predicate.GetTableAliasSet();
    EXPECT_TRUE(table_alias_set.find("tbl2") == table_alias_set.end());
  }

  // The predicates of lower filter should refer to tables contained in agg
  for (auto &predicate : filter_gexpr->Op().As<LogicalFilter>()->GetPredicates()) {
    auto table_alias_set = predicate.GetTableAliasSet();
    EXPECT_TRUE(table_alias_set.find("tbl2") != table_alias_set.end());
  }

  delete(expr_cmp);
  delete(optimizer_context);
}

}