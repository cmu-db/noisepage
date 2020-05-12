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
#include "parser/expression/abstract_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/comparison_expression.h"
#include "test_util/test_harness.h"

#include "gtest/gtest.h"

namespace terrier::optimizer {
class OptimizerRewriteTest : public TerrierTest {
  void SetUp() override { TerrierTest::SetUp(); }

  void TearDown() override { TerrierTest::TearDown(); }
};

// NOLINTNEXTLINE
TEST_F(OptimizerRewriteTest, PushImplicitFilterThroughJoinAndEmbedFilterIntoGetTest) {
  auto optimizer_context = new OptimizerContext(nullptr);
  auto optimization_context = new OptimizationContext(optimizer_context, nullptr);
  optimizer_context->AddOptimizationContext(optimization_context);

  // Setup the task stack
  auto task_stack = new OptimizerTaskStack();
  optimizer_context->SetTaskPool(task_stack);

  // Create OperatorNode of (GET A) INNER_JOIN (GET B), INNER_JOIN has two predicates
  std::vector<std::unique_ptr<OperatorNode>> lg_children;
  auto left_get = std::make_unique<OperatorNode>(
      LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), {}, "tbl1", false),
      std::move(lg_children));

  std::vector<std::unique_ptr<OperatorNode>> rg_children;
  auto right_get = std::make_unique<OperatorNode>(
      LogicalGet::Make(catalog::db_oid_t(4), catalog::namespace_oid_t(5), catalog::table_oid_t(6), {}, "tbl2", false),
      std::move(rg_children));

  // Build two expressions for inner join
  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetDecimal(1.0));
  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto expression_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>{"tbl1"});
  auto expression_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>{"tbl2"});

  std::vector<AnnotatedExpression> join_pred{expression_1, expression_2};
  std::vector<std::unique_ptr<OperatorNode>> join_children;
  join_children.emplace_back(std::move(left_get));
  join_children.emplace_back(std::move(right_get));
  auto join = std::make_unique<OperatorNode>(LogicalInnerJoin::Make(std::move(join_pred)), std::move(join_children));

  // RecordOperatorNodeIntoGroup
  GroupExpression *gexpr;
  EXPECT_TRUE(optimizer_context->RecordOperatorNodeIntoGroup(common::ManagedPointer(join), &gexpr));
  EXPECT_TRUE(gexpr != nullptr);

  // Add rewrite tasks
  group_id_t root_id = gexpr->GetGroupID();
  task_stack->Push(new TopDownRewrite(root_id, optimization_context, RuleSetName::PREDICATE_PUSH_DOWN));

  // Execute the tasks in stack
  // Two rules will be applied: PUSH_FILTER_THROUGH_JOIN & EMBED_FILTER_INTO_GET
  while (!task_stack->Empty()) {
    auto task = task_stack->Pop();
    task->Execute();
    delete task;
  }

  // Expected OperatorNode: (GET A) INNER_JOIN (GET B), while pred1 is in GET A and pred2 is in GET B
  auto root_gexpr = optimizer_context->GetMemo().GetGroupByID(root_id)->GetLogicalExpression();
  EXPECT_EQ(root_gexpr->Op().GetType(), OpType::LOGICALINNERJOIN);
  EXPECT_EQ(root_gexpr->GetChildrenGroupsSize(), 2);
  EXPECT_EQ(root_gexpr->Op().As<LogicalInnerJoin>()->GetJoinPredicates().size(), 0);

  auto left_get_gexpr =
      optimizer_context->GetMemo().GetGroupByID(root_gexpr->GetChildGroupId(0))->GetLogicalExpression();
  EXPECT_EQ(left_get_gexpr->Op().GetType(), OpType::LOGICALGET);
  EXPECT_EQ(left_get_gexpr->Op().As<LogicalGet>()->GetPredicates().size(), 1);
  EXPECT_EQ(left_get_gexpr->Op().As<LogicalGet>()->GetPredicates()[0], expression_1);

  auto right_get_gexpr =
      optimizer_context->GetMemo().GetGroupByID(root_gexpr->GetChildGroupId(1))->GetLogicalExpression();
  EXPECT_EQ(right_get_gexpr->Op().GetType(), OpType::LOGICALGET);
  EXPECT_EQ(right_get_gexpr->Op().As<LogicalGet>()->GetPredicates().size(), 1);
  EXPECT_EQ(right_get_gexpr->Op().As<LogicalGet>()->GetPredicates()[0], expression_2);

  delete expr_b_1;
  delete expr_b_2;
  delete optimizer_context;
}

// NOLINTNEXTLINE
TEST_F(OptimizerRewriteTest, PushExplicitFilterThroughJoinAndEmbedFilterIntoGetTest) {
  auto optimizer_context = new OptimizerContext(nullptr);
  auto optimization_context = new OptimizationContext(optimizer_context, nullptr);
  optimizer_context->AddOptimizationContext(optimization_context);

  // Setup the task stack
  auto task_stack = new OptimizerTaskStack();
  optimizer_context->SetTaskPool(task_stack);

  // Create OperatorNode of FILTER((GET A) INNER_JOIN (GET B)), FILTER has two predicates
  std::vector<std::unique_ptr<OperatorNode>> lg_children;
  auto left_get = std::make_unique<OperatorNode>(
      LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), {}, "tbl1", false),
      std::move(lg_children));

  std::vector<std::unique_ptr<OperatorNode>> rg_children;
  auto right_get = std::make_unique<OperatorNode>(
      LogicalGet::Make(catalog::db_oid_t(4), catalog::namespace_oid_t(5), catalog::table_oid_t(6), {}, "tbl2", false),
      std::move(rg_children));

  // Build two expressions for filter
  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetDecimal(1.0));
  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto expression_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>{"tbl1"});
  auto expression_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>{"tbl2"});

  std::vector<AnnotatedExpression> join_pred;
  std::vector<std::unique_ptr<OperatorNode>> join_children;
  join_children.emplace_back(std::move(left_get));
  join_children.emplace_back(std::move(right_get));
  auto join = std::make_unique<OperatorNode>(LogicalInnerJoin::Make(std::move(join_pred)), std::move(join_children));

  std::vector<AnnotatedExpression> filter_pred{expression_1, expression_2};
  std::vector<std::unique_ptr<OperatorNode>> filter_children;
  filter_children.emplace_back(std::move(join));
  auto filter = std::make_unique<OperatorNode>(LogicalFilter::Make(std::move(filter_pred)), std::move(filter_children));

  // RecordOperatorNodeIntoGroup
  GroupExpression *gexpr;
  EXPECT_TRUE(optimizer_context->RecordOperatorNodeIntoGroup(common::ManagedPointer(filter), &gexpr));
  EXPECT_TRUE(gexpr != nullptr);

  // Add rewrite tasks
  group_id_t root_id = gexpr->GetGroupID();
  task_stack->Push(new TopDownRewrite(root_id, optimization_context, RuleSetName::PREDICATE_PUSH_DOWN));

  // Execute the tasks in stack
  // Two rules will be applied: PUSH_FILTER_THROUGH_JOIN & EMBED_FILTER_INTO_GET
  while (!task_stack->Empty()) {
    auto task = task_stack->Pop();
    task->Execute();
    delete task;
  }

  // Expected OperatorNode: (GET A) INNER_JOIN (GET B), while pred1 is in GET A and pred2 is in GET B
  auto root_gexpr = optimizer_context->GetMemo().GetGroupByID(root_id)->GetLogicalExpression();
  EXPECT_EQ(root_gexpr->Op().GetType(), OpType::LOGICALINNERJOIN);
  EXPECT_EQ(root_gexpr->GetChildrenGroupsSize(), 2);
  EXPECT_EQ(root_gexpr->Op().As<LogicalInnerJoin>()->GetJoinPredicates().size(), 0);

  auto left_get_gexpr =
      optimizer_context->GetMemo().GetGroupByID(root_gexpr->GetChildGroupId(0))->GetLogicalExpression();
  EXPECT_EQ(left_get_gexpr->Op().GetType(), OpType::LOGICALGET);
  EXPECT_EQ(left_get_gexpr->Op().As<LogicalGet>()->GetPredicates().size(), 1);
  EXPECT_EQ(left_get_gexpr->Op().As<LogicalGet>()->GetPredicates()[0], expression_1);

  auto right_get_gexpr =
      optimizer_context->GetMemo().GetGroupByID(root_gexpr->GetChildGroupId(1))->GetLogicalExpression();
  EXPECT_EQ(right_get_gexpr->Op().GetType(), OpType::LOGICALGET);
  EXPECT_EQ(right_get_gexpr->Op().As<LogicalGet>()->GetPredicates().size(), 1);
  EXPECT_EQ(right_get_gexpr->Op().As<LogicalGet>()->GetPredicates()[0], expression_2);

  delete expr_b_1;
  delete expr_b_2;
  delete optimizer_context;
}

// NOLINTNEXTLINE
TEST_F(OptimizerRewriteTest, InnerJoinToSemiJoinTest) {
  auto optimizer_context = new OptimizerContext(nullptr);
  auto optimization_context = new OptimizationContext(optimizer_context, nullptr);
  optimizer_context->AddOptimizationContext(optimization_context);

  // Setup the task stack
  auto task_stack = new OptimizerTaskStack();
  optimizer_context->SetTaskPool(task_stack);

  // Create OperatorNode of FILTER((GET A) INNER_JOIN (GET B)), FILTER has three predicates and one of them is
  // COMPARE_IN
  std::vector<std::unique_ptr<OperatorNode>> lg_children;
  auto left_get = std::make_unique<OperatorNode>(
      LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), {}, "tbl1", false),
      std::move(lg_children));

  std::vector<std::unique_ptr<OperatorNode>> rg_children;
  auto right_get = std::make_unique<OperatorNode>(
      LogicalGet::Make(catalog::db_oid_t(4), catalog::namespace_oid_t(5), catalog::table_oid_t(6), {}, "tbl2", false),
      std::move(rg_children));

  // Build three expressions for filter
  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetDecimal(1.0));
  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto expression_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>{"tbl1"});
  auto expression_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>{"tbl2"});

  parser::AbstractExpression *expr_col_1 = new parser::ColumnValueExpression();
  parser::AbstractExpression *expr_col_2 = new parser::ColumnValueExpression();
  std::vector<std::unique_ptr<parser::AbstractExpression>> cmp_children;
  cmp_children.emplace_back(expr_col_1);
  cmp_children.emplace_back(expr_col_2);
  parser::AbstractExpression *expr_in =
      new parser::ComparisonExpression(parser::ExpressionType::COMPARE_IN, std::move(cmp_children));
  auto expression_in = AnnotatedExpression(common::ManagedPointer<parser::AbstractExpression>(expr_in),
                                           std::unordered_set<std::string>{"tbl1", "tbl3"});

  std::vector<AnnotatedExpression> join_pred;
  std::vector<std::unique_ptr<OperatorNode>> join_children;
  join_children.emplace_back(std::move(left_get));
  join_children.emplace_back(std::move(right_get));
  auto join = std::make_unique<OperatorNode>(LogicalInnerJoin::Make(std::move(join_pred)), std::move(join_children));

  std::vector<AnnotatedExpression> filter_pred{expression_1, expression_2, expression_in};
  std::vector<std::unique_ptr<OperatorNode>> filter_children;
  filter_children.emplace_back(std::move(join));
  auto filter = std::make_unique<OperatorNode>(LogicalFilter::Make(std::move(filter_pred)), std::move(filter_children));

  // RecordOperatorNodeIntoGroup
  GroupExpression *gexpr;
  EXPECT_TRUE(optimizer_context->RecordOperatorNodeIntoGroup(common::ManagedPointer(filter), &gexpr));
  EXPECT_TRUE(gexpr != nullptr);

  // Add rewrite tasks
  group_id_t root_id = gexpr->GetGroupID();
  task_stack->Push(new TopDownRewrite(root_id, optimization_context, RuleSetName::PREDICATE_PUSH_DOWN));

  // Execute the tasks in stack
  // Two rules will be applied: PUSH_FILTER_THROUGH_JOIN & EMBED_FILTER_INTO_GET
  while (!task_stack->Empty()) {
    auto task = task_stack->Pop();
    task->Execute();
    delete task;
  }

  // Expected OperatorNode: (GET A) SEMIJOIN (GET B), pred1 is in GET A, pred2 is in GET B
  // pred_in has been transformed to COMPARE_EQUAL and is in SEMIJOIN
  auto root_gexpr = optimizer_context->GetMemo().GetGroupByID(root_id)->GetLogicalExpression();
  EXPECT_EQ(root_gexpr->Op().GetType(), OpType::LOGICALSEMIJOIN);
  EXPECT_EQ(root_gexpr->GetChildrenGroupsSize(), 2);
  EXPECT_EQ(root_gexpr->Op().As<LogicalSemiJoin>()->GetJoinPredicates().size(), 1);
  EXPECT_EQ(root_gexpr->Op().As<LogicalSemiJoin>()->GetJoinPredicates()[0].GetExpr()->GetExpressionType(),
            parser::ExpressionType::COMPARE_EQUAL);

  auto left_get_gexpr =
      optimizer_context->GetMemo().GetGroupByID(root_gexpr->GetChildGroupId(0))->GetLogicalExpression();
  EXPECT_EQ(left_get_gexpr->Op().GetType(), OpType::LOGICALGET);
  EXPECT_EQ(left_get_gexpr->Op().As<LogicalGet>()->GetPredicates().size(), 1);
  EXPECT_EQ(left_get_gexpr->Op().As<LogicalGet>()->GetPredicates()[0], expression_1);

  auto right_get_gexpr =
      optimizer_context->GetMemo().GetGroupByID(root_gexpr->GetChildGroupId(1))->GetLogicalExpression();
  EXPECT_EQ(right_get_gexpr->Op().GetType(), OpType::LOGICALGET);
  EXPECT_EQ(right_get_gexpr->Op().As<LogicalGet>()->GetPredicates().size(), 1);
  EXPECT_EQ(right_get_gexpr->Op().As<LogicalGet>()->GetPredicates()[0], expression_2);

  delete expr_b_1;
  delete expr_b_2;
  delete expr_in;
  delete optimizer_context;
}

// NOLINTNEXTLINE
TEST_F(OptimizerRewriteTest, PushFilterThroughAggregationAndEmbedFilterIntoGetTest) {
  auto optimizer_context = new OptimizerContext(nullptr);
  auto optimization_context = new OptimizationContext(optimizer_context, nullptr);
  optimizer_context->AddOptimizationContext(optimization_context);

  // Setup the task stack
  auto task_stack = new OptimizerTaskStack();
  optimizer_context->SetTaskPool(task_stack);

  // Create OperatorNode of FILTER(AGG (GET A)), FILTER has two predicates
  std::vector<std::unique_ptr<OperatorNode>> get_children;
  auto get = std::make_unique<OperatorNode>(
      LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), {}, "tbl1", false),
      std::move(get_children));

  // Build two expressions for filter
  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetDecimal(1.0));
  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto expression_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>{"tbl1"});
  auto expression_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>{"tbl2"});

  std::vector<std::unique_ptr<OperatorNode>> agg_children;
  agg_children.emplace_back(std::move(get));
  auto agg = std::make_unique<OperatorNode>(LogicalAggregateAndGroupBy::Make(), std::move(agg_children));

  std::vector<AnnotatedExpression> filter_pred{expression_1, expression_2};
  std::vector<std::unique_ptr<OperatorNode>> filter_children;
  filter_children.emplace_back(std::move(agg));
  auto filter = std::make_unique<OperatorNode>(LogicalFilter::Make(std::move(filter_pred)), std::move(filter_children));

  // RecordOperatorNodeIntoGroup
  GroupExpression *gexpr;
  EXPECT_TRUE(optimizer_context->RecordOperatorNodeIntoGroup(common::ManagedPointer(filter), &gexpr));
  EXPECT_TRUE(gexpr != nullptr);

  // Add rewrite tasks
  group_id_t root_id = gexpr->GetGroupID();
  task_stack->Push(new TopDownRewrite(root_id, optimization_context, RuleSetName::PREDICATE_PUSH_DOWN));

  // Execute the tasks in stack
  // Two rules will be applied: PUSH_FILTER_THROUGH_AGGREGATION & EMBED_FILTER_INTO_GET
  while (!task_stack->Empty()) {
    auto task = task_stack->Pop();
    task->Execute();
    delete task;
  }

  // Expected OperatorNode: AGG (GET A), while pred1 and pred2 is in GET A
  auto root_gexpr = optimizer_context->GetMemo().GetGroupByID(root_id)->GetLogicalExpression();
  EXPECT_EQ(root_gexpr->Op().GetType(), OpType::LOGICALAGGREGATEANDGROUPBY);
  EXPECT_EQ(root_gexpr->GetChildrenGroupsSize(), 1);

  auto get_gexpr = optimizer_context->GetMemo().GetGroupByID(root_gexpr->GetChildGroupId(0))->GetLogicalExpression();
  EXPECT_EQ(get_gexpr->Op().GetType(), OpType::LOGICALGET);
  EXPECT_EQ(get_gexpr->Op().As<LogicalGet>()->GetPredicates().size(), 2);
  std::vector<AnnotatedExpression> preds{expression_1, expression_2};
  EXPECT_EQ(get_gexpr->Op().As<LogicalGet>()->GetPredicates(), preds);

  delete expr_b_1;
  delete expr_b_2;
  delete optimizer_context;
}

// NOLINTNEXTLINE
TEST_F(OptimizerRewriteTest, CombindConsecutiveFilterAndEmbedFilterIntoGetTest) {
  auto optimizer_context = new OptimizerContext(nullptr);
  auto optimization_context = new OptimizationContext(optimizer_context, nullptr);
  optimizer_context->AddOptimizationContext(optimization_context);

  // Setup the task stack
  auto task_stack = new OptimizerTaskStack();
  optimizer_context->SetTaskPool(task_stack);

  // Create OperatorNode of FILTER1 (FILTER2 (GET A))
  std::vector<std::unique_ptr<OperatorNode>> get_children;
  auto get = std::make_unique<OperatorNode>(
      LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), {}, "tbl1", false),
      std::move(get_children));
  auto get_copy = get->Copy();

  // Build two expressions for filters
  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetDecimal(1.0));
  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto expression_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>{"tbl1"});
  auto expression_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>{"tbl2"});

  std::vector<AnnotatedExpression> filter_pred{expression_2};
  std::vector<std::unique_ptr<OperatorNode>> filter_children;
  filter_children.emplace_back(std::move(get));
  auto filter = std::make_unique<OperatorNode>(LogicalFilter::Make(std::move(filter_pred)), std::move(filter_children));

  std::vector<AnnotatedExpression> root_pred{expression_1};
  std::vector<std::unique_ptr<OperatorNode>> root_children;
  root_children.emplace_back(std::move(filter));
  auto root = std::make_unique<OperatorNode>(LogicalFilter::Make(std::move(root_pred)), std::move(root_children));

  // RecordOperatorNodeIntoGroup
  GroupExpression *gexpr;
  EXPECT_TRUE(optimizer_context->RecordOperatorNodeIntoGroup(common::ManagedPointer(root), &gexpr));
  EXPECT_TRUE(gexpr != nullptr);

  // Add rewrite tasks
  group_id_t root_id = gexpr->GetGroupID();
  task_stack->Push(new TopDownRewrite(root_id, optimization_context, RuleSetName::PREDICATE_PUSH_DOWN));

  // Execute the tasks in stack
  // Two rules will be applied: COMBINE_CONSECUTIVE_FILTER & EMBED_FILTER_INTO_GET
  while (!task_stack->Empty()) {
    auto task = task_stack->Pop();
    task->Execute();
    delete task;
  }

  // Expected OperatorNode: GET A
  auto root_gexpr = optimizer_context->GetMemo().GetGroupByID(root_id)->GetLogicalExpression();
  EXPECT_EQ(root_gexpr->Op().GetType(), OpType::LOGICALGET);
  EXPECT_EQ(root_gexpr->GetChildrenGroupsSize(), 0);
  EXPECT_EQ(root_gexpr->Op().As<LogicalGet>()->GetPredicates().size(), 2);
  std::vector<AnnotatedExpression> preds{expression_1, expression_2};
  EXPECT_EQ(root_gexpr->Op().As<LogicalGet>()->GetPredicates(), preds);

  delete expr_b_1;
  delete expr_b_2;
  delete optimizer_context;
}

// NOLINTNEXTLINE
TEST_F(OptimizerRewriteTest, EmbedFilterIntoGetTest) {
  auto optimizer_context = new OptimizerContext(nullptr);
  auto optimization_context = new OptimizationContext(optimizer_context, nullptr);
  optimizer_context->AddOptimizationContext(optimization_context);

  // Setup the task stack
  auto task_stack = new OptimizerTaskStack();
  optimizer_context->SetTaskPool(task_stack);

  // Create OperatorNode of FILTER (GET A) filter has two predicates
  std::vector<std::unique_ptr<OperatorNode>> get_children;
  auto get = std::make_unique<OperatorNode>(
      LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), {}, "tbl1", false),
      std::move(get_children));
  auto get_copy = get->Copy();

  // Build two expressions for filter
  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetDecimal(1.0));
  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto expression_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>{"tbl1"});
  auto expression_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>{"tbl2"});

  std::vector<AnnotatedExpression> root_pred{expression_1, expression_2};
  std::vector<std::unique_ptr<OperatorNode>> root_children;
  root_children.emplace_back(std::move(get));
  auto root = std::make_unique<OperatorNode>(LogicalFilter::Make(std::move(root_pred)), std::move(root_children));

  // RecordOperatorNodeIntoGroup
  GroupExpression *gexpr;
  EXPECT_TRUE(optimizer_context->RecordOperatorNodeIntoGroup(common::ManagedPointer(root), &gexpr));
  EXPECT_TRUE(gexpr != nullptr);

  // Add rewrite tasks
  group_id_t root_id = gexpr->GetGroupID();
  task_stack->Push(new TopDownRewrite(root_id, optimization_context, RuleSetName::PREDICATE_PUSH_DOWN));

  // Execute the tasks in stack
  // EMBED_FILTER_INTO_GET will be applied
  while (!task_stack->Empty()) {
    auto task = task_stack->Pop();
    task->Execute();
    delete task;
  }

  // Expected OperatorNode: GET A, which has two predicates
  auto root_gexpr = optimizer_context->GetMemo().GetGroupByID(root_id)->GetLogicalExpression();
  EXPECT_EQ(root_gexpr->Op().GetType(), OpType::LOGICALGET);
  EXPECT_EQ(root_gexpr->GetChildrenGroupsSize(), 0);
  EXPECT_EQ(root_gexpr->Op().As<LogicalGet>()->GetPredicates().size(), 2);
  std::vector<AnnotatedExpression> preds{expression_1, expression_2};
  EXPECT_EQ(root_gexpr->Op().As<LogicalGet>()->GetPredicates(), preds);

  delete expr_b_1;
  delete expr_b_2;
  delete optimizer_context;
}

// NOLINTNEXTLINE
TEST_F(OptimizerRewriteTest, MarkJoinGetToInnerJoinTest) {
  auto optimizer_context = new OptimizerContext(nullptr);
  auto optimization_context = new OptimizationContext(optimizer_context, nullptr);
  optimizer_context->AddOptimizationContext(optimization_context);

  // Setup the task stack
  auto task_stack = new OptimizerTaskStack();
  optimizer_context->SetTaskPool(task_stack);

  // Create OperatorNode of (GET A) MARK_JOIN (GET A)
  std::vector<std::unique_ptr<OperatorNode>> children;
  auto left_get = std::make_unique<OperatorNode>(
      LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), {}, "tbl", false),
      std::move(children));

  auto right_get = left_get->Copy();
  EXPECT_EQ(*left_get, *right_get);

  std::vector<std::unique_ptr<OperatorNode>> join_children;
  join_children.emplace_back(std::move(left_get));
  join_children.emplace_back(std::move(right_get));
  auto join = std::make_unique<OperatorNode>(LogicalMarkJoin::Make(), std::move(join_children));

  // RecordOperatorNodeIntoGroup
  GroupExpression *join_gexpr;
  EXPECT_TRUE(optimizer_context->RecordOperatorNodeIntoGroup(common::ManagedPointer(join), &join_gexpr));
  EXPECT_TRUE(join_gexpr != nullptr);

  // Add rewrite tasks
  group_id_t root_id = join_gexpr->GetGroupID();
  task_stack->Push(new BottomUpRewrite(root_id, optimization_context, RuleSetName::UNNEST_SUBQUERY, false));

  // Execute the tasks in stack
  // MARK_JOIN_GET_TO_INNER_JOIN will be applied
  while (!task_stack->Empty()) {
    auto task = task_stack->Pop();
    task->Execute();
    delete task;
  }

  // Expected OperatorNode: (GET A) INNER_JOIN (GET A)
  auto root_gexpr = optimizer_context->GetMemo().GetGroupByID(root_id)->GetLogicalExpression();
  EXPECT_EQ(root_gexpr->Op().GetType(), OpType::LOGICALINNERJOIN);
  EXPECT_EQ(root_gexpr->GetChildrenGroupsSize(), 2);

  auto left_get_gexpr =
      optimizer_context->GetMemo().GetGroupByID(root_gexpr->GetChildGroupId(0))->GetLogicalExpression();
  EXPECT_EQ(left_get_gexpr->Op().GetType(), OpType::LOGICALGET);

  auto right_get_gexpr =
      optimizer_context->GetMemo().GetGroupByID(root_gexpr->GetChildGroupId(1))->GetLogicalExpression();
  EXPECT_EQ(right_get_gexpr->Op().GetType(), OpType::LOGICALGET);

  delete optimizer_context;
}

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
  task_stack->Push(new BottomUpRewrite(root_id, optimization_context, RuleSetName::UNNEST_SUBQUERY, false));

  // Execute the tasks in stack
  // SINGLE_JOIN_GET_TO_INNER_JOIN will be applied
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

  auto agg_gexpr = optimizer_context->GetMemo().GetGroupByID(root_gexpr->GetChildGroupId(1))->GetLogicalExpression();
  EXPECT_EQ(agg_gexpr->Op().GetType(), OpType::LOGICALAGGREGATEANDGROUPBY);
  EXPECT_EQ(root_gexpr->GetChildrenGroupsSize(), 2);

  auto right_get_gexpr =
      optimizer_context->GetMemo().GetGroupByID(agg_gexpr->GetChildGroupId(0))->GetLogicalExpression();
  EXPECT_EQ(right_get_gexpr->Op().GetType(), OpType::LOGICALGET);

  delete optimizer_context;
}

// NOLINTNEXTLINE
TEST_F(OptimizerRewriteTest, DependentJoinGetToInnerJoinTest) {
  auto optimizer_context = new OptimizerContext(nullptr);
  auto optimization_context = new OptimizationContext(optimizer_context, nullptr);
  optimizer_context->AddOptimizationContext(optimization_context);

  // Setup the task stack
  auto task_stack = new OptimizerTaskStack();
  optimizer_context->SetTaskPool(task_stack);

  // Create OperatorNode of (GET A) SINGLE_JOIN (FILTER (AGGREGATE (GET B)))
  std::vector<std::unique_ptr<OperatorNode>> lg_children;
  auto left_get = std::make_unique<OperatorNode>(
      LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), {}, "tbl1", false),
      std::move(lg_children));
  auto lg_copy = left_get->Copy();

  std::vector<std::unique_ptr<OperatorNode>> rg_children;
  auto right_get = std::make_unique<OperatorNode>(
      LogicalGet::Make(catalog::db_oid_t(4), catalog::namespace_oid_t(5), catalog::table_oid_t(6), {}, "tbl2", false),
      std::move(rg_children));

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
  std::vector<AnnotatedExpression> predicates{outer_expression, inner_expression};
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
  // DEPENDENT_JOIN_GET_TO_INNER_JOIN will be applied
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

  auto filter_gexpr = optimizer_context->GetMemo().GetGroupByID(join_gexpr->GetChildGroupId(1))->GetLogicalExpression();
  EXPECT_EQ(filter_gexpr->Op().GetType(), OpType::LOGICALFILTER);
  EXPECT_EQ(filter_gexpr->GetChildrenGroupsSize(), 1);

  auto agg_gexpr = optimizer_context->GetMemo().GetGroupByID(filter_gexpr->GetChildGroupId(0))->GetLogicalExpression();
  EXPECT_EQ(agg_gexpr->Op().GetType(), OpType::LOGICALAGGREGATEANDGROUPBY);
  EXPECT_EQ(agg_gexpr->GetChildrenGroupsSize(), 1);

  auto right_get_gexpr =
      optimizer_context->GetMemo().GetGroupByID(agg_gexpr->GetChildGroupId(0))->GetLogicalExpression();
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

  delete expr_cmp;
  delete optimizer_context;
}

// NOLINTNEXTLINE
TEST_F(OptimizerRewriteTest, PullFilterThroughMarkJoinAndMarkJoinToInnerJoinTest) {
  auto optimizer_context = new OptimizerContext(nullptr);
  auto optimization_context = new OptimizationContext(optimizer_context, nullptr);
  optimizer_context->AddOptimizationContext(optimization_context);

  // Setup the task stack
  auto task_stack = new OptimizerTaskStack();
  optimizer_context->SetTaskPool(task_stack);

  // Create OperatorNode of (GET A) MARK_JOIN (FILTER (GET B))
  std::vector<std::unique_ptr<OperatorNode>> lg_children;
  auto left_get = std::make_unique<OperatorNode>(
      LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), {}, "tbl1", false),
      std::move(lg_children));
  auto lg_copy = left_get->Copy();

  std::vector<std::unique_ptr<OperatorNode>> rg_children;
  auto right_get = std::make_unique<OperatorNode>(
      LogicalGet::Make(catalog::db_oid_t(4), catalog::namespace_oid_t(5), catalog::table_oid_t(6), {}, "tbl2", false),
      std::move(rg_children));

  // Build a COMPARE_EQUAL expression for filter, which contains two children
  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);

  // Two predicates refer to different tables
  auto expression_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>{"tbl1"});
  auto expression_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>{"tbl2"});
  std::vector<AnnotatedExpression> predicates{expression_1, expression_2};
  std::vector<std::unique_ptr<OperatorNode>> filter_children;
  filter_children.emplace_back(std::move(right_get));
  auto filter = std::make_unique<OperatorNode>(LogicalFilter::Make(std::move(predicates)), std::move(filter_children));

  std::vector<std::unique_ptr<OperatorNode>> join_children;
  join_children.emplace_back(std::move(left_get));
  join_children.emplace_back(std::move(filter));
  auto join = std::make_unique<OperatorNode>(LogicalMarkJoin::Make(), std::move(join_children));

  // RecordOperatorNodeIntoGroup
  GroupExpression *original_gexpr;
  EXPECT_TRUE(optimizer_context->RecordOperatorNodeIntoGroup(common::ManagedPointer(join), &original_gexpr));
  EXPECT_TRUE(original_gexpr != nullptr);

  // Add rewrite tasks
  group_id_t root_id = original_gexpr->GetGroupID();
  task_stack->Push(new BottomUpRewrite(root_id, optimization_context, RuleSetName::UNNEST_SUBQUERY, false));

  // Execute the tasks in stack
  // Two rules will be applied: PULL_FILTER_THROUGH_MARK_JOIN and MARK_JOIN_GET_TO_INNER_JOIN
  while (!task_stack->Empty()) {
    auto task = task_stack->Pop();
    task->Execute();
    delete task;
  }

  // Expected OperatorNode: FILTER((GET A) INNER_JOIN (GET B))
  auto root_gexpr = optimizer_context->GetMemo().GetGroupByID(root_id)->GetLogicalExpression();
  EXPECT_EQ(root_gexpr->Op().GetType(), OpType::LOGICALFILTER);
  EXPECT_EQ(root_gexpr->GetChildrenGroupsSize(), 1);
  std::vector<AnnotatedExpression> preds{expression_1, expression_2};
  EXPECT_EQ(root_gexpr->Op().As<LogicalFilter>()->GetPredicates(), preds);

  auto join_gexpr = optimizer_context->GetMemo().GetGroupByID(root_gexpr->GetChildGroupId(0))->GetLogicalExpression();
  EXPECT_EQ(join_gexpr->Op().GetType(), OpType::LOGICALINNERJOIN);
  EXPECT_EQ(join_gexpr->GetChildrenGroupsSize(), 2);

  auto left_get_gexpr =
      optimizer_context->GetMemo().GetGroupByID(join_gexpr->GetChildGroupId(0))->GetLogicalExpression();
  EXPECT_EQ(left_get_gexpr->Op().GetType(), OpType::LOGICALGET);

  auto right_get_gexpr =
      optimizer_context->GetMemo().GetGroupByID(join_gexpr->GetChildGroupId(0))->GetLogicalExpression();
  EXPECT_EQ(right_get_gexpr->Op().GetType(), OpType::LOGICALGET);

  delete expr_b_1;
  delete expr_b_2;
  delete optimizer_context;
}

// NOLINTNEXTLINE
TEST_F(OptimizerRewriteTest, PullFilterThroughAggregationTest) {
  auto optimizer_context = new OptimizerContext(nullptr);
  auto optimization_context = new OptimizationContext(optimizer_context, nullptr);
  optimizer_context->AddOptimizationContext(optimization_context);

  // Setup the task stack
  auto task_stack = new OptimizerTaskStack();
  optimizer_context->SetTaskPool(task_stack);

  // Create OperatorNode of AGGREGATE(FILTER (GET A))
  // The filter contains two predicates.
  // One of them(inner one) refers to the same table as GET while the other(outer one) doesn't.
  std::vector<std::unique_ptr<OperatorNode>> get_children;
  auto get = std::make_unique<OperatorNode>(
      LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), {}, "tbl1", false),
      std::move(get_children));

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
  std::vector<AnnotatedExpression> predicates{outer_expression, inner_expression};
  std::vector<std::unique_ptr<OperatorNode>> filter_children;
  filter_children.emplace_back(std::move(get));
  auto filter = std::make_unique<OperatorNode>(LogicalFilter::Make(std::move(predicates)), std::move(filter_children));

  std::vector<std::unique_ptr<OperatorNode>> agg_children;
  agg_children.emplace_back(std::move(filter));
  auto agg = std::make_unique<OperatorNode>(LogicalAggregateAndGroupBy::Make(), std::move(agg_children));

  // RecordOperatorNodeIntoGroup
  GroupExpression *original_gexpr;
  EXPECT_TRUE(optimizer_context->RecordOperatorNodeIntoGroup(common::ManagedPointer(agg), &original_gexpr));
  EXPECT_TRUE(original_gexpr != nullptr);

  // Add rewrite tasks
  group_id_t root_id = original_gexpr->GetGroupID();
  task_stack->Push(new BottomUpRewrite(root_id, optimization_context, RuleSetName::UNNEST_SUBQUERY, false));

  // Execute the tasks in stack
  // PULL_FILTER_THROUGH_AGGREGATION will be applied
  while (!task_stack->Empty()) {
    auto task = task_stack->Pop();
    task->Execute();
    delete task;
  }

  // Expected OperatorNode: FILTER (AGGREGATE (FILTER (GET A))
  // The upper filter should have the outer_expression as its predicate.
  // The lower filter should have the inner_expression as its predicate.
  auto root_gexpr = optimizer_context->GetMemo().GetGroupByID(root_id)->GetLogicalExpression();
  EXPECT_EQ(root_gexpr->Op().GetType(), OpType::LOGICALFILTER);
  EXPECT_EQ(root_gexpr->GetChildrenGroupsSize(), 1);
  EXPECT_EQ(root_gexpr->Op().As<LogicalFilter>()->GetPredicates().size(), 1);
  EXPECT_EQ(root_gexpr->Op().As<LogicalFilter>()->GetPredicates()[0], outer_expression);

  auto agg_gexpr = optimizer_context->GetMemo().GetGroupByID(root_gexpr->GetChildGroupId(0))->GetLogicalExpression();
  EXPECT_EQ(agg_gexpr->Op().GetType(), OpType::LOGICALAGGREGATEANDGROUPBY);
  EXPECT_EQ(agg_gexpr->GetChildrenGroupsSize(), 1);

  auto filter_gexpr = optimizer_context->GetMemo().GetGroupByID(agg_gexpr->GetChildGroupId(0))->GetLogicalExpression();
  EXPECT_EQ(filter_gexpr->Op().GetType(), OpType::LOGICALFILTER);
  EXPECT_EQ(filter_gexpr->GetChildrenGroupsSize(), 1);
  EXPECT_EQ(filter_gexpr->Op().As<LogicalFilter>()->GetPredicates().size(), 1);
  EXPECT_EQ(filter_gexpr->Op().As<LogicalFilter>()->GetPredicates()[0], inner_expression);

  auto get_gexpr = optimizer_context->GetMemo().GetGroupByID(filter_gexpr->GetChildGroupId(0))->GetLogicalExpression();
  EXPECT_EQ(get_gexpr->Op().GetType(), OpType::LOGICALGET);

  delete expr_cmp;
  delete optimizer_context;
}

}  // namespace terrier::optimizer
