#include <stack>

#include "optimizer/binding.h"
#include "optimizer/optimizer_defs.h"
#include "optimizer/optimizer_metadata.h"
#include "optimizer/optimizer_task.h"
#include "optimizer/optimizer_task_pool.h"
#include "optimizer/pattern.h"

#include "util/test_harness.h"

namespace terrier::optimizer {

struct OptimizerMetadataTest : public TerrierTest {
  void SetUp() override { TerrierTest::SetUp(); }

  void TearDown() override { TerrierTest::TearDown(); }
};

// NOLINTNEXTLINE
TEST_F(OptimizerMetadataTest, PatternTest) {
  // Creates a Pattern and makes sure everything is set correctly
  auto join = new Pattern(OpType::LOGICALINNERJOIN);
  auto left_child = new Pattern(OpType::LOGICALGET);
  auto right_child = new Pattern(OpType::LOGICALEXTERNALFILEGET);
  join->AddChild(left_child);
  join->AddChild(right_child);

  // Pattern should own its children
  EXPECT_EQ(join->GetChildPatternsSize(), 2);
  EXPECT_EQ(join->Type(), OpType::LOGICALINNERJOIN);

  EXPECT_EQ(join->Children().size(), 2);
  EXPECT_EQ(join->Children()[0]->GetChildPatternsSize(), 0);
  EXPECT_EQ(join->Children()[1]->GetChildPatternsSize(), 0);

  EXPECT_EQ(join->Children()[0]->Type(), OpType::LOGICALGET);
  EXPECT_EQ(join->Children()[1]->Type(), OpType::LOGICALEXTERNALFILEGET);
  delete join;
}

// NOLINTNEXTLINE
TEST_F(OptimizerMetadataTest, OptimizerTaskStackTest) {
  auto task_stack = new OptimizerTaskStack();

  std::stack<OptimizerTask *> track_stack;
  for (size_t i = 0; i < 5; i++) {
    auto task = new OptimizeGroup(nullptr, nullptr);
    track_stack.push(task);
    task_stack->Push(task);
  }

  EXPECT_TRUE(!task_stack->Empty());
  while (!task_stack->Empty()) {
    auto *task_stack_pop = task_stack->Pop();
    auto *track_pop = track_stack.top();
    track_stack.pop();
    EXPECT_EQ(task_stack_pop, track_pop);

    delete task_stack_pop;
  }

  EXPECT_TRUE(track_stack.empty());
  delete task_stack;
}

// NOLINTNEXTLINE
TEST_F(OptimizerMetadataTest, OptimizerTaskStackRemainTest) {
  auto task_stack = new OptimizerTaskStack();

  for (size_t i = 0; i < 5; i++) {
    auto task = new OptimizeGroup(nullptr, nullptr);
    task_stack->Push(task);
  }

  // Shouldn't leak memory!
  EXPECT_TRUE(!task_stack->Empty());
  delete task_stack;
}

// NOLINTNEXTLINE
TEST_F(OptimizerMetadataTest, OptimizerMetadataTaskStackTest) {
  auto metadata = OptimizerMetadata(nullptr);

  auto task_stack = new OptimizerTaskStack();
  task_stack->Push(new OptimizeGroup(nullptr, nullptr));
  metadata.SetTaskPool(task_stack);

  auto *pushed = new OptimizeGroup(nullptr, nullptr);
  metadata.PushTask(pushed);
  EXPECT_EQ(task_stack->Pop(), pushed);
  EXPECT_TRUE(!task_stack->Empty());
  delete pushed;
}

// NOLINTNEXTLINE
TEST_F(OptimizerMetadataTest, OptimizerMetadataTaskStackNullptrTest) {
  auto metadata = OptimizerMetadata(nullptr);
  metadata.SetTaskPool(nullptr);

  auto task_stack = new OptimizerTaskStack();
  task_stack->Push(new OptimizeGroup(nullptr, nullptr));
  metadata.SetTaskPool(task_stack);

  // This should clean up memory
  metadata.SetTaskPool(nullptr);
}

// NOLINTNEXTLINE
TEST_F(OptimizerMetadataTest, RecordTransformedExpressionNoDuplicate) {
  auto metadata = OptimizerMetadata(nullptr);

  // Create OperatorExpression of DISTINCT <= GET
  auto *get = new OperatorExpression(
      LogicalGet::make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), {}, "tbl", false),
      {});
  auto *distinct = new OperatorExpression(LogicalDistinct::make(), {get});

  // RecordTransformedExpression
  GroupExpression *gexpr;
  EXPECT_TRUE(metadata.RecordTransformedExpression(distinct, &gexpr));
  EXPECT_TRUE(gexpr != nullptr);

  EXPECT_EQ(gexpr->Op(), distinct->GetOp());
  EXPECT_EQ(gexpr->GetChildGroupIDs().size(), 1);

  auto child_group = gexpr->GetChildGroupId(0);
  auto group = metadata.GetMemo().GetGroupByID(child_group);
  EXPECT_EQ(group->GetLogicalExpressions().size(), 1);

  auto child_gexpr = group->GetLogicalExpressions()[0];
  EXPECT_EQ(child_gexpr->Op(), get->GetOp());
  EXPECT_EQ(child_gexpr->GetChildGroupIDs().size(), 0);

  delete distinct;
}

// NOLINTNEXTLINE
TEST_F(OptimizerMetadataTest, RecordTransformedExpressionDuplicateSingleLayer) {
  auto metadata = OptimizerMetadata(nullptr);

  // Create OperatorExpression of JOIN <= (GET A, GET A)
  auto *leftGet = new OperatorExpression(
      LogicalGet::make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), {}, "tbl", false),
      {});
  auto *rightGet = leftGet->Copy();
  EXPECT_EQ(*leftGet, *rightGet);
  auto *join = new OperatorExpression(LogicalInnerJoin::make(), {leftGet, rightGet});

  // RecordTransformedExpression
  GroupExpression *joinGexpr;
  EXPECT_TRUE(metadata.RecordTransformedExpression(join, &joinGexpr));
  EXPECT_TRUE(joinGexpr != nullptr);

  EXPECT_EQ(joinGexpr->Op(), join->GetOp());
  EXPECT_EQ(joinGexpr->GetChildGroupIDs().size(), 2);
  EXPECT_EQ(joinGexpr->GetChildGroupId(0), joinGexpr->GetChildGroupId(1));

  auto child = joinGexpr->GetChildGroupId(0);
  auto group = metadata.GetMemo().GetGroupByID(child);
  EXPECT_EQ(group->GetLogicalExpressions().size(), 1);

  auto child_gexpr = group->GetLogicalExpressions()[0];
  EXPECT_EQ(child_gexpr->Op(), leftGet->GetOp());
  EXPECT_EQ(child_gexpr->Op(), rightGet->GetOp());
  EXPECT_EQ(child_gexpr->GetChildGroupIDs().size(), 0);

  delete join;
}

// NOLINTNEXTLINE
TEST_F(OptimizerMetadataTest, RecordTransformedExpressionDuplicateMultiLayer) {
  auto metadata = OptimizerMetadata(nullptr);

  // Create OperatorExpression (A JOIN B) JOIN (A JOIN B)
  auto *leftGet = new OperatorExpression(
      LogicalGet::make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), {}, "tbl", false),
      {});
  auto *rightGet = leftGet->Copy();
  EXPECT_EQ(*leftGet, *rightGet);
  auto *leftJoin = new OperatorExpression(LogicalInnerJoin::make(), {leftGet, rightGet});
  auto *rightJoin = leftJoin->Copy();
  EXPECT_EQ(*leftJoin, *rightJoin);
  auto *join = new OperatorExpression(LogicalInnerJoin::make(), {leftJoin, rightJoin});

  // RecordTransformedExpression
  GroupExpression *joinGexpr;
  EXPECT_TRUE(metadata.RecordTransformedExpression(join, &joinGexpr));
  EXPECT_TRUE(joinGexpr != nullptr);

  EXPECT_EQ(joinGexpr->Op(), join->GetOp());
  EXPECT_EQ(joinGexpr->GetChildGroupIDs().size(), 2);
  EXPECT_EQ(joinGexpr->GetChildGroupId(0), joinGexpr->GetChildGroupId(1));

  auto join_child = joinGexpr->GetChildGroupId(0);
  auto join_group = metadata.GetMemo().GetGroupByID(join_child);
  EXPECT_EQ(join_group->GetLogicalExpressions().size(), 1);

  auto join_gexpr = join_group->GetLogicalExpressions()[0];
  EXPECT_EQ(join_gexpr->Op(), leftJoin->GetOp());
  EXPECT_EQ(join_gexpr->Op(), rightJoin->GetOp());
  EXPECT_EQ(join_gexpr->GetChildGroupIDs().size(), 2);
  EXPECT_EQ(join_gexpr->GetChildGroupId(0), join_gexpr->GetChildGroupId(1));

  auto child = join_gexpr->GetChildGroupId(0);
  auto child_group = metadata.GetMemo().GetGroupByID(child);
  EXPECT_EQ(child_group->GetLogicalExpressions().size(), 1);

  auto child_gexpr = child_group->GetLogicalExpressions()[0];
  EXPECT_EQ(child_gexpr->Op(), leftGet->GetOp());
  EXPECT_EQ(child_gexpr->Op(), rightGet->GetOp());
  EXPECT_EQ(child_gexpr->GetChildGroupIDs().size(), 0);

  delete join;
}

// NOLINTNEXTLINE
TEST_F(OptimizerMetadataTest, RecordTransformedExpressionDuplicate) {
  auto metadata = OptimizerMetadata(nullptr);

  auto *tbl_free = new OperatorExpression(TableFreeScan::make(), {});

  GroupExpression *tbl_free_gexpr;
  EXPECT_TRUE(metadata.RecordTransformedExpression(tbl_free, &tbl_free_gexpr));
  EXPECT_TRUE(tbl_free_gexpr != nullptr);

  // Duplicate should return false
  GroupExpression *dup_free_gexpr;
  EXPECT_TRUE(!metadata.RecordTransformedExpression(tbl_free, &dup_free_gexpr));
  EXPECT_TRUE(dup_free_gexpr != nullptr);
  EXPECT_EQ(tbl_free_gexpr, dup_free_gexpr);

  delete tbl_free;
}

// NOLINTNEXTLINE
TEST_F(OptimizerMetadataTest, SimpleBindingTest) {
  auto metadata = OptimizerMetadata(nullptr);

  auto *leftGet = new OperatorExpression(
      LogicalGet::make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), {}, "tbl", false),
      {});
  auto *rightGet = leftGet->Copy();
  EXPECT_EQ(*leftGet, *rightGet);
  auto *join = new OperatorExpression(LogicalInnerJoin::make(), {leftGet, rightGet});

  GroupExpression *gexpr = nullptr;
  EXPECT_TRUE(metadata.RecordTransformedExpression(join, &gexpr));
  EXPECT_TRUE(gexpr != nullptr);

  auto *pattern = new Pattern(OpType::LOGICALINNERJOIN);
  pattern->AddChild(new Pattern(OpType::LOGICALGET));
  pattern->AddChild(new Pattern(OpType::LOGICALGET));

  auto *binding_iterator = new GroupExprBindingIterator(metadata.GetMemo(), gexpr, pattern);
  EXPECT_TRUE(binding_iterator->HasNext());

  auto *binding = binding_iterator->Next();
  EXPECT_EQ(*binding, *join);
  EXPECT_TRUE(!binding_iterator->HasNext());

  delete binding;
  delete binding_iterator;
  delete pattern;
  delete join;
}

// NOLINTNEXTLINE
TEST_F(OptimizerMetadataTest, SingleWildcardTest) {
  auto metadata = OptimizerMetadata(nullptr);

  auto *leftGet = new OperatorExpression(
      LogicalGet::make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), {}, "tbl", false),
      {});
  auto *rightGet = leftGet->Copy();
  EXPECT_EQ(*leftGet, *rightGet);
  auto *join = new OperatorExpression(LogicalInnerJoin::make(), {leftGet, rightGet});

  GroupExpression *gexpr = nullptr;
  EXPECT_TRUE(metadata.RecordTransformedExpression(join, &gexpr));
  EXPECT_TRUE(gexpr != nullptr);

  auto *pattern = new Pattern(OpType::LOGICALINNERJOIN);
  pattern->AddChild(new Pattern(OpType::LEAF));
  pattern->AddChild(new Pattern(OpType::LEAF));

  auto *binding_iterator = new GroupExprBindingIterator(metadata.GetMemo(), gexpr, pattern);
  EXPECT_TRUE(binding_iterator->HasNext());

  auto *binding = binding_iterator->Next();
  EXPECT_EQ(binding->GetOp(), join->GetOp());
  EXPECT_EQ(binding->GetChildren().size(), 2);

  auto *left = binding->GetChildren()[0];
  auto *right = binding->GetChildren()[1];
  EXPECT_TRUE(*left == *right);

  auto leaf = binding->GetChildren()[0]->GetOp().As<LeafOperator>();
  EXPECT_TRUE(leaf != nullptr);
  EXPECT_EQ(leaf->GetOriginGroup(), gexpr->GetChildGroupId(0));

  EXPECT_TRUE(!binding_iterator->HasNext());

  delete binding;
  delete binding_iterator;
  delete pattern;
  delete join;
}

}  // namespace terrier::optimizer
