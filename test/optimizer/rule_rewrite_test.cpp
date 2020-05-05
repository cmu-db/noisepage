
#include "gtest/gtest.h"

#include "optimizer/rewriter.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression_defs.h"
#include "test_util/test_harness.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace terrier::optimizer {

class RuleRewriteTests : public TerrierTest {
 public:
  // Creates expression: (A = X) AND (B = Y)
  parser::AbstractExpression *CreateMultiLevelExpression(parser::AbstractExpression *a, parser::AbstractExpression *x,
                                                         parser::AbstractExpression *b, parser::AbstractExpression *y) {
    std::vector<std::unique_ptr<parser::AbstractExpression>> left_children;
    left_children.push_back(a->Copy());
    left_children.push_back(x->Copy());
    std::unique_ptr<parser::AbstractExpression> left_eq =
        std::make_unique<parser::ComparisonExpression>(parser::ExpressionType::COMPARE_EQUAL, std::move(left_children));

    std::vector<std::unique_ptr<parser::AbstractExpression>> right_children;
    right_children.push_back(b->Copy());
    right_children.push_back(y->Copy());
    std::unique_ptr<parser::AbstractExpression> right_eq = std::make_unique<parser::ComparisonExpression>(
        parser::ExpressionType::COMPARE_EQUAL, std::move(right_children));

    std::vector<std::unique_ptr<parser::AbstractExpression>> root_children;
    root_children.push_back(std::move(left_eq));
    root_children.push_back(std::move(right_eq));
    return new parser::ConjunctionExpression(parser::ExpressionType::CONJUNCTION_AND, std::move(root_children));
  }

  parser::ConstantValueExpression *GetConstantExpression(int val) {
    auto value = type::TransientValueFactory::GetInteger(val);
    return new parser::ConstantValueExpression(value);
  }
};

TEST_F(RuleRewriteTests, TransitiveClosureUnableTest) {
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  auto cv1 = GetConstantExpression(1);
  auto *tv_base1 = new parser::ColumnValueExpression("A", "B");
  auto *tv_base2 = new parser::ColumnValueExpression("A", "C");
  auto *tv_base3 = new parser::ColumnValueExpression("A", "D");

  auto *rewriter = new Rewriter(txn_context);

  // Base (A = 1) AND (B = C)
  auto base = CreateMultiLevelExpression(tv_base1, cv1, tv_base2, tv_base3);

  auto expr = rewriter->RewriteExpression(common::ManagedPointer(base));
  std::cout << "Deleting rewriter\n";
  delete rewriter;
  std::cout << "Done\n";
  delete base;

  // Returned expression should not be changed
  EXPECT_TRUE(expr->GetExpressionType() == parser::ExpressionType::CONJUNCTION_AND);
  EXPECT_TRUE(expr->GetChildrenSize() == 2);

  auto left_eq = expr->GetChild(0);
  auto right_eq = expr->GetChild(1);
  EXPECT_TRUE(left_eq->GetExpressionType() == parser::ExpressionType::COMPARE_EQUAL);
  EXPECT_TRUE(right_eq->GetExpressionType() == parser::ExpressionType::COMPARE_EQUAL);
  EXPECT_TRUE(left_eq->GetChildrenSize() == 2);
  EXPECT_TRUE(right_eq->GetChildrenSize() == 2);

  auto ll_tv = left_eq->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  auto lr_cv = left_eq->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
  auto rl_tv = right_eq->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  auto rr_tv = right_eq->GetChild(1).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_TRUE(ll_tv != nullptr && lr_cv != nullptr && rl_tv != nullptr && rr_tv != nullptr);
  EXPECT_TRUE((*lr_cv) == (*cv1));
  EXPECT_TRUE((*ll_tv) == (*tv_base1));
  EXPECT_TRUE((*rl_tv) == (*tv_base2));
  EXPECT_TRUE((*rr_tv) == (*tv_base3));

  txn_manager.Abort(txn_context);

  delete cv1;
  delete tv_base1;
  delete tv_base2;
  delete tv_base3;
  delete expr.Get();
}

/*
TEST_F(RuleRewriteTests, TransitiveClosureRewrite) {
  auto timestamp_manager = transaction::TimestampManager();
  auto *deferred_action_manager = new transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto *buffer_pool = new storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(deferred_action_manager),
      common::ManagedPointer(buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  auto cv1 = GetConstantExpression(1);
  auto *tv_base1 = new parser::ColumnValueExpression("A", "B");
  auto *tv_base2 = new parser::ColumnValueExpression("A", "C");

  auto *rewriter = new Rewriter(txn_context);

  // Base (A = 1) AND (A = B)
  auto base = CreateMultiLevelExpression(tv_base1, cv1, tv_base1, tv_base2);

  auto expr = rewriter->RewriteExpression(common::ManagedPointer(base));
  delete rewriter;
  delete base;

  // Returned expression should not be changed
  EXPECT_TRUE(expr->GetExpressionType() == parser::ExpressionType::CONJUNCTION_AND);
  EXPECT_TRUE(expr->GetChildrenSize() == 2);

  auto left_eq = expr->GetChild(0);
  auto right_eq = expr->GetChild(1);
  EXPECT_TRUE(left_eq->GetExpressionType() == parser::ExpressionType::COMPARE_EQUAL);
  EXPECT_TRUE(right_eq->GetExpressionType() == parser::ExpressionType::COMPARE_EQUAL);
  EXPECT_TRUE(left_eq->GetChildrenSize() == 2);
  EXPECT_TRUE(right_eq->GetChildrenSize() == 2);

  auto ll_tv = left_eq->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  auto lr_cv = left_eq->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
  auto rl_cv = right_eq->GetChild(0).CastManagedPointerTo<parser::ConstantValueExpression>();
  auto rr_tv = right_eq->GetChild(1).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_TRUE(ll_tv != nullptr && lr_cv != nullptr && rl_cv != nullptr && rr_tv != nullptr);
  EXPECT_TRUE((*lr_cv) == (*cv1));
  EXPECT_TRUE((*ll_tv) == (*tv_base1));
  EXPECT_TRUE((*rl_cv) == (*cv1));
  EXPECT_TRUE((*rr_tv) == (*tv_base2));

  delete cv1;
  delete tv_base1;
  delete tv_base2;
  delete expr.Get();

  txn_manager.Abort(txn_context);
}

TEST_F(RuleRewriteTests, TransitiveClosureHalfTrue) {
  auto timestamp_manager = transaction::TimestampManager();
  auto *deferred_action_manager = new transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto *buffer_pool = new storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(deferred_action_manager),
      common::ManagedPointer(buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  auto cv1 = GetConstantExpression(1);
  auto *tv_base1 = new parser::ColumnValueExpression("A", "B");

  auto *rewriter = new Rewriter(txn_context);

  // Base (A = 1) AND (A = B)
  auto base = CreateMultiLevelExpression(tv_base1, cv1, tv_base1, tv_base1);

  auto expr = rewriter->RewriteExpression(common::ManagedPointer(base));
  delete rewriter;
  delete base;

  // Returned expression should not be changed
  EXPECT_TRUE(expr->GetExpressionType() == parser::ExpressionType::COMPARE_EQUAL);
  EXPECT_TRUE(expr->GetChildrenSize() == 2);

  auto ll_tv = expr->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  auto lr_cv = expr->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
  EXPECT_TRUE(ll_tv != nullptr && lr_cv != nullptr);
  EXPECT_TRUE((*lr_cv) == (*cv1));
  EXPECT_TRUE((*ll_tv) == (*tv_base1));

  delete cv1;
  delete tv_base1;
  delete expr.Get();

  txn_manager.Abort(txn_context);
}
*/
}  // namespace terrier::optimizer
