
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

// ========================================================================== //
// Utility methods for tests
// ========================================================================== //

class RuleRewriteTests : public TerrierTest {
 public:
  // Creates expression: (A = X) AND (B = Y)
  /**
   * Util function for rewriter tests. Creates a multi-level expression of the form (A [c1] X) AND (B [c2] Y).
   * @param a, b, x, y The four sub-expressions (corresponding to their positions above)
   * @param c1, c2 the two comparison types (c1 for a and x, c2 for b and y)
   * @return the multi-level expression
   */
  static parser::AbstractExpression *CreateMultiLevelExpression(parser::AbstractExpression *a,
                                                                parser::AbstractExpression *x,
                                                                parser::AbstractExpression *b,
                                                                parser::AbstractExpression *y,
                                                                parser::ExpressionType c1, parser::ExpressionType c2) {
    std::vector<std::unique_ptr<parser::AbstractExpression>> left_children;
    left_children.push_back(a->Copy());
    left_children.push_back(x->Copy());
    std::unique_ptr<parser::AbstractExpression> left_eq =
        std::make_unique<parser::ComparisonExpression>(c1, std::move(left_children));

    std::vector<std::unique_ptr<parser::AbstractExpression>> right_children;
    right_children.push_back(b->Copy());
    right_children.push_back(y->Copy());
    std::unique_ptr<parser::AbstractExpression> right_eq =
        std::make_unique<parser::ComparisonExpression>(c2, std::move(right_children));

    std::vector<std::unique_ptr<parser::AbstractExpression>> root_children;
    root_children.push_back(std::move(left_eq));
    root_children.push_back(std::move(right_eq));
    return new parser::ConjunctionExpression(parser::ExpressionType::CONJUNCTION_AND, std::move(root_children));
  }

  /**
   * Util function for rewriter tests. Creates an integer constant expression with the provided value.
   * @param val the integer value of the expression
   * @return the constant expression
   */
  static parser::ConstantValueExpression *GetConstantExpression(int val) {
    auto value = type::TransientValueFactory::GetInteger(val);
    return new parser::ConstantValueExpression(value);
  }

  /**
   * Util function for rewriter tests. Creates an boolean constant expression with the provided value.
   * @param val the boolean value of the expression
   * @return the constant expression
   */
  static parser::ConstantValueExpression *GetConstantExpression(bool val) {
    auto value = type::TransientValueFactory::GetBoolean(val);
    return new parser::ConstantValueExpression(value);
  }
};

// ========================================================================== //
// TransitiveClosureConstantTransform tests
// ========================================================================== //

TEST_F(RuleRewriteTests, TransitiveClosureUnableTest) {
  /*
   * Transitive Closure test -- for when the match pattern matches but the expression cannot be rewritten
   *   Input expression: (A.B = 1) AND (A.C = A.D)
   *   Expected output: same as input (shouldn't be rewritten)
   */
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  auto *cv1 = GetConstantExpression(1);
  auto *tv_base1 = new parser::ColumnValueExpression("A", "B");
  auto *tv_base2 = new parser::ColumnValueExpression("A", "C");
  auto *tv_base3 = new parser::ColumnValueExpression("A", "D");

  auto *rewriter = new Rewriter(txn_context);

  // Base (A = 1) AND (B = C)
  auto *base = CreateMultiLevelExpression(tv_base1, cv1, tv_base2, tv_base3, parser::ExpressionType::COMPARE_EQUAL,
                                          parser::ExpressionType::COMPARE_EQUAL);

  auto expr = rewriter->RewriteExpression(common::ManagedPointer(base));
  delete rewriter;
  delete base;

  // Returned expression should not be changed
  EXPECT_EQ(parser::ExpressionType::CONJUNCTION_AND, expr->GetExpressionType());
  EXPECT_EQ(2, expr->GetChildrenSize());

  auto left_eq = expr->GetChild(0);
  auto right_eq = expr->GetChild(1);
  EXPECT_EQ(parser::ExpressionType::COMPARE_EQUAL, left_eq->GetExpressionType());
  EXPECT_EQ(parser::ExpressionType::COMPARE_EQUAL, right_eq->GetExpressionType());
  EXPECT_EQ(2, left_eq->GetChildrenSize());
  EXPECT_EQ(2, right_eq->GetChildrenSize());

  auto ll_tv = left_eq->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  auto lr_cv = left_eq->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
  auto rl_tv = right_eq->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  auto rr_tv = right_eq->GetChild(1).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_TRUE(ll_tv != nullptr && lr_cv != nullptr && rl_tv != nullptr && rr_tv != nullptr);
  EXPECT_EQ((*lr_cv), (*cv1));
  EXPECT_EQ((*ll_tv), (*tv_base1));
  EXPECT_EQ((*rl_tv), (*tv_base2));
  EXPECT_EQ((*rr_tv), (*tv_base3));

  txn_manager.Abort(txn_context);
  delete txn_context;

  delete cv1;
  delete tv_base1;
  delete tv_base2;
  delete tv_base3;
}

TEST_F(RuleRewriteTests, TransitiveClosureRewrite) {
  /*
   * Transitive Closure Test -- full rewrite
   *   Input expression: (A.B = 1) AND (A.B = A.C)
   *   Expected output: (A.B = 1) AND (1 = A.C)
   */
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  parser::AbstractExpression *cv1 = GetConstantExpression(1);
  parser::AbstractExpression *tv_base1 = new parser::ColumnValueExpression("A", "B");
  parser::AbstractExpression *tv_base2 = new parser::ColumnValueExpression("A", "C");

  auto *rewriter = new Rewriter(txn_context);

  // Base (B = 1) AND (B = C)
  auto *base = CreateMultiLevelExpression(tv_base1, cv1, tv_base1, tv_base2, parser::ExpressionType::COMPARE_EQUAL,
                                          parser::ExpressionType::COMPARE_EQUAL);

  auto expr = rewriter->RewriteExpression(common::ManagedPointer(base));
  delete rewriter;
  delete base;

  // Returned expression should not be changed
  EXPECT_EQ(parser::ExpressionType::CONJUNCTION_AND, expr->GetExpressionType());
  EXPECT_EQ(2, expr->GetChildrenSize());

  auto left_eq = expr->GetChild(0);
  auto right_eq = expr->GetChild(1);
  EXPECT_EQ(parser::ExpressionType::COMPARE_EQUAL, left_eq->GetExpressionType());
  EXPECT_EQ(parser::ExpressionType::COMPARE_EQUAL, right_eq->GetExpressionType());
  EXPECT_EQ(2, left_eq->GetChildrenSize());
  EXPECT_EQ(2, right_eq->GetChildrenSize());

  auto ll_tv = left_eq->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  auto lr_cv = left_eq->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
  EXPECT_EQ(parser::ExpressionType::VALUE_CONSTANT, right_eq->GetChild(0)->GetExpressionType());
  auto rl_cv = right_eq->GetChild(0).CastManagedPointerTo<parser::ConstantValueExpression>();
  auto rr_tv = right_eq->GetChild(1).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_TRUE(ll_tv != nullptr && lr_cv != nullptr && rl_cv != nullptr && rr_tv != nullptr);
  EXPECT_EQ((*lr_cv), (*cv1));
  EXPECT_EQ((*ll_tv), (*tv_base1));
  EXPECT_EQ((*rl_cv), (*cv1));
  EXPECT_EQ((*rr_tv), (*tv_base2));

  delete cv1;
  delete tv_base1;
  delete tv_base2;

  txn_manager.Abort(txn_context);
  delete txn_context;
}

TEST_F(RuleRewriteTests, TransitiveClosureFlippedRewrite) {
  /*
   * Transitive Closure Test -- full rewrite with flipped ordering (uses equivalent transform)
   *   Input expression: (A.B = A.C) AND (A.B = 1)
   *   Expected output: (A.B = 1) AND (1 = A.C)
   */
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  parser::AbstractExpression *cv1 = GetConstantExpression(1);
  parser::AbstractExpression *tv_base1 = new parser::ColumnValueExpression("A", "B");
  parser::AbstractExpression *tv_base2 = new parser::ColumnValueExpression("A", "C");

  auto *rewriter = new Rewriter(txn_context);

  // Base (B = C) AND (B = 1)
  auto *base = CreateMultiLevelExpression(tv_base1, tv_base2, tv_base1, cv1, parser::ExpressionType::COMPARE_EQUAL,
                                          parser::ExpressionType::COMPARE_EQUAL);

  auto expr = rewriter->RewriteExpression(common::ManagedPointer(base));
  delete rewriter;
  delete base;

  // Returned expression should be (B = 1) AND (1 = C)
  //   (equivalent transform switches around order of clauses, then transitive closure rule is applied)
  EXPECT_EQ(parser::ExpressionType::CONJUNCTION_AND, expr->GetExpressionType());
  EXPECT_EQ(2, expr->GetChildrenSize());

  auto left_eq = expr->GetChild(0);
  auto right_eq = expr->GetChild(1);
  EXPECT_EQ(parser::ExpressionType::COMPARE_EQUAL, left_eq->GetExpressionType());
  EXPECT_EQ(parser::ExpressionType::COMPARE_EQUAL, right_eq->GetExpressionType());
  EXPECT_EQ(2, left_eq->GetChildrenSize());
  EXPECT_EQ(2, right_eq->GetChildrenSize());

  EXPECT_EQ(parser::ExpressionType::COLUMN_VALUE, left_eq->GetChild(0)->GetExpressionType());
  EXPECT_EQ(parser::ExpressionType::VALUE_CONSTANT, left_eq->GetChild(1)->GetExpressionType());
  EXPECT_EQ(parser::ExpressionType::VALUE_CONSTANT, right_eq->GetChild(0)->GetExpressionType());
  EXPECT_EQ(parser::ExpressionType::COLUMN_VALUE, right_eq->GetChild(1)->GetExpressionType());

  auto ll_tv = left_eq->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  auto lr_cv = left_eq->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
  auto rl_cv = right_eq->GetChild(0).CastManagedPointerTo<parser::ConstantValueExpression>();
  auto rr_tv = right_eq->GetChild(1).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_TRUE(ll_tv != nullptr && lr_cv != nullptr && rl_cv != nullptr && rr_tv != nullptr);
  EXPECT_EQ((*lr_cv), (*cv1));
  EXPECT_EQ((*ll_tv), (*tv_base1));
  EXPECT_EQ((*rl_cv), (*cv1));
  EXPECT_EQ((*rr_tv), (*tv_base2));

  delete cv1;
  delete tv_base1;
  delete tv_base2;

  txn_manager.Abort(txn_context);
  delete txn_context;
}

TEST_F(RuleRewriteTests, TransitiveClosureHalfTrue) {
  /*
   * Transitive Closure Test -- for when match pattern matches but second clause is unnecessary
   *   Input expression: (A.B = 1) AND (A.B = A.B)
   *   Expected output: (A.B = 1)
   */
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  auto *cv1 = GetConstantExpression(1);
  auto *tv_base1 = new parser::ColumnValueExpression("A", "B");

  auto *rewriter = new Rewriter(txn_context);

  // Base (B = 1) AND (B = B)
  auto *base = CreateMultiLevelExpression(tv_base1, cv1, tv_base1, tv_base1, parser::ExpressionType::COMPARE_EQUAL,
                                          parser::ExpressionType::COMPARE_EQUAL);

  auto expr = rewriter->RewriteExpression(common::ManagedPointer(base));
  delete rewriter;
  delete base;

  // Returned expression should only have left branch
  EXPECT_EQ(parser::ExpressionType::COMPARE_EQUAL, expr->GetExpressionType());
  EXPECT_EQ(2, expr->GetChildrenSize());

  auto ll_tv = expr->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  auto lr_cv = expr->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
  EXPECT_TRUE(ll_tv != nullptr && lr_cv != nullptr);
  EXPECT_EQ((*cv1), (*lr_cv));
  EXPECT_EQ((*tv_base1), (*ll_tv));

  delete cv1;
  delete tv_base1;

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// ========================================================================== //
// ComparisonIntersection tests
// ========================================================================== //
TEST_F(RuleRewriteTests, ComparisonIntersectionEmptyIntersection) {
  /*
   * Comparison Intersection test -- check that comparisons with empty intersections get rewritten to FALSE
   */
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  auto *column_ref_b = new parser::ColumnValueExpression("A", "B");
  auto *column_ref_c = new parser::ColumnValueExpression("A", "C");

  auto *rewriter = new Rewriter(txn_context);

  // Expressions that all get rewritten to FALSE due to empty intersection
  std::vector<parser::AbstractExpression *> empty_rewrite_expressions = {
      // (B < C) AND (B > C)
      CreateMultiLevelExpression(column_ref_b, column_ref_c, column_ref_b, column_ref_c,
                                 parser::ExpressionType::COMPARE_LESS_THAN,
                                 parser::ExpressionType::COMPARE_GREATER_THAN),
      // (B > C) AND (B = C)
      CreateMultiLevelExpression(column_ref_b, column_ref_c, column_ref_b, column_ref_c,
                                 parser::ExpressionType::COMPARE_GREATER_THAN, parser::ExpressionType::COMPARE_EQUAL),
      // (B < C) AND (B = C)
      CreateMultiLevelExpression(column_ref_b, column_ref_c, column_ref_b, column_ref_c,
                                 parser::ExpressionType::COMPARE_LESS_THAN, parser::ExpressionType::COMPARE_EQUAL),
      // (B > C) AND (B <= C)
      CreateMultiLevelExpression(column_ref_b, column_ref_c, column_ref_b, column_ref_c,
                                 parser::ExpressionType::COMPARE_GREATER_THAN,
                                 parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO),
      // (B < C) AND (B >= C)
      CreateMultiLevelExpression(column_ref_b, column_ref_c, column_ref_b, column_ref_c,
                                 parser::ExpressionType::COMPARE_LESS_THAN,
                                 parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO),
  };

  for (auto *base : empty_rewrite_expressions) {
    auto expr = rewriter->RewriteExpression(common::ManagedPointer(base));
    delete base;

    // Result should be single boolean constant expression (false)
    TERRIER_ASSERT(expr->GetExpressionType() == parser::ExpressionType::VALUE_CONSTANT,
                   "Rewritten expression should be a boolean constant value");
    TERRIER_ASSERT(expr->GetChildrenSize() == 0, "Rewritten expression should have no children");
  }
  delete rewriter;

  txn_manager.Abort(txn_context);
  delete txn_context;
  delete column_ref_b;
  delete column_ref_c;
}

TEST_F(RuleRewriteTests, ComparisonIntersectionEqualIntersection) {
  /*
   * Comparison Intersection test -- check that comparisons that yield intersections of the form B = C do so properly
   */
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  auto *column_ref_b = new parser::ColumnValueExpression("A", "B");
  auto *column_ref_c = new parser::ColumnValueExpression("A", "C");

  auto *rewriter = new Rewriter(txn_context);

  // Expressions that all get rewritten to FALSE due to empty intersection
  std::vector<parser::AbstractExpression *> empty_rewrite_expressions = {
      // (B < C) AND (B > C)
      CreateMultiLevelExpression(column_ref_b, column_ref_c, column_ref_b, column_ref_c,
                                 parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO,
                                 parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO)};

  for (auto *base : empty_rewrite_expressions) {
    auto expr = rewriter->RewriteExpression(common::ManagedPointer(base));
    delete base;

    // Result should be single boolean constant expression (false)
    TERRIER_ASSERT(expr->GetExpressionType() == parser::ExpressionType::COMPARE_EQUAL,
                   "Rewritten expression should be an EQUAL");
    TERRIER_ASSERT(expr->GetChildrenSize() == 2, "Rewritten expression should have 2 children");

    auto left_child = expr->GetChildren()[0];
    auto right_child = expr->GetChildren()[1];
    TERRIER_ASSERT(*left_child == *column_ref_b, "Left child should be A.B");
    TERRIER_ASSERT(*right_child == *column_ref_c, "Right child should be A.C");
  }
  delete rewriter;

  txn_manager.Abort(txn_context);
  delete txn_context;
  delete column_ref_b;
  delete column_ref_c;
}

// ========================================================================== //
// Mix tests
// ========================================================================== //
TEST_F(RuleRewriteTests, TransitiveClosureComparisonIntersectionMix) {
  /*
   * Transitive Closure Test -- full rewrite
   *   Input expression: (A.B = 1) AND ((A.B <= A.C) AND (A.B >= A.C))
   *   Expected output: (A.B = 1) AND (1 = A.C)
   */
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  parser::AbstractExpression *cv1 = GetConstantExpression(1);
  parser::AbstractExpression *tv_base1 = new parser::ColumnValueExpression("A", "B");
  parser::AbstractExpression *tv_base2 = new parser::ColumnValueExpression("A", "C");

  auto *left_child = CreateMultiLevelExpression(tv_base1, tv_base2, tv_base1, tv_base2,
                                                parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO,
                                                parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO);

  std::vector<std::unique_ptr<parser::AbstractExpression>> right_grandchildren;
  right_grandchildren.push_back(tv_base1->Copy());
  right_grandchildren.push_back(cv1->Copy());
  auto *right_child =
      new parser::ComparisonExpression(parser::ExpressionType::COMPARE_EQUAL, std::move(right_grandchildren));

  auto *rewriter = new Rewriter(txn_context);

  // Base (B = 1) AND (B = C)
  std::vector<std::unique_ptr<parser::AbstractExpression>> base_children;
  base_children.push_back(left_child->Copy());
  base_children.push_back(right_child->Copy());
  auto *base = new parser::ConjunctionExpression(parser::ExpressionType::CONJUNCTION_AND, std::move(base_children));

  auto expr = rewriter->RewriteExpression(common::ManagedPointer<parser::AbstractExpression>(base));
  delete rewriter;
  delete base;
  delete right_child;
  delete left_child;

  // Returned expression should not be changed
  EXPECT_EQ(parser::ExpressionType::CONJUNCTION_AND, expr->GetExpressionType());
  EXPECT_EQ(2, expr->GetChildrenSize());

  auto left_eq = expr->GetChild(0);
  auto right_eq = expr->GetChild(1);
  EXPECT_EQ(parser::ExpressionType::COMPARE_EQUAL, left_eq->GetExpressionType());
  EXPECT_EQ(parser::ExpressionType::COMPARE_EQUAL, right_eq->GetExpressionType());
  EXPECT_EQ(2, left_eq->GetChildrenSize());
  EXPECT_EQ(2, right_eq->GetChildrenSize());

  auto ll_tv = left_eq->GetChild(0).CastManagedPointerTo<parser::ColumnValueExpression>();
  auto lr_cv = left_eq->GetChild(1).CastManagedPointerTo<parser::ConstantValueExpression>();
  EXPECT_EQ(parser::ExpressionType::VALUE_CONSTANT, right_eq->GetChild(0)->GetExpressionType());
  auto rl_cv = right_eq->GetChild(0).CastManagedPointerTo<parser::ConstantValueExpression>();
  auto rr_tv = right_eq->GetChild(1).CastManagedPointerTo<parser::ColumnValueExpression>();
  EXPECT_TRUE(ll_tv != nullptr && lr_cv != nullptr && rl_cv != nullptr && rr_tv != nullptr);
  EXPECT_EQ((*lr_cv), (*cv1));
  EXPECT_EQ((*ll_tv), (*tv_base1));
  EXPECT_EQ((*rl_cv), (*cv1));
  EXPECT_EQ((*rr_tv), (*tv_base2));

  delete cv1;
  delete tv_base1;
  delete tv_base2;

  txn_manager.Abort(txn_context);
  delete txn_context;
}

}  // namespace terrier::optimizer
