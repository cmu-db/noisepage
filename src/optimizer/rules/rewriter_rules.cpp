
#include "optimizer/rules/rewriter_rules.h"
#include "optimizer/expression_node.h"
#include "optimizer/expression_node_contents.h"
#include "optimizer/optimizer_context.h"
#include "parser/expression/conjunction_expression.h"

namespace terrier::optimizer {

// ==============================================
//
// EquivalentTransform methods
//
// ==============================================
EquivalentTransform::EquivalentTransform(RuleType rule, parser::ExpressionType root) {
  type_ = rule;

  auto *left = new Pattern(parser::ExpressionType::GROUP_MARKER);
  auto *right = new Pattern(parser::ExpressionType::GROUP_MARKER);
  match_pattern_ = new Pattern(root);
  match_pattern_->AddChild(left);
  match_pattern_->AddChild(right);
}

RulePromise EquivalentTransform::Promise(GroupExpression *gexpr) const {
  (void)gexpr;
  return RulePromise::LOGICAL_PROMISE;
}

bool EquivalentTransform::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                OptimizationContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void EquivalentTransform::Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                                    std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                    OptimizationContext *context) const {
  (void)context;
  TERRIER_ASSERT(input->GetChildren().size() == 2, "Equivalent transform pattern should have 2 children");

  // Get children of original, place into copy in reverse order
  std::vector<std::unique_ptr<AbstractOptimizerNode>> new_children;
  auto left = input->GetChildren()[0]->Copy();
  auto right = input->GetChildren()[1]->Copy();
  new_children.push_back(std::move(right));
  new_children.push_back(std::move(left));

  auto type = match_pattern_->GetExpType();
  auto *expr = new parser::ConjunctionExpression(type, {});
  auto *expr_node_contents = new ExpressionNodeContents(common::ManagedPointer<parser::AbstractExpression>(expr));
  expr_node_contents->RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn());
  std::unique_ptr<AbstractOptimizerNode> expr_node =
      std::make_unique<ExpressionNode>(common::ManagedPointer<AbstractOptimizerNodeContents>(expr_node_contents),
                                       std::move(new_children), context->GetOptimizerContext()->GetTxn());

  transformed->push_back(std::move(expr_node));
}

// ==============================================
//
// TransitiveClosureConstantTransform methods
//
// ==============================================
TransitiveClosureConstantTransform::TransitiveClosureConstantTransform() {
  type_ = RuleType::TRANSITIVE_CLOSURE_CONSTANT_TRANSFORM;

  // (T.X == a) AND (T.X == T.Y)
  match_pattern_ = new Pattern(parser::ExpressionType::CONJUNCTION_AND);

  // Left side: (T.X == a)
  auto *l_equals = new Pattern(parser::ExpressionType::COMPARE_EQUAL);
  auto *l_left_child = new Pattern(parser::ExpressionType::COLUMN_VALUE);
  auto *l_right_child = new Pattern(parser::ExpressionType::VALUE_CONSTANT);
  l_equals->AddChild(l_left_child);
  l_equals->AddChild(l_right_child);

  // Right side: (T.X == T.Y)
  auto *r_equals = new Pattern(parser::ExpressionType::COMPARE_EQUAL);
  auto *r_left_child = new Pattern(parser::ExpressionType::COLUMN_VALUE);
  auto *r_right_child = new Pattern(parser::ExpressionType::COLUMN_VALUE);
  r_equals->AddChild(r_left_child);
  r_equals->AddChild(r_right_child);

  match_pattern_->AddChild(l_equals);
  match_pattern_->AddChild(r_equals);
}

RulePromise TransitiveClosureConstantTransform::Promise(GroupExpression *gexpr) const {
  (void)gexpr;
  return RulePromise::LOGICAL_PROMISE;
}

bool TransitiveClosureConstantTransform::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                               OptimizationContext *context) const {
  (void)plan;
  (void)context;
  return true;
}

void TransitiveClosureConstantTransform::Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                                                   std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                                   OptimizationContext *context) const {
  // Asserting guarantees provided by the GroupExprBindingIterator
  // Structure: (A.B = x) AND (A.B = C.D)
  TERRIER_ASSERT(input->GetChildren().size() == 2, "Input should have two children");
  TERRIER_ASSERT(input->Contents()->GetExpType() == parser::ExpressionType::CONJUNCTION_AND,
                 "Root input should be AND");

  auto l_eq = input->GetChildren()[0];
  auto r_eq = input->GetChildren()[1];
  TERRIER_ASSERT(l_eq->GetChildren().size() == 2, "Left child should have 2 children");
  TERRIER_ASSERT(r_eq->GetChildren().size() == 2, "Right child should have 2 children");
  TERRIER_ASSERT(l_eq->Contents()->GetExpType() == parser::ExpressionType::COMPARE_EQUAL, "Left child should be EQUAL");
  TERRIER_ASSERT(r_eq->Contents()->GetExpType() == parser::ExpressionType::COMPARE_EQUAL,
                 "Right child should be EQUAL");

  auto l_tv = l_eq->GetChildren()[0];
  auto l_cv = l_eq->GetChildren()[1];
  TERRIER_ASSERT(l_tv->GetChildren().empty(), "Left EQUAL should have no grandchildren");
  TERRIER_ASSERT(l_cv->GetChildren().empty(), "Left EQUAL should have no grandchildren");
  TERRIER_ASSERT(l_tv->Contents()->GetExpType() == parser::ExpressionType::COLUMN_VALUE,
                 "Left EQUAL left child should be column value");
  TERRIER_ASSERT(l_cv->Contents()->GetExpType() == parser::ExpressionType::VALUE_CONSTANT,
                 "Left EQUAL right child should be constant");

  auto r_tv_l = r_eq->GetChildren()[0];
  auto r_tv_r = r_eq->GetChildren()[1];
  TERRIER_ASSERT(r_tv_l->GetChildren().empty(), "Right EQUAL should have no grandchildren");
  TERRIER_ASSERT(r_tv_r->GetChildren().empty(), "Right EQUAL should have no grandchildren");
  TERRIER_ASSERT(r_tv_l->Contents()->GetExpType() == parser::ExpressionType::COLUMN_VALUE,
                 "Right EQUAL left child should be column value");
  TERRIER_ASSERT(r_tv_r->Contents()->GetExpType() == parser::ExpressionType::COLUMN_VALUE,
                 "Right EQUAL left child should be column value");

  auto l_tv_c = l_tv->Contents().CastManagedPointerTo<ExpressionNodeContents>();
  auto r_tv_l_c = r_tv_l->Contents().CastManagedPointerTo<ExpressionNodeContents>();
  auto r_tv_r_c = r_tv_r->Contents().CastManagedPointerTo<ExpressionNodeContents>();
  TERRIER_ASSERT(l_tv_c != nullptr && r_tv_l_c != nullptr && r_tv_r_c != nullptr, "No contents fields should be null");

  auto l_tv_expr = l_tv_c->GetExpr();
  auto r_tv_l_expr = r_tv_l_c->GetExpr();
  auto r_tv_r_expr = r_tv_r_c->GetExpr();

  // At this stage, we have the arbitrary structure: (A.B = x) AND (C.D = E.F)
  if ((*r_tv_l_expr) == (*r_tv_r_expr)) {
    // Handles case where C.D = E.F, which can rewrite to just A.B = x
    transformed->push_back(l_eq->Copy());
    return;
  }

  if ((*l_tv_expr) != (*r_tv_l_expr) && (*l_tv_expr) != (*r_tv_r_expr)) {
    // We know that A.B != C.D and A.B != E.F, so no optimization possible
    return;
  }

  std::unique_ptr<AbstractOptimizerNode> new_l_eq = l_eq->Copy();
  std::unique_ptr<AbstractOptimizerNode> new_r_eq =
      std::make_unique<ExpressionNode>(r_eq->Contents(), context->GetOptimizerContext()->GetTxn());
  std::unique_ptr<AbstractOptimizerNode> constant_value_copy = l_cv->Copy();

  // At this stage, we have knowledge that C.D != E.F
  if ((*l_tv_expr) == (*r_tv_l_expr)) {
    // At this stage, we have knowledge that A.B = C.D
    new_r_eq->PushChild(l_cv->Copy());
    new_r_eq->PushChild(r_tv_r->Copy());
  } else {
    // At this stage, we have knowledge that A.B = E.F
    new_r_eq->PushChild(r_tv_l->Copy());
    new_r_eq->PushChild(l_cv->Copy());
  }

  // Create new root expression
  std::unique_ptr<AbstractOptimizerNode> transformed_expression =
      std::make_unique<ExpressionNode>(input->Contents(), context->GetOptimizerContext()->GetTxn());
  transformed_expression->PushChild(std::move(new_l_eq));
  transformed_expression->PushChild(std::move(new_r_eq));
  transformed->push_back(std::move(transformed_expression));
}

}  // namespace terrier::optimizer