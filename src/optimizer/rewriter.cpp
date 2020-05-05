//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rewriter.cpp
//
// Identification: src/optimizer/rewriter.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/rewriter.h"

#include <memory>
#include "loggers/optimizer_logger.h"
#include "optimizer/abstract_optimizer_node.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/expression_node.h"
#include "optimizer/expression_node_contents.h"
#include "optimizer/optimization_context.h"
#include "optimizer/optimizer_context.h"
#include "optimizer/optimizer_task_pool.h"
#include "optimizer/rule.h"
#include "parser/expression/abstract_expression.h"
#include "transaction/transaction_context.h"

namespace terrier::optimizer {

Rewriter::Rewriter(transaction::TransactionContext *txn) {
  context_ = new OptimizerContext(nullptr);
  SetTxn(txn);
}

void Rewriter::Reset(transaction::TransactionContext *txn) {
  std::cout << " deleting context\n";
  delete context_;
  std::cout << " done\n";
  context_ = new OptimizerContext(nullptr);
  SetTxn(txn);
}

void Rewriter::RewriteLoop(group_id_t root_group_id) {
  std::unique_ptr<OptimizationContext> root_context = std::make_unique<OptimizationContext>(context_, nullptr);
  auto task_stack = std::make_unique<OptimizerTaskStack>();
  context_->SetTaskPool(task_stack.get());

  // Rewrite using all rules (which will be applied based on priority)
  task_stack->Push(new BottomUpRewrite(root_group_id, root_context.get(), RuleSetName::GENERIC_RULES, false));

  // Generate equivalences first
  //auto *equiv_task = new TopDownRewrite(root_group_id, root_context.get(), RuleSetName::EQUIVALENT_TRANSFORM);
  //equiv_task->SetReplaceOnTransform(false);  // generate equivalent
  //task_stack->Push(equiv_task);

  // Iterate through the task stack
  while (!task_stack->Empty()) {
    std::cout << "task!\n";
    auto task = task_stack->Pop();
    std::cout << "task popped\n";
    task->Execute();
    std::cout << "task executed\n";
  }
  std::cout << "done with rewrite loop\n";
}

common::ManagedPointer<parser::AbstractExpression> Rewriter::RebuildExpression(group_id_t root) {
  auto cur_group = context_->GetMemo().GetGroupByID(root);
  auto exprs = cur_group->GetLogicalExpressions();

  // If we optimized a group successfully, then it would have been
  // collapsed to only a single group. If we did not optimize a group,
  // then they are all equivalent, so pick any.
  TERRIER_ASSERT(exprs.size() >= 1, "Optimized group should collapse into a single group");
  auto expr = exprs[0];

  std::vector<group_id_t> child_groups = expr->GetChildGroupIDs();
  std::vector<std::unique_ptr<parser::AbstractExpression>> child_exprs;
  for (auto group : child_groups) {
    // Build children first
    common::ManagedPointer<parser::AbstractExpression> child = RebuildExpression(group);
    TERRIER_ASSERT(child != nullptr, "child should not be null");

    child_exprs.push_back(child->Copy());
  }

  common::ManagedPointer<ExpressionNodeContents> c = expr->Contents().CastManagedPointerTo<ExpressionNodeContents>();
  TERRIER_ASSERT(c != nullptr, "contents should not be null");

  return c->CopyWithChildren(std::move(child_exprs));
}

std::unique_ptr<AbstractOptimizerNode> Rewriter::ConvertToOptimizerNode(
    common::ManagedPointer<parser::AbstractExpression> expr) {
  // TODO(): remove the Copy invocation when in terrier since terrier uses shared_ptr
  //
  // This Copy() is not very efficient at all. but within Peloton, this is the only way
  // to present a common::ManagedPointer to the AbstractOptimizerNodeContents/Expression classes. In terrier,
  // this Copy() is *definitely* not needed because the AbstractExpression there already
  // utilizes common::ManagedPointer properly.
  // common::ManagedPointer<parser::AbstractExpression> copy =
  //     common::ManagedPointer<parser::AbstractExpression>(expr);

  // Create current AbstractOptimizerNode
  auto *expr_node_contents = new ExpressionNodeContents(expr);
  expr_node_contents->RegisterWithTxnContext(context_->GetTxn());
  auto container = common::ManagedPointer<AbstractOptimizerNodeContents>(expr_node_contents);
  std::unique_ptr<AbstractOptimizerNode> expression = std::make_unique<ExpressionNode>(
      container, std::vector<std::unique_ptr<AbstractOptimizerNode>>(), context_->GetTxn());

  // Convert all the children
  size_t child_count = expr->GetChildrenSize();
  for (size_t i = 0; i < child_count; i++) {
    expression->PushChild(ConvertToOptimizerNode(expr->GetChild(i)));
  }

  // copy->ClearChildren();
  return expression;
}

common::ManagedPointer<GroupExpression> Rewriter::RecordTreeGroups(
    common::ManagedPointer<parser::AbstractExpression> expr) {
  std::unique_ptr<AbstractOptimizerNode> exp = ConvertToOptimizerNode(expr);
  GroupExpression *gexpr;
  context_->RecordOptimizerNodeIntoGroup(common::ManagedPointer(exp), &gexpr);
  return common::ManagedPointer(gexpr);
}

common::ManagedPointer<parser::AbstractExpression> Rewriter::RewriteExpression(
    common::ManagedPointer<parser::AbstractExpression> expr) {
  if (expr == nullptr) return nullptr;

  // This is needed in order to provide template classes the correct interface
  // and also handle immutable AbstractExpression.
  common::ManagedPointer<GroupExpression> gexpr = RecordTreeGroups(expr);
  OPTIMIZER_LOG_TRACE("Converted tree to internal data structures");

  group_id_t root_id = gexpr->GetGroupID();
  RewriteLoop(root_id);
  OPTIMIZER_LOG_TRACE("Performed rewrite loop pass");

  common::ManagedPointer<parser::AbstractExpression> expr_tree = RebuildExpression(root_id);
  OPTIMIZER_LOG_TRACE("Rebuilt expression tree from memo table");

  std::cout << "rebuilt expression\n";
  Reset(context_->GetTxn());
  std::cout << "reset rewriter\n";
  OPTIMIZER_LOG_TRACE("Reset the rewriter");
  return expr_tree;
}

}  // namespace terrier::optimizer
