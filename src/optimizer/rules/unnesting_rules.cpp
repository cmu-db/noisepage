#include "optimizer/rules/unnesting_rules.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "loggers/optimizer_logger.h"
#include "optimizer/group_expression.h"
#include "optimizer/index_util.h"
#include "optimizer/logical_operators.h"
#include "optimizer/optimizer_context.h"
#include "optimizer/optimizer_defs.h"
#include "optimizer/physical_operators.h"
#include "optimizer/properties.h"
#include "optimizer/util.h"
#include "parser/expression_util.h"

namespace terrier::optimizer {

///////////////////////////////////////////////////////////////////////////////
/// UnnestMarkJoinToInnerJoin
///////////////////////////////////////////////////////////////////////////////
UnnestMarkJoinToInnerJoin::UnnestMarkJoinToInnerJoin() {
  type_ = RuleType::MARK_JOIN_GET_TO_INNER_JOIN;

  match_pattern_ = new Pattern(OpType::LOGICALMARKJOIN);
  match_pattern_->AddChild(new Pattern(OpType::LEAF));
  match_pattern_->AddChild(new Pattern(OpType::LEAF));
}

RulePromise UnnestMarkJoinToInnerJoin::Promise(GroupExpression *group_expr) const {
  return RulePromise::LOGICAL_PROMISE;
}

bool UnnestMarkJoinToInnerJoin::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                      OptimizationContext *context) const {
  (void)context;
  (void)plan;

  UNUSED_ATTRIBUTE auto children = plan->GetChildren();
  TERRIER_ASSERT(children.size() == 2, "LogicalMarkJoin should have 2 children");
  return true;
}

void UnnestMarkJoinToInnerJoin::Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                                          std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                          UNUSED_ATTRIBUTE OptimizationContext *context) const {
  OPTIMIZER_LOG_TRACE("UnnestMarkJoinToInnerJoin::Transform");
  UNUSED_ATTRIBUTE auto mark_join = input->Contents()->GetContentsAs<LogicalMarkJoin>();
  TERRIER_ASSERT(mark_join->GetJoinPredicates().empty(), "MarkJoin should have 0 predicates");

  auto join_children = input->GetChildren();
  std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
  c.emplace_back(join_children[0]->Copy());
  c.emplace_back(join_children[1]->Copy());
  auto output = std::make_unique<OperatorNode>(
      LogicalInnerJoin::Make().RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()), std::move(c),
      context->GetOptimizerContext()->GetTxn());
  transformed->emplace_back(std::move(output));
}

///////////////////////////////////////////////////////////////////////////////
/// SingleJoinGetToInnerJoin
///////////////////////////////////////////////////////////////////////////////
UnnestSingleJoinToInnerJoin::UnnestSingleJoinToInnerJoin() {
  type_ = RuleType::MARK_JOIN_GET_TO_INNER_JOIN;

  match_pattern_ = new Pattern(OpType::LOGICALSINGLEJOIN);
  match_pattern_->AddChild(new Pattern(OpType::LEAF));
  match_pattern_->AddChild(new Pattern(OpType::LEAF));
}

RulePromise UnnestSingleJoinToInnerJoin::Promise(GroupExpression *group_expr) const {
  return RulePromise::LOGICAL_PROMISE;
}

bool UnnestSingleJoinToInnerJoin::Check(common::ManagedPointer<AbstractOptimizerNode> plan,
                                        OptimizationContext *context) const {
  (void)context;
  (void)plan;

  UNUSED_ATTRIBUTE auto children = plan->GetChildren();
  TERRIER_ASSERT(children.size() == 2, "SingleJoin should have 2 children");
  return true;
}

void UnnestSingleJoinToInnerJoin::Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                                            std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                                            UNUSED_ATTRIBUTE OptimizationContext *context) const {
  OPTIMIZER_LOG_TRACE("UnnestSingleJoinToInnerJoin::Transform");
  UNUSED_ATTRIBUTE auto single_join = input->Contents()->GetContentsAs<LogicalSingleJoin>();
  TERRIER_ASSERT(single_join->GetJoinPredicates().empty(), "SingleJoin should have no predicates");

  auto join_children = input->GetChildren();
  std::vector<std::unique_ptr<AbstractOptimizerNode>> c;
  c.emplace_back(join_children[0]->Copy());
  c.emplace_back(join_children[1]->Copy());
  auto output = std::make_unique<OperatorNode>(
      LogicalInnerJoin::Make().RegisterWithTxnContext(context->GetOptimizerContext()->GetTxn()), std::move(c),
      context->GetOptimizerContext()->GetTxn());
  transformed->emplace_back(std::move(output));
}

}  // namespace terrier::optimizer
