#include "optimizer/rules/unnesting_rules.h"

#include <memory>
#include <type_traits>
#include <vector>

#include "common/macros.h"
#include "common/managed_pointer.h"
#include "loggers/optimizer_logger.h"
#include "optimizer/logical_operators.h"
#include "optimizer/operator_node.h"
#include "optimizer/operator_node_contents.h"
#include "optimizer/optimizer_defs.h"
#include "optimizer/pattern.h"
#include "optimizer/rule.h"

namespace terrier::optimizer {
class GroupExpression;
class OptimizationContext;
}  // namespace terrier::optimizer

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

bool UnnestMarkJoinToInnerJoin::Check(common::ManagedPointer<OperatorNode> plan, OptimizationContext *context) const {
  (void)context;
  (void)plan;

  UNUSED_ATTRIBUTE auto children = plan->GetChildren();
  TERRIER_ASSERT(children.size() == 2, "LogicalMarkJoin should have 2 children");
  return true;
}

void UnnestMarkJoinToInnerJoin::Transform(common::ManagedPointer<OperatorNode> input,
                                          std::vector<std::unique_ptr<OperatorNode>> *transformed,
                                          UNUSED_ATTRIBUTE OptimizationContext *context) const {
  OPTIMIZER_LOG_TRACE("UnnestMarkJoinToInnerJoin::Transform");
  UNUSED_ATTRIBUTE auto mark_join = input->GetOp().As<LogicalMarkJoin>();
  TERRIER_ASSERT(mark_join->GetJoinPredicates().empty(), "MarkJoin should have 0 predicates");

  auto join_children = input->GetChildren();
  std::vector<std::unique_ptr<OperatorNode>> c;
  c.emplace_back(join_children[0]->Copy());
  c.emplace_back(join_children[1]->Copy());
  auto output = std::make_unique<OperatorNode>(LogicalInnerJoin::Make(), std::move(c));
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

bool UnnestSingleJoinToInnerJoin::Check(common::ManagedPointer<OperatorNode> plan, OptimizationContext *context) const {
  (void)context;
  (void)plan;

  UNUSED_ATTRIBUTE auto children = plan->GetChildren();
  TERRIER_ASSERT(children.size() == 2, "SingleJoin should have 2 children");
  return true;
}

void UnnestSingleJoinToInnerJoin::Transform(common::ManagedPointer<OperatorNode> input,
                                            std::vector<std::unique_ptr<OperatorNode>> *transformed,
                                            UNUSED_ATTRIBUTE OptimizationContext *context) const {
  OPTIMIZER_LOG_TRACE("UnnestSingleJoinToInnerJoin::Transform");
  UNUSED_ATTRIBUTE auto single_join = input->GetOp().As<LogicalSingleJoin>();
  TERRIER_ASSERT(single_join->GetJoinPredicates().empty(), "SingleJoin should have no predicates");

  auto join_children = input->GetChildren();
  std::vector<std::unique_ptr<OperatorNode>> c;
  c.emplace_back(join_children[0]->Copy());
  c.emplace_back(join_children[1]->Copy());
  auto output = std::make_unique<OperatorNode>(LogicalInnerJoin::Make(), std::move(c));
  transformed->emplace_back(std::move(output));
}

}  // namespace terrier::optimizer
