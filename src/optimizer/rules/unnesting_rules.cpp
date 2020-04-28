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
  type_ = RuleType::SINGLE_JOIN_GET_TO_INNER_JOIN;

  match_pattern_ = new Pattern(OpType::LOGICALSINGLEJOIN);
  match_pattern_->AddChild(new Pattern(OpType::LEAF));
  auto right_child = new Pattern(OpType::LOGICALAGGREGATEANDGROUPBY);
  right_child->AddChild(new Pattern(OpType::LEAF));
  match_pattern_->AddChild(right_child);
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

///////////////////////////////////////////////////////////////////////////////
/// DependentSingleJoinToInnerJoin
///////////////////////////////////////////////////////////////////////////////
DependentSingleJoinToInnerJoin::DependentSingleJoinToInnerJoin() {
  type_ = RuleType::DEPENDENT_JOIN_GET_TO_INNER_JOIN;

  match_pattern_ = new Pattern(OpType::LOGICALSINGLEJOIN);
  match_pattern_->AddChild(new Pattern(OpType::LEAF));
  auto right_child = new Pattern(OpType::LOGICALFILTER);
  auto agg_child = new Pattern(OpType::LOGICALAGGREGATEANDGROUPBY);
  agg_child->AddChild(new Pattern(OpType::LEAF));
  right_child->AddChild(agg_child);
  match_pattern_->AddChild(right_child);
}

RulePromise DependentSingleJoinToInnerJoin::Promise(GroupExpression *group_expr) const {
  return RulePromise::LOGICAL_PROMISE;
}

bool DependentSingleJoinToInnerJoin::Check(common::ManagedPointer<OperatorNode> plan,
                                           OptimizationContext *context) const {
  (void)context;
  (void)plan;

  UNUSED_ATTRIBUTE auto children = plan->GetChildren();
  TERRIER_ASSERT(children.size() == 2, "SingleJoin should have 2 children");
  return true;
}

void DependentSingleJoinToInnerJoin::Transform(common::ManagedPointer<OperatorNode> input,
                                               std::vector<std::unique_ptr<OperatorNode>> *transformed,
                                               UNUSED_ATTRIBUTE OptimizationContext *context) const {
  UNUSED_ATTRIBUTE auto single_join = input->GetOp().As<LogicalSingleJoin>();
  TERRIER_ASSERT(single_join->GetJoinPredicates().empty(), "SingleJoin should have no predicates");
  // From LOGICALSINGLEJOIN -> LOGICALFILTER -> LOGICALAGGREGATEANDGROUPBY
  //  to LOGICALFILTER -> LOGICALINNERJOIN -> LOGICALFILTER -> LOGICALAGGREGATEANDGROUPBY
  auto &memo = context->GetOptimizerContext()->GetMemo();
  auto filter_expr = input->GetChildren()[1];
  auto agg_expr = filter_expr->GetChildren()[0];
  auto agg_group_id = agg_expr->GetChildren()[0]->GetOp().As<LeafOperator>()->GetOriginGroup();
  const auto &agg_group_aliases_set = memo.GetGroupByID(agg_group_id)->GetTableAliases();
  auto &filter_predicates = filter_expr->GetOp().As<LogicalFilter>()->GetPredicates();

  std::vector<AnnotatedExpression> top_predicates;
  std::vector<AnnotatedExpression> down_predicates;
  std::vector<common::ManagedPointer<parser::AbstractExpression>> new_groupby_cols;

  // loop over all predicates check each of them if they refer table not contained in agg
  for (auto &predicate : filter_predicates) {
    if (OptimizerUtil::IsSubset(agg_group_aliases_set, predicate.GetTableAliasSet())) {
      down_predicates.emplace_back(predicate);
    } else {
      // Correlated predicate, already in the form of
      // (outer_relation.a = (expr))
      top_predicates.emplace_back(predicate);
      auto root_expr = predicate.GetExpr();
      if (root_expr->GetChild(0)->GetDepth() < root_expr->GetDepth()) {
        new_groupby_cols.emplace_back(root_expr->GetChild(1).Get());
      } else {
        new_groupby_cols.emplace_back(root_expr->GetChild(0).Get());
      }
    }
  }

  // Create a new agg node
  auto aggregation = agg_expr->GetOp().As<LogicalAggregateAndGroupBy>();
  for (auto &col : aggregation->GetColumns()) {
    new_groupby_cols.emplace_back(col);
  }
  std::vector<std::unique_ptr<OperatorNode>> c;
  c.emplace_back(agg_expr->GetChildren()[0]->Copy());
  std::vector<AnnotatedExpression> new_having = aggregation->GetHaving();
  auto new_aggr = std::make_unique<OperatorNode>(
      LogicalAggregateAndGroupBy::Make(std::move(new_groupby_cols), std::move(new_having)), std::move(c));

  // Create a new inner join node from single join
  std::vector<std::unique_ptr<OperatorNode>> ci;
  ci.emplace_back(input->GetChildren()[0]->Copy());
  if (!down_predicates.empty()) {
    std::vector<std::unique_ptr<OperatorNode>> cf;
    cf.emplace_back(std::move(new_aggr));
    auto filter = std::make_unique<OperatorNode>(LogicalFilter::Make(std::move(down_predicates)), std::move(cf));
    ci.emplace_back(std::move(filter));
  } else {
    ci.emplace_back(std::move(new_aggr));
  }
  auto new_inner = std::make_unique<OperatorNode>(LogicalInnerJoin::Make(), std::move(ci));

  std::unique_ptr<OperatorNode> output;
  // Create new filter nodes
  // Construct a top filter if any
  if (!top_predicates.empty()) {
    std::vector<std::unique_ptr<OperatorNode>> cf;
    cf.emplace_back(std::move(new_inner));
    output = std::make_unique<OperatorNode>(LogicalFilter::Make(std::move(top_predicates)), std::move(cf));
  } else {
    output = std::move(new_inner);
  }
  transformed->emplace_back(std::move(output));
}

}  // namespace terrier::optimizer
