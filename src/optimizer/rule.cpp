#include "optimizer/group_expression.h"
#include "optimizer/rules/implementation_rules.h"
#include "optimizer/rules/rewrite_rules.h"
#include "optimizer/rules/transformation_rules.h"
#include "optimizer/rules/unnesting_rules.h"

namespace terrier::optimizer {

RewriteRulePromise Rule::Promise(GroupExpression *group_expr, OptimizationContext *context) const {
  (void)context;
  auto root_type = match_pattern_->Type();
  // This rule is not applicable
  if (root_type != OpType::LEAF && root_type != group_expr->Op().GetType()) {
    return RewriteRulePromise::NO_PROMISE;
  }
  if (IsPhysical()) return RewriteRulePromise::PHYSICAL_PROMISE;
  return RewriteRulePromise::LOGICAL_PROMISE;
}

RuleSet::RuleSet() {
  AddTransformationRule(new LogicalInnerJoinCommutativity());
  AddTransformationRule(new LogicalInnerJoinAssociativity());
  AddImplementationRule(new LogicalDeleteToPhysicalDelete());
  AddImplementationRule(new LogicalUpdateToPhysicalUpdate());
  AddImplementationRule(new LogicalInsertToPhysicalInsert());
  AddImplementationRule(new LogicalInsertSelectToPhysicalInsertSelect());
  AddImplementationRule(new LogicalGroupByToPhysicalHashGroupBy());
  AddImplementationRule(new LogicalAggregateToPhysicalAggregate());
  AddImplementationRule(new LogicalGetToPhysicalTableFreeScan());
  AddImplementationRule(new LogicalGetToPhysicalSeqScan());
  AddImplementationRule(new LogicalGetToPhysicalIndexScan());
  AddImplementationRule(new LogicalExternalFileGetToPhysicalExternalFileGet());
  AddImplementationRule(new LogicalQueryDerivedGetToPhysicalQueryDerivedScan());
  AddImplementationRule(new LogicalInnerJoinToPhysicalInnerNLJoin());
  AddImplementationRule(new LogicalInnerJoinToPhysicalInnerHashJoin());
  AddImplementationRule(new LogicalLimitToPhysicalLimit());
  AddImplementationRule(new LogicalExportToPhysicalExport());

  AddRewriteRule(RewriteRuleSetName::PREDICATE_PUSH_DOWN, new RewritePushImplicitFilterThroughJoin());
  AddRewriteRule(RewriteRuleSetName::PREDICATE_PUSH_DOWN, new RewritePushExplicitFilterThroughJoin());
  AddRewriteRule(RewriteRuleSetName::PREDICATE_PUSH_DOWN, new RewritePushFilterThroughAggregation());
  AddRewriteRule(RewriteRuleSetName::PREDICATE_PUSH_DOWN, new RewriteCombineConsecutiveFilter());
  AddRewriteRule(RewriteRuleSetName::PREDICATE_PUSH_DOWN, new RewriteEmbedFilterIntoGet());

  AddRewriteRule(RewriteRuleSetName::UNNEST_SUBQUERY, new RewritePullFilterThroughMarkJoin());
  AddRewriteRule(RewriteRuleSetName::UNNEST_SUBQUERY, new UnnestMarkJoinToInnerJoin());
  AddRewriteRule(RewriteRuleSetName::UNNEST_SUBQUERY, new RewritePullFilterThroughAggregation());
}

}  // namespace terrier::optimizer
