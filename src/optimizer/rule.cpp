#include "optimizer/group_expression.h"
#include "optimizer/rules/implementation_rules.h"
#include "optimizer/rules/rewrite_rules.h"
#include "optimizer/rules/transformation_rules.h"
#include "optimizer/rules/unnesting_rules.h"

namespace terrier::optimizer {

RulePromise Rule::Promise(GroupExpression *group_expr) const {
  auto root_type = match_pattern_->Type();
  // This rule is not applicable
  if (root_type != OpType::LEAF && root_type != group_expr->Contents()->GetOpType()) {
    return RulePromise::NO_PROMISE;
  }
  if (IsPhysical()) return RulePromise::PHYSICAL_PROMISE;
  return RulePromise::LOGICAL_PROMISE;
}

RuleSet::RuleSet() {
  AddRule(RuleSetName::LOGICAL_TRANSFORMATION, new LogicalInnerJoinCommutativity());
  AddRule(RuleSetName::LOGICAL_TRANSFORMATION, new LogicalInnerJoinAssociativity());

  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalDeleteToPhysicalDelete());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalUpdateToPhysicalUpdate());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalInsertToPhysicalInsert());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalInsertSelectToPhysicalInsertSelect());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalGroupByToPhysicalHashGroupBy());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalAggregateToPhysicalAggregate());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalGetToPhysicalTableFreeScan());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalGetToPhysicalSeqScan());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalGetToPhysicalIndexScan());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalExternalFileGetToPhysicalExternalFileGet());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalQueryDerivedGetToPhysicalQueryDerivedScan());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalInnerJoinToPhysicalInnerIndexJoin());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalInnerJoinToPhysicalInnerNLJoin());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalSemiJoinToPhysicalSemiLeftHashJoin());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalInnerJoinToPhysicalInnerHashJoin());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalLeftJoinToPhysicalLeftHashJoin());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalLimitToPhysicalLimit());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalExportToPhysicalExport());

  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalCreateDatabaseToPhysicalCreateDatabase());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalCreateFunctionToPhysicalCreateFunction());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalCreateIndexToPhysicalCreateIndex());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalCreateTableToPhysicalCreateTable());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalCreateNamespaceToPhysicalCreateNamespace());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalCreateTriggerToPhysicalCreateTrigger());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalCreateViewToPhysicalCreateView());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalDropDatabaseToPhysicalDropDatabase());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalDropTableToPhysicalDropTable());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalDropIndexToPhysicalDropIndex());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalDropNamespaceToPhysicalDropNamespace());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalDropTriggerToPhysicalDropTrigger());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalDropViewToPhysicalDropView());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalAnalyzeToPhysicalAnalyze());

  AddRule(RuleSetName::PREDICATE_PUSH_DOWN, new RewritePushImplicitFilterThroughJoin());
  AddRule(RuleSetName::PREDICATE_PUSH_DOWN, new RewritePushExplicitFilterThroughJoin());
  AddRule(RuleSetName::PREDICATE_PUSH_DOWN, new RewritePushFilterThroughAggregation());
  AddRule(RuleSetName::PREDICATE_PUSH_DOWN, new RewriteCombineConsecutiveFilter());
  AddRule(RuleSetName::PREDICATE_PUSH_DOWN, new RewriteEmbedFilterIntoGet());

  AddRule(RuleSetName::UNNEST_SUBQUERY, new RewritePullFilterThroughMarkJoin());
  AddRule(RuleSetName::UNNEST_SUBQUERY, new UnnestMarkJoinToInnerJoin());
  AddRule(RuleSetName::UNNEST_SUBQUERY, new UnnestSingleJoinToInnerJoin());
  AddRule(RuleSetName::UNNEST_SUBQUERY, new DependentSingleJoinToInnerJoin());
  AddRule(RuleSetName::UNNEST_SUBQUERY, new RewritePullFilterThroughAggregation());
}

}  // namespace terrier::optimizer
