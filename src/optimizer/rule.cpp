#include "optimizer/rules/rewriter_rules.h"
#include "optimizer/group_expression.h"
#include "optimizer/rules/implementation_rules.h"
#include "optimizer/rules/rewrite_rules.h"
#include "optimizer/rules/transformation_rules.h"
#include "optimizer/rules/unnesting_rules.h"

namespace terrier::optimizer {

RulePromise Rule::Promise(GroupExpression *group_expr) const {
  auto root_type = match_pattern_->GetOpType();
  // This rule is not applicable
  if (root_type != OpType::LEAF && root_type != group_expr->Contents()->GetOpType()) {
    return RulePromise::NO_PROMISE;
  }
  if (IsPhysical()) return RulePromise::PHYSICAL_PROMISE;
  return RulePromise::LOGICAL_PROMISE;
}

RuleSet::RuleSet() {

  // ===== Optimizer Rules ===== //
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
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalInnerJoinToPhysicalInnerNLJoin());
  AddRule(RuleSetName::PHYSICAL_IMPLEMENTATION, new LogicalInnerJoinToPhysicalInnerHashJoin());
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
  AddRule(RuleSetName::UNNEST_SUBQUERY, new RewritePullFilterThroughAggregation());

  // ===== Query Rewriter Rules ===== //

  // Equivalent Transform related rules (flip AND, OR, EQUAL)
  std::vector<std::pair<RuleType, parser::ExpressionType>> equiv_pairs = {
      std::make_pair(RuleType::EQUIV_AND, parser::ExpressionType::CONJUNCTION_AND),
      std::make_pair(RuleType::EQUIV_OR, parser::ExpressionType::CONJUNCTION_OR),
      std::make_pair(RuleType::EQUIV_COMPARE_EQUAL, parser::ExpressionType::COMPARE_EQUAL)
  };

  for (auto &pair : equiv_pairs) {
    AddRule(RuleSetName::EQUIVALENT_TRANSFORM,
                    new EquivalentTransform(pair.first, pair.second));
  }

  // Additional rewriter rules
  AddRule(RuleSetName::GENERIC_RULES, new TransitiveClosureConstantTransform());
}

}  // namespace terrier::optimizer
