#include "optimizer/group_expression.h"
#include "optimizer/rules/implementation_rules.h"
#include "optimizer/rules/rewrite_rules.h"
#include "optimizer/rules/rewriter_rules.h"
#include "optimizer/rules/transformation_rules.h"
#include "optimizer/rules/unnesting_rules.h"

namespace terrier::optimizer {

RulePromise Rule::Promise(GroupExpression *group_expr) const {
  auto root_op_type = match_pattern_->GetOpType();
  auto root_exp_type = match_pattern_->GetExpType();
  // This rule is not applicable
  if (root_op_type != OpType::LEAF && root_op_type != OpType::UNDEFINED &&
      root_op_type != group_expr->Contents()->GetOpType()) {
    return RulePromise::NO_PROMISE;
  }
  if (root_exp_type != parser::ExpressionType::INVALID && root_exp_type != parser::ExpressionType::GROUP_MARKER &&
      root_exp_type != group_expr->Contents()->GetExpType()) {
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

  // Symmetric Reordering related rules (flip AND, OR, EQUAL, NOT_EQUAL)
  std::vector<std::pair<RuleType, parser::ExpressionType>> symmetric_reordering_pairs = {
      std::make_pair(RuleType::SYMMETRIC_REORDERING_AND, parser::ExpressionType::CONJUNCTION_AND),
      std::make_pair(RuleType::SYMMETRIC_REORDERING_OR, parser::ExpressionType::CONJUNCTION_OR),
      std::make_pair(RuleType::SYMMETRIC_REORDERING_EQUAL, parser::ExpressionType::COMPARE_EQUAL),
      std::make_pair(RuleType::SYMMETRIC_REORDERING_NOT_EQUAL, parser::ExpressionType::COMPARE_NOT_EQUAL)};

  for (auto &pair : symmetric_reordering_pairs) {
    AddRule(RuleSetName::SYMMETRIC_REORDERING, new SymmetricReordering(pair.first, pair.second));
  }

  // Additional rewriter rules
  std::vector<std::pair<RuleType, std::vector<parser::ExpressionType>>> comparison_intersection_pairs = {
      // (A < B) AND (A > B) --> FALSE
      std::make_pair(RuleType::COMPARISON_INTERSECTION_LT_GT,
                     std::vector<parser::ExpressionType>({parser::ExpressionType::COMPARE_LESS_THAN,
                                                          parser::ExpressionType::COMPARE_GREATER_THAN,
                                                          parser::ExpressionType::INVALID})),
      // (A < B) AND (A = B) --> FALSE
      std::make_pair(RuleType::COMPARISON_INTERSECTION_GT_EQ,
                     std::vector<parser::ExpressionType>({parser::ExpressionType::COMPARE_GREATER_THAN,
                                                          parser::ExpressionType::COMPARE_EQUAL,
                                                          parser::ExpressionType::INVALID})),
      // (A > B) AND (A = B) --> FALSE
      std::make_pair(RuleType::COMPARISON_INTERSECTION_LT_EQ,
                     std::vector<parser::ExpressionType>({parser::ExpressionType::COMPARE_LESS_THAN,
                                                          parser::ExpressionType::COMPARE_EQUAL,
                                                          parser::ExpressionType::INVALID})),
      // (A < B) AND (A >= B) --> FALSE
      std::make_pair(RuleType::COMPARISON_INTERSECTION_LT_GE,
                     std::vector<parser::ExpressionType>({parser::ExpressionType::COMPARE_LESS_THAN,
                                                          parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO,
                                                          parser::ExpressionType::INVALID})),
      // (A > B) AND (A <= B) --> FALSE
      std::make_pair(RuleType::COMPARISON_INTERSECTION_GT_LE,
                     std::vector<parser::ExpressionType>({parser::ExpressionType::COMPARE_GREATER_THAN,
                                                          parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO,
                                                          parser::ExpressionType::INVALID})),
      // (A >= B) AND (A <= B) --> A = B
      std::make_pair(RuleType::COMPARISON_INTERSECTION_GE_LE,
                     std::vector<parser::ExpressionType>({parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO,
                                                          parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO,
                                                          parser::ExpressionType::COMPARE_EQUAL})),
      // (A >= B) AND (A = B) --> A = B
      std::make_pair(RuleType::COMPARISON_INTERSECTION_GE_EQ,
                     std::vector<parser::ExpressionType>({parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO,
                                                          parser::ExpressionType::COMPARE_EQUAL,
                                                          parser::ExpressionType::COMPARE_EQUAL})),
      // (A <= B) AND (A = B) --> A = B
      std::make_pair(RuleType::COMPARISON_INTERSECTION_LE_EQ,
                     std::vector<parser::ExpressionType>({parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO,
                                                          parser::ExpressionType::COMPARE_EQUAL,
                                                          parser::ExpressionType::COMPARE_EQUAL})),
      // (A >= B) AND (A != B) --> A > B
      std::make_pair(RuleType::COMPARISON_INTERSECTION_NE_GE,
                     std::vector<parser::ExpressionType>({parser::ExpressionType::COMPARE_NOT_EQUAL,
                                                          parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO,
                                                          parser::ExpressionType::COMPARE_GREATER_THAN})),
      // (A <= B) AND (A != B) --> A < B
      std::make_pair(RuleType::COMPARISON_INTERSECTION_NE_LE,
                     std::vector<parser::ExpressionType>({parser::ExpressionType::COMPARE_NOT_EQUAL,
                                                          parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO,
                                                          parser::ExpressionType::COMPARE_LESS_THAN}))};

  for (auto &pair : comparison_intersection_pairs) {
    AddRule(RuleSetName::GENERIC_RULES,
            new ComparisonIntersection(pair.first, pair.second[0], pair.second[1], pair.second[2]));
  }

  AddRule(RuleSetName::GENERIC_RULES, new TransitiveClosureConstantTransform());
}

}  // namespace terrier::optimizer
