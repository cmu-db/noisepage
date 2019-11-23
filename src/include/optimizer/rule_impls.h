#pragma once

#include "optimizer/rule.h"

#include <memory>
#include <vector>

namespace terrier::optimizer {

//===--------------------------------------------------------------------===//
// Transformation rules
//===--------------------------------------------------------------------===//

/**
 * Rule transforms (A JOIN B) -> (B JOIN A)
 */
class InnerJoinCommutativity : public Rule {
 public:
  /**
   * Constructor
   */
  InnerJoinCommutativity();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

/**
 * Rule transforms (A JOIN B) JOIN C -> A JOIN (B JOIN C)
 */

class InnerJoinAssociativity : public Rule {
 public:
  /**
   * Constructor
   */
  InnerJoinAssociativity();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

//===--------------------------------------------------------------------===//
// Implementation rules
//===--------------------------------------------------------------------===//

/**
 * Rule transforms Logical Scan -> Sequential Scan
 */
class GetToSeqScan : public Rule {
 public:
  /**
   * Constructor
   */
  GetToSeqScan();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

/**
 * Rule transforms LogicalExternalFileGet -> ExternalFileGet
 */
class LogicalExternalFileGetToPhysical : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalExternalFileGetToPhysical();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

/**
 * Generate dummy scan for queries like "SELECT 1", there's no actual
 * table to generate
 */
class GetToTableFreeScan : public Rule {
 public:
  /**
   * Constructor
   */
  GetToTableFreeScan();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

/**
 * Rule transforms Logical Scan -> Index Scan
 */
class GetToIndexScan : public Rule {
 public:
  /**
   * Constructor
   */
  GetToIndexScan();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

/**
 * Rule that transforms query derived scans for nested queries
 */
class LogicalQueryDerivedGetToPhysical : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalQueryDerivedGetToPhysical();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

/**
 * Rule transforms LogicalDelete -> Delete
 */
class LogicalDeleteToPhysical : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalDeleteToPhysical();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

/**
 * Rule transforms LogicalUpdate -> Update
 */
class LogicalUpdateToPhysical : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalUpdateToPhysical();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

/**
 * Rule transforms LogicalInsert -> Insert
 */
class LogicalInsertToPhysical : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalInsertToPhysical();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

/**
 * Rule transforms LogicalInsertSelect -> InsertSelect
 */
class LogicalInsertSelectToPhysical : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalInsertSelectToPhysical();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

/**
 * Rule transforms LogicalGroupBy -> HashGroupBy
 */
class LogicalGroupByToHashGroupBy : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalGroupByToHashGroupBy();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

/**
 * Rule transforms LogicalAggregate -> Aggregate
 */
class LogicalAggregateToPhysical : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalAggregateToPhysical();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

/**
 * Rule transforms Logical Inner Join to InnerNLJoin
 */
class InnerJoinToInnerNLJoin : public Rule {
 public:
  /**
   * Constructor
   */
  InnerJoinToInnerNLJoin();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

/**
 * Rule transforms Logical Inner Join to InnerHashJoin
 */
class InnerJoinToInnerHashJoin : public Rule {
 public:
  /**
   * Constructor
   */
  InnerJoinToInnerHashJoin();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

/**
 * Rule transforms LogicalLimit -> Limit
 */
class ImplementLimit : public Rule {
 public:
  /**
   * Constructor
   */
  ImplementLimit();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

/**
 * Rule transforms Logical Export -> Physical Export
 */
class LogicalExportToPhysicalExport : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalExportToPhysicalExport();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

//===--------------------------------------------------------------------===//
// Rewrite rules
//===--------------------------------------------------------------------===//

/**
 * Rule performs predicate push-down to push a filter through join. For
 * example, for query "SELECT test.a, test.b FROM test, test1 WHERE test.a = 5"
 * we could push "test.a=5" through the join to evaluate at the table scan
 * level
 */
class PushFilterThroughJoin : public Rule {
 public:
  /**
   * Constructor
   */
  PushFilterThroughJoin();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

/**
 * Rule transforms consecutive filters into a single filter
 */
class CombineConsecutiveFilter : public Rule {
 public:
  /**
   * Constructor
   */
  CombineConsecutiveFilter();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

/**
 * Rule performs predicate push-down to push a filter through aggregation, also
 * will embed filter into aggregation operator if appropriate.
 */
class PushFilterThroughAggregation : public Rule {
 public:
  /**
   * Constructor
   */
  PushFilterThroughAggregation();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

/**
 * Rule embeds a filter into a scan operator. After predicate push-down, we
 * eliminate all filters in the operator trees. Predicates should be associated
 * with get or join
 */
class EmbedFilterIntoGet : public Rule {
 public:
  /**
   * Constructor
   */
  EmbedFilterIntoGet();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// Unnesting rules
// We use this promise to determine which rules should be applied first if
// multiple rules are applicable, we need to first pull filters up through mark-join
// then turn mark-join into a regular join operator
enum class UnnestPromise { Low = 1, High };
// TODO(boweic): MarkJoin and SingleJoin should not be transformed into inner
// join. Sometimes MarkJoin could be transformed into semi-join, but for now we
// do not have these operators in the llvm cogen engine. Once we have those, we
// should not use the following rules in the rewrite phase
///////////////////////////////////////////////////////////////////////////////
/// MarkJoinGetToInnerJoin
class MarkJoinToInnerJoin : public Rule {
 public:
  /**
   * Constructor
   */
  MarkJoinToInnerJoin();

  /**
   * Gets the rule's promise to apply against a GroupExpression
   * @param group_expr GroupExpression to compute promise from
   * @param context OptimizeContext currently executing under
   * @returns The promise value of applying the rule for ordering
   */
  int Promise(GroupExpression *group_expr, OptimizeContext *context) const override;

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};
///////////////////////////////////////////////////////////////////////////////
/// SingleJoinToInnerJoin
class SingleJoinToInnerJoin : public Rule {
 public:
  /**
   * Constructor
   */
  SingleJoinToInnerJoin();

  /**
   * Gets the rule's promise to apply against a GroupExpression
   * @param group_expr GroupExpression to compute promise from
   * @param context OptimizeContext currently executing under
   * @returns The promise value of applying the rule for ordering
   */
  int Promise(GroupExpression *group_expr, OptimizeContext *context) const override;

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// PullFilterThroughMarkJoin
class PullFilterThroughMarkJoin : public Rule {
 public:
  /**
   * Constructor
   */
  PullFilterThroughMarkJoin();

  /**
   * Gets the rule's promise to apply against a GroupExpression
   * @param group_expr GroupExpression to compute promise from
   * @param context OptimizeContext currently executing under
   * @returns The promise value of applying the rule for ordering
   */
  int Promise(GroupExpression *group_expr, OptimizeContext *context) const override;

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};

///////////////////////////////////////////////////////////////////////////////
/// PullFilterThroughAggregation
class PullFilterThroughAggregation : public Rule {
 public:
  /**
   * Constructor
   */
  PullFilterThroughAggregation();

  /**
   * Gets the rule's promise to apply against a GroupExpression
   * @param group_expr GroupExpression to compute promise from
   * @param context OptimizeContext currently executing under
   * @returns The promise value of applying the rule for ordering
   */
  int Promise(GroupExpression *group_expr, OptimizeContext *context) const override;

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizeContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizeContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizeContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizeContext *context) const override;
};
}  // namespace terrier::optimizer
