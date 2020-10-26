#pragma once

#include <memory>
#include <vector>

#include "optimizer/rule.h"

namespace noisepage::optimizer {

/**
 * Rule performs predicate push-down to push a filter through join. For
 * example, for query "SELECT test.a, test.b FROM test, test1 WHERE test.a = 5"
 * we could push "test.a=5" through the join to evaluate at the table scan
 * level
 */
class RewritePushImplicitFilterThroughJoin : public Rule {
 public:
  /**
   * Constructor
   */
  RewritePushImplicitFilterThroughJoin();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule performs predicate push-down to push a filter through join. For
 * example, for query "SELECT test.a, test.b FROM test, test1 WHERE test.a = 5"
 * we could push "test.a=5" through the join to evaluate at the table scan
 * level
 */
class RewritePushExplicitFilterThroughJoin : public Rule {
 public:
  /**
   * Constructor
   */
  RewritePushExplicitFilterThroughJoin();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms consecutive filters into a single filter
 */
class RewriteCombineConsecutiveFilter : public Rule {
 public:
  /**
   * Constructor
   */
  RewriteCombineConsecutiveFilter();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule performs predicate push-down to push a filter through aggregation, also
 * will embed filter into aggregation operator if appropriate.
 */
class RewritePushFilterThroughAggregation : public Rule {
 public:
  /**
   * Constructor
   */
  RewritePushFilterThroughAggregation();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule embeds a filter into a scan operator. After predicate push-down, we
 * eliminate all filters in the operator trees. Predicates should be associated
 * with get or join
 */
class RewriteEmbedFilterIntoGet : public Rule {
 public:
  /**
   * Constructor
   */
  RewriteEmbedFilterIntoGet();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rewrite Pull Filter through Mark Join
 */
class RewritePullFilterThroughMarkJoin : public Rule {
 public:
  /**
   * Constructor
   */
  RewritePullFilterThroughMarkJoin();

  /**
   * Gets the rule's promise to apply against a GroupExpression
   * @param group_expr GroupExpression to compute promise from
   * @returns The promise value of applying the rule for ordering
   */
  RulePromise Promise(GroupExpression *group_expr) const override;

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rewrite Pull Filter through aggregation
 */
class RewritePullFilterThroughAggregation : public Rule {
 public:
  /**
   * Constructor
   */
  RewritePullFilterThroughAggregation();

  /**
   * Gets the rule's promise to apply against a GroupExpression
   * @param group_expr GroupExpression to compute promise from
   * @returns The promise value of applying the rule for ordering
   */
  RulePromise Promise(GroupExpression *group_expr) const override;

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

}  // namespace noisepage::optimizer
