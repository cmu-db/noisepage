#pragma once

#include <memory>
#include <vector>

#include "optimizer/rule.h"

namespace noisepage::optimizer {

/**
 * Rule transforms (A JOIN B) -> (B JOIN A)
 */
class LogicalInnerJoinCommutativity : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalInnerJoinCommutativity();

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
 * Rule transforms (A JOIN B) JOIN C -> A JOIN (B JOIN C)
 */

class LogicalInnerJoinAssociativity : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalInnerJoinAssociativity();

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
 * Rule embeds a logical limit into a child scan operator.
 * TODO(dpatra): After pruning stage, we should eliminate all limits with children get operators in the operator trees.
 */
class SetLimitInGet : public Rule {
 public:
  /**
   * Constructor
   */
  SetLimitInGet();

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
 * Rule embeds a logical limit into a child scan operator.
 * TODO(dpatra): After pruning stage, we should eliminate all limits with children get operators in the operator trees.
 */
class SetLimitInLogicalInnerJoin : public Rule {
 public:
  /**
   * Constructor
   */
  SetLimitInLogicalInnerJoin();

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
