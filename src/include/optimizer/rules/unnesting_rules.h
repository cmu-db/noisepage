#pragma once

#include <memory>
#include <vector>

#include "optimizer/rule.h"

namespace terrier::optimizer {

// TODO(boweic): MarkJoin and SingleJoin should not be transformed into inner
// join. Sometimes MarkJoin could be transformed into semi-join, but for now we
// do not have these operators in the llvm cogen engine. Once we have those, we
// should not use the following rules in the rewrite phase

/**
 *  Unnest Mark Join to Inner Join
 */
class UnnestMarkJoinToInnerJoin : public Rule {
 public:
  /**
   * Constructor
   */
  UnnestMarkJoinToInnerJoin();

  /**
   * Gets the rule's promise to apply against a GroupExpression
   * @param group_expr GroupExpression to compute promise from
   * @param context OptimizationContext currently executing under
   * @returns The promise value of applying the rule for ordering
   */
  RewriteRulePromise Promise(GroupExpression *group_expr, OptimizationContext *context) const override;

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Unnest Single Join to Inner Join
 */
class UnnestSingleJoinToInnerJoin : public Rule {
 public:
  /**
   * Constructor
   */
  UnnestSingleJoinToInnerJoin();

  /**
   * Gets the rule's promise to apply against a GroupExpression
   * @param group_expr GroupExpression to compute promise from
   * @param context OptimizationContext currently executing under
   * @returns The promise value of applying the rule for ordering
   */
  RewriteRulePromise Promise(GroupExpression *group_expr, OptimizationContext *context) const override;

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorExpression to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input OperatorExpression passes the check
   */
  bool Check(common::ManagedPointer<OperatorExpression> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorExpression to transform
   * @param transformed Vector of transformed OperatorExpressions
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<OperatorExpression> input,
                 std::vector<std::unique_ptr<OperatorExpression>> *transformed,
                 OptimizationContext *context) const override;
};

};  // namespace terrier::optimizer
