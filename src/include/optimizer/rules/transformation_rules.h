#pragma once

#include <memory>
#include <vector>

#include "optimizer/rule.h"

namespace terrier::optimizer {

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

}  // namespace terrier::optimizer
