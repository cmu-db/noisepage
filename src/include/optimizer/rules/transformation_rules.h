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

}  // namespace noisepage::optimizer
