#include "optimizer/abstract_optimizer_node.h"
#include "optimizer/group_expression.h"
#include "optimizer/optimization_context.h"
#include "optimizer/rule.h"

namespace terrier::optimizer {

/*
 * Equivalent Transform: When a symmetric operator (==, !=, AND, OR) has two
 *   children, the comparison expression gets its arguments flipped.
 * Examples:
 *   "T.X != 3" ==> "3 != T.X"
 *   "(T.X == 1) AND (T.Y == 2)" ==> "(T.Y == 2) AND (T.X == 1)"
 */
class EquivalentTransform : public Rule {
 public:
  EquivalentTransform(RuleType rule, parser::ExpressionType root);

  RulePromise Promise(GroupExpression *gexpr) const override;
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/*
 * Transitive Closure Constant Transform: When the same tuple value reference is
 *   compared against a constant and another tuple value reference, we can
 *   rewrite one of the tuple value expressions as that constant.
 * Example:
 *   "(T.X == 3) AND (T.X == T.Y)" ==> "(T.X == 3) AND (3 == T.Y)"
 */
class TransitiveClosureConstantTransform : public Rule {
 public:
  TransitiveClosureConstantTransform();

  RulePromise Promise(GroupExpression *gexpr) const override;
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

}  // namespace terrier::optimizer