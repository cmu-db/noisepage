#include "optimizer/abstract_optimizer_node.h"
#include "optimizer/group_expression.h"
#include "optimizer/optimization_context.h"
#include "optimizer/rule.h"

namespace terrier::optimizer {

/*
 * Symmetric Reordering: When a symmetric operator (==, !=, AND, OR) has two
 *   children, the comparison expression gets its arguments flipped.
 * Examples:
 *   "T.X != 3" ==> "3 != T.X"
 *   "(T.X == 1) AND (T.Y == 2)" ==> "(T.Y == 2) AND (T.X == 1)"
 */
class SymmetricReordering : public Rule {
 public:
  /**
   * Constructor.
   * @param rule the rule type of this rule
   * @param root the symmetric operation this rule works with
   */
  SymmetricReordering(RuleType rule, parser::ExpressionType root);

  /**
   * Returns the type of promise for this rule.
   * @param gexpr the group expression (unused)
   * @return an indicator that this is a high-priority rule
   */
  RulePromise Promise(GroupExpression *gexpr) const override;

  /**
   * @param plan (unused)
   * @param context (unused)
   * @return true
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Checks if the input can be transformed according to this rule, and if so, pushes the transformed expression
   *   onto the provided output vector.
   * @param input the input expression potentially being transformed
   * @param transformed the output vector
   * @param context the context for this optimization
   */
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
  /**
   * Constructor.
   */
  TransitiveClosureConstantTransform();

  /**
   * Returns the type of promise for this rule.
   * @param gexpr the group expression (unused)
   * @return an indicator that this is a high-priority rule
   */
  RulePromise Promise(GroupExpression *gexpr) const override;

  /**
   * @param plan (unused)
   * @param context (unused)
   * @return true
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Checks if the input can be transformed according to this rule, and if so, pushes the transformed expression
   *   onto the provided output vector.
   * @param input the input expression potentially being transformed
   * @param transformed the output vector
   * @param context the context for this optimization
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/*
 * Comparison Intersection: If the same two expressions are being compared in two separate branches of an
 *   AND clause, we can rewrite it into a single comparison depending on what the two comparators are.
 * Examples:
 *   "A >= B AND A != B" --> "A > B"
 *    "A > B AND A < B"  --> "FALSE"
 */
class ComparisonIntersection : public Rule {
 public:
  /**
   * Constructor. Constructs a rewrite of the form "(A [c1] B) AND (A [c2] B)" --> "A [c3] B"
   *   for comparators c1, c2, and c3. Alternatively, if c3 is INVALID, then the rewrite just goes
   *   to "FALSE".
   * @param type the type of this rule
   * @param left_comparison c1
   * @param right_comparison c2
   * @param result_comparison c3
   */
  ComparisonIntersection(RuleType type, parser::ExpressionType left_comparison, parser::ExpressionType right_comparison,
                         parser::ExpressionType result_comparison);

  /**
   * Returns the type of promise for this rule.
   * @param gexpr the group expression (unused)
   * @return an indicator that this is a high-priority rule
   */
  RulePromise Promise(GroupExpression *gexpr) const override;

  /**
   * @param plan (unused)
   * @param context (unused)
   * @return true
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Checks if the input can be transformed according to this rule, and if so, pushes the transformed expression
   *   onto the provided output vector.
   * @param input the input expression potentially being transformed
   * @param transformed the output vector
   * @param context the context for this optimization
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;

 protected:
  /**
   * Comparison type of the input's left branch
   */
  parser::ExpressionType left_compare_type_;

  /**
   * Comparison type of the input's right branch
   */
  parser::ExpressionType right_compare_type_;

  /**
   * Comparison type of the transformed expression
   */
  parser::ExpressionType result_compare_type_;
};

}  // namespace terrier::optimizer