#pragma once

#include <memory>
#include <vector>

#include "optimizer/operator_node_contents.h"

namespace terrier::optimizer {

/**
 * Class defining a Pattern used for binding
 */
class Pattern {
 public:
  /**
   * Creates a new operator-based pattern
   * @param op Operator that node should match
   */
  explicit Pattern(OpType op) : op_type_(op), exp_type_(parser::ExpressionType::INVALID) {}

  /**
   * Creates a new expression-based pattern
   * @param exp  ExpressionType the node should match
   */
  explicit Pattern(parser::ExpressionType exp) : op_type_(OpType::UNDEFINED), exp_type_(exp) {}

  /**
   * Destructor. Deletes all children
   */
  ~Pattern() {
    for (auto child : children_) {
      delete child;
    }
  }

  /**
   * Adds a child to the pattern.
   * Memory control of child passes to this pattern.
   *
   * @param child Pointer to child
   */
  void AddChild(Pattern *child) { children_.push_back(child); }

  /**
   * Gets a vector of the children
   * @returns managed children of the pattern node
   */
  const std::vector<Pattern *> &Children() const { return children_; }

  /**
   * Gets number of children
   * @returns number of children
   */
  size_t GetChildPatternsSize() const { return children_.size(); }

  /**
   * Gets the operator this Pattern supposed to represent
   * @returns OpType that Pattern matches against
   */
  OpType GetOpType() const { return op_type_; }

  /**
   * Gets the expression type this Pattern is supposed to represent
   * @return ExpressionType the pattern matches against
   */
  parser::ExpressionType GetExpType() const { return exp_type_; }

 private:
  /**
   * Target Node Type (Operator-based)
   */
  OpType op_type_;

  /**
   * Target Node Type (Expression-based)
   */
  parser::ExpressionType exp_type_;

  /**
   * Pattern Children
   */
  std::vector<Pattern *> children_;
};

}  // namespace terrier::optimizer
