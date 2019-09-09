#pragma once

#include "optimizer/operator_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace terrier::optimizer {

/**
 * This class is used to represent nodes in the operator tree. The operator tree is generated
 * by the binder by visiting the abstract syntax tree (AST) produced by the parser and servers
 * as the input to the query optimizer.
 */
class OperatorExpression {
 public:
  /**
   * Create an OperatorExpression
   * @param op an operator to bind to this OperatorExpression node
   * @param children children of this OperatorExpression
   */
  explicit OperatorExpression(Operator &&op, std::vector<OperatorExpression *> &&children)
      : op_(op), children_(std::move(children)) {}

  /**
   * Copy
   */
  OperatorExpression *Copy() {
    std::vector<OperatorExpression *> child;
    for (auto op : children_) {
      child.push_back(op->Copy());
    }
    return new OperatorExpression(Operator(op_), std::move(child));
  }

  /**
   * Equality comparison
   * @param other OperatorExpression to compare against
   * @returns true if equal
   */
  bool operator==(const OperatorExpression &other) const {
    if (op_ != other.op_) return false;
    if (children_.size() != other.children_.size()) return false;

    for (size_t idx = 0; idx < children_.size(); idx++) {
      auto child = children_[idx];
      auto other_child = other.children_[idx];
      TERRIER_ASSERT(child != nullptr, "OperatorExpression should not have null children");
      TERRIER_ASSERT(other_child != nullptr, "OperatorExpression should not have null children");

      if (*child != *other_child) return false;
    }

    return true;
  }

  /**
   * Not equal comparison
   * @param other OperatorExpression to compare against
   * @returns true if not equal
   */
  bool operator!=(const OperatorExpression &other) const { return !(*this == other); }

  /**
   * @return vector of children
   */
  const std::vector<OperatorExpression *> &GetChildren() const { return children_; }

  /**
   * @return underlying operator
   */
  const Operator &GetOp() const { return op_; }

  /**
   * destructor that delete its children
   */
  ~OperatorExpression() {
    for (auto child : children_) delete child;
  }

 private:
  /**
   * Underlying operator
   */
  Operator op_;

  /**
   * Vector of children
   */
  std::vector<OperatorExpression *> children_;
};

}  // namespace terrier::optimizer
