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
  explicit OperatorExpression(Operator op, std::vector<std::unique_ptr<OperatorExpression>> &&children)
      : op_(std::move(op)), children_(std::move(children)) {}

  /**
   * Copy
   */
  std::unique_ptr<OperatorExpression> Copy() {
    std::vector<std::unique_ptr<OperatorExpression>> child;
    for (const auto &op : children_) {
      child.emplace_back(op->Copy());
    }
    return std::make_unique<OperatorExpression>(Operator(op_), std::move(child));
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
      auto &child = children_[idx];
      auto &other_child = other.children_[idx];
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
   * Move constructor
   * @param op other to construct from
   */
  OperatorExpression(OperatorExpression &&op) noexcept : op_(std::move(op.op_)), children_(std::move(op.children_)) {}

  /**
   * @return vector of children
   */
  std::vector<common::ManagedPointer<OperatorExpression>> GetChildren() const {
    std::vector<common::ManagedPointer<OperatorExpression>> result;
    result.reserve(children_.size());
    for (auto &i : children_) result.emplace_back(i);
    return result;
  }

  /**
   * @return underlying operator
   */
  const Operator &GetOp() const { return op_; }

  /**
   * Add a operator expression as child
   * @param child_op The operator expression to be added as child
   */
  void PushChild(std::unique_ptr<OperatorExpression> child_op) { children_.emplace_back(std::move(child_op)); }

 private:
  /**
   * Underlying operator
   */
  Operator op_;

  /**
   * Vector of children
   */
  std::vector<std::unique_ptr<OperatorExpression>> children_;
};

}  // namespace terrier::optimizer
