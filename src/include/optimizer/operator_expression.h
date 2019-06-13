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
    std::vector<OperatorExpression*> child;
    for (auto op : children_) { child.push_back(op->Copy()); }

    // Copy constructor here
    Operator op_copy = op_;
    return new OperatorExpression(std::move(op_copy), std::move(child));
  }

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
