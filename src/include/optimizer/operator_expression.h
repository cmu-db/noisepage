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

  void PushChild(OperatorExpression* child_op) { children_.push_back(child_op); }

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
