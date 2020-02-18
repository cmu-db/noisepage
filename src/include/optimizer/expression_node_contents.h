#pragma once

#include "common/managed_pointer.h"
#include "optimizer/abstract_optimizer_node_contents.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"

namespace terrier::optimizer {

class ExpressionNodeContents : public AbstractOptimizerNodeContents {
 public:
  // Default constructors
  ExpressionNodeContents() = default;
  ExpressionNodeContents(const ExpressionNodeContenst &other) : AbstractOptimizerNodeContents() { expr_ = other.expr_; }

  /**
   * Constructor based on an abstract expression pointer -- wraps that pointer
   * in an ExpressionNodeContents object
   * @param expr The expression to be wrapped
   */
  ExpressionNodeContents(common::ManagedPointer<parser::AbstractExpression> expr) { expr_ = expr; }

  OpType GetOpType() const { return OpType::UNDEFINED; }

  parser::ExpressionType GetExpType() const {
    if (IsDefined()) {
      return expr->GetExpressionType();
    }
    return parser::ExpressionType::INVALID;
  }

  common::ManagedPointer<parser::AbstractExpression> GetExpr() const { return expr_; }

  // Dummy Accept
  void Accept(OperatorVisitor *v) const { (void)v; }

  bool IsLogical() const { return true; }

  bool IsPhysical() const { return false; }

  std::string GetName() const {
    if (IsDefined()) {
      return expr_->GetExpressionName();
    }
    throw OptimizerException("Undefined expression name.");
  }

  common::hash_t Hash() const {
    if (IsDefined()) {
      return expr_->Hash();
    }
    return 0;
  }

  bool operator==(const AbstractOptimizerNodeContents &r) {
    if (r.GetExpType() != parser::ExpressionType::INVALID) {
      const ExpressionNodeContents &contents = dynamic_cast<const ExpressionNodeContents &>(r);
      return (*this == contents);
    }
    return false;
  }

  bool operator==(const ExpressionNodeContents &r) {
    // TODO(): proper equality check
    // Equality check relies on performing the following:
    // - Check each node's ExpressionType
    // - Check other parameters for a given node
    // We believe that in terrier so long as the AbstractExpression
    // are children-less, operator== provides sufficient checking.
    // The reason behind why the children-less guarantee is required,
    // is that the "real" children are actually tracked by the
    // ExpressionNode class.
    if (IsDefined() && r.IsDefined()) {
      return false;
    } else if (!IsDefined() && !r.IsDefined()) {
      return true;
    }
    return false;
  }

  bool IsDefined() const { return (expr_ != nullptr); }

  common::ManagedPointer<parser::AbstractExpression> CopyWithChildren(
      std::vector<common::ManagedPointer<parser::AbstractExpression>> children);

 private:
  common::ManagedPointer<parser::AbstractExpression> expr_;
};

}  // namespace terrier::optimizer