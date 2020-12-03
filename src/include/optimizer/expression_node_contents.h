#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/error/exception.h"
#include "common/managed_pointer.h"
#include "optimizer/abstract_optimizer_node_contents.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"

namespace noisepage::transaction {
class TransactionContext;
}

namespace noisepage::optimizer {

/**
 * A wrapper for an AbstractExpression for the query rewriter.
 */
class ExpressionNodeContents : public AbstractOptimizerNodeContents {
 public:
  /**
   * Default constructor
   */
  ExpressionNodeContents() = default;

  /**
   * Copy constructor
   * @param other The other node contents we're copying from
   */
  ExpressionNodeContents(const ExpressionNodeContents &other) : AbstractOptimizerNodeContents(other) {
    expr_ = other.expr_;
  }

  /**
   * Constructor based on an abstract expression pointer -- wraps that pointer
   * in an ExpressionNodeContents object
   * @param expr The expression to be wrapped
   */
  explicit ExpressionNodeContents(common::ManagedPointer<parser::AbstractExpression> expr) { expr_ = expr; }

  /**
   * @return The invalid OpType, since this is an ExpressionNodeContents
   */
  OpType GetOpType() const override { return OpType::UNDEFINED; }

  /**
   * @return This expression node content's expression type
   */
  parser::ExpressionType GetExpType() const override {
    if (IsDefined()) {
      return expr_->GetExpressionType();
    }
    return parser::ExpressionType::INVALID;
  }

  /**
   * @return The contained expression within the
   */
  common::ManagedPointer<parser::AbstractExpression> GetExpr() const { return expr_; }

  /**
   * Dummy Accept method
   * @param v An OperatorVisitor. Goes unused
   */
  void Accept(common::ManagedPointer<OperatorVisitor> v) const override;

  /**
   * @return whether or not this expression is logical (which it always is)
   */
  bool IsLogical() const override { return true; }

  /**
   * @return whether or not this expression is physical (which it always isn't)
   */
  bool IsPhysical() const override { return false; }

  /**
   * @return The name of this expression
   */
  std::string GetName() const override {
    if (IsDefined()) {
      return expr_->GetExpressionName();
    }
    throw OPTIMIZER_EXCEPTION("Undefined expression name.");
  }

  /**
   * @return The hash of this ExpressionNodeContents
   */
  common::hash_t Hash() const override {
    if (IsDefined()) {
      return expr_->Hash();
    }
    return 0;
  }

  /**
   * @param r The other (abstract) node contents to be compared
   * @return Whether or not this is equal to the other node contents
   */
  bool operator==(const AbstractOptimizerNodeContents &r) override {
    if (r.GetExpType() != parser::ExpressionType::INVALID) {
      const auto &contents = dynamic_cast<const ExpressionNodeContents &>(r);
      return (*this == contents);
    }
    return false;
  }

  /**
   * @param r The other (expression) node contents to be compared
   * @return Whether or not this is equal to the other node contents
   */
  bool operator==(const ExpressionNodeContents &r) {
    // TODO(esargent): proper equality check
    // Equality check relies on performing the following:
    // - Check each node's ExpressionType
    // - Check other parameters for a given node
    // We believe that in noisepage so long as the AbstractExpression
    // are children-less, operator== provides sufficient checking.
    // The reason behind why the children-less guarantee is required,
    // is that the "real" children are actually tracked by the
    // ExpressionNode class.
    if (IsDefined() && r.IsDefined()) {
      return false;
    }
    return !IsDefined() && !r.IsDefined();
  }

  /**
   * @return Whether or not this node contents contains a defined expression
   */
  bool IsDefined() const override { return (expr_ != nullptr); }

  /**
   * Produces a copy of this expression node contents, with the provided
   * child expressions
   * @param children A vector of the copy expression's children
   * @return A copied expression with the provided children.
   */
  common::ManagedPointer<parser::AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<parser::AbstractExpression>> children);

  /**
   * Registers this expression contents with the provided transaction context
   *   so that copies can be managed by this transaction's lifetime (i.e.
   *   copies get deallocated on commit/abort)
   * @param txn the transaction context to register with
   */
  void RegisterWithTxnContext(transaction::TransactionContext *txn) { txn_ = txn; }

 private:
  common::ManagedPointer<parser::AbstractExpression> expr_{};

  transaction::TransactionContext *txn_;
};

}  // namespace noisepage::optimizer
