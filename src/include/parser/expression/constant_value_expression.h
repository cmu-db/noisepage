#pragma once

#include <memory>
#include <vector>
#include "common/hash_util.h"
#include "parser/expression/abstract_expression.h"
#include "parser/sql_node_visitor.h"
#include "type/value.h"

namespace terrier::parser {

/**
 * Represents a logical constant expression.
 */
class ConstantValueExpression : public AbstractExpression {
 public:
  /**
   * Instantiate a new constant value expression.
   * @param value value to be held
   */
  explicit ConstantValueExpression(const type::Value &value)
      : AbstractExpression(ExpressionType::VALUE_CONSTANT, value.GetType(), {}), value_(value) {}

  common::hash_t Hash() const override {
    return common::HashUtil::CombineHashes(AbstractExpression::Hash(), value_.Hash());
  }

  bool operator==(const AbstractExpression &other) const override {
    if (GetExpressionType() != other.GetExpressionType()) {
      return false;
    }
    auto const &const_expr = dynamic_cast<const ConstantValueExpression &>(other);
    return value_ == const_expr.GetValue();
  }

  std::unique_ptr<AbstractExpression> Copy() const override { return std::make_unique<ConstantValueExpression>(*this); }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return the constant value stored in this expression
   */
  type::Value GetValue() const { return value_; }

 private:
  type::Value value_;
};

}  // namespace terrier::parser
