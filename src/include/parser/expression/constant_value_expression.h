#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "binder/bind_node_visitor.h"
#include "common/hash_util.h"
#include "parser/expression/abstract_expression.h"
#include "type/transient_value.h"
#include "type/transient_value_factory.h"
#include "type/transient_value_peeker.h"
#include "util/time_util.h"

namespace terrier::parser {
/**
 * ConstantValueExpression represents a constant, e.g. numbers, string literals.
 */
class ConstantValueExpression : public AbstractExpression {
 public:
  /**
   * Instantiate a new constant value expression.
   * @param value value to be held
   */
  explicit ConstantValueExpression(type::TransientValue value)
      : AbstractExpression(ExpressionType::VALUE_CONSTANT, value.Type(), {}), value_(std::move(value)) {}

  /** Default constructor for deserialization. */
  ConstantValueExpression() = default;

  common::hash_t Hash() const override {
    return common::HashUtil::CombineHashes(AbstractExpression::Hash(), value_.Hash());
  }

  bool operator==(const AbstractExpression &other) const override {
    if (!AbstractExpression::operator==(other)) return false;
    auto const &const_expr = dynamic_cast<const ConstantValueExpression &>(other);
    return value_ == const_expr.GetValue();
  }

  /**
   * Copies this ConstantValueExpression
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> Copy() const override;

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   * @returns copy of this with new children
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override {
    TERRIER_ASSERT(children.empty(), "COnstantValueExpression should have 0 children");
    return Copy();
  }

  void DeriveReturnValueType() override { return_value_type_ = GetValue().Type(); }

  void DeriveExpressionName() override;

  /** @return the constant value stored in this expression */
  type::TransientValue GetValue() const { return value_; }

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }

  /**
   * @return expression serialized to json
   * @note TransientValue::ToJson() is private, ConstantValueExpression is a friend
   * @see TransientValue for why TransientValue::ToJson is private
   */
  nlohmann::json ToJson() const override;

  /**
   * @param j json to deserialize
   * @note TransientValue::FromJson() is private, ConstantValueExpression is a friend
   * @see TransientValue for why TransientValue::FromJson is private
   */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  friend class binder::BindNodeVisitor; /* value_ may be modified, e.g., when parsing dates. */
  /** The constant held inside this ConstantValueExpression. */
  type::TransientValue value_;
};

DEFINE_JSON_HEADER_DECLARATIONS(ConstantValueExpression);

}  // namespace terrier::parser
