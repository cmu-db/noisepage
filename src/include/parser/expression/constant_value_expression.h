#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "common/hash_util.h"
#include "parser/expression/abstract_expression.h"
#include "type/transient_value.h"

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

  void DeriveExpressionName() override {
    if (!this->GetAlias().empty()) {
      this->SetExpressionName(this->GetAlias());
    } else {
      this->SetExpressionName(value_.ToString());
    }
  }

  std::unique_ptr<AbstractExpression> Copy() const override {
    return std::make_unique<ConstantValueExpression>(GetValue());
  }

  /** @return the constant value stored in this expression */
  type::TransientValue GetValue() const { return value_; }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return expression serialized to json
   * @note TransientValue::ToJson() is private, ConstantValueExpression is a friend
   * @see TransientValue for why TransientValue::ToJson is private
   */
  nlohmann::json ToJson() const override {
    nlohmann::json j = AbstractExpression::ToJson();
    j["value"] = value_;
    return j;
  }

  /**
   * @param j json to deserialize
   * @note TransientValue::FromJson() is private, ConstantValueExpression is a friend
   * @see TransientValue for why TransientValue::FromJson is private
   */
  void FromJson(const nlohmann::json &j) override {
    AbstractExpression::FromJson(j);
    value_ = j.at("value").get<type::TransientValue>();
  }

 private:
  /** The constant held inside this ConstantValueExpression. */
  type::TransientValue value_;
};

DEFINE_JSON_DECLARATIONS(ConstantValueExpression);

}  // namespace terrier::parser
