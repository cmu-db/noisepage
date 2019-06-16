#pragma once

#include <memory>
#include <vector>
#include "common/hash_util.h"
#include "parser/expression/abstract_expression.h"
#include "type/transient_value.h"

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
  explicit ConstantValueExpression(const type::TransientValue &value)
      : AbstractExpression(ExpressionType::VALUE_CONSTANT, value.Type(), {}), value_(value) {}

  /**
   * Default constructor for deserialization
   */
  ConstantValueExpression() = default;

  ~ConstantValueExpression() override = default;

  common::hash_t Hash() const override {
    return common::HashUtil::CombineHashes(AbstractExpression::Hash(), value_.Hash());
  }

  bool operator==(const AbstractExpression &other) const override {
    if (!AbstractExpression::operator==(other)) return false;
    auto const &const_expr = dynamic_cast<const ConstantValueExpression &>(other);
    return value_ == const_expr.GetValue();
  }

  const AbstractExpression *Copy() const override { return new ConstantValueExpression(type::TransientValue(value_)); }

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   */
  const AbstractExpression *CopyWithChildren(std::vector<const AbstractExpression *> children) const override {
    TERRIER_ASSERT(children.empty(), "COnstantValueExpression should have 0 children");
    return Copy();
  }

  /**
   * @return the constant value stored in this expression
   */
  type::TransientValue GetValue() const { return value_; }

  /**
   * @return expression serialized to json
   * @note ToJson is a private member of TransientValue, ConstantValueExpression can access it because it
   * is a friend class of TransientValue.
   * @see TransientValue for why ToJson is made private
   */
  nlohmann::json ToJson() const override {
    nlohmann::json j = AbstractExpression::ToJson();
    j["value"] = value_;
    return j;
  }

  /**
   * @param j json to deserialize
   * @note FromJson is a private member of TransientValue, ConstantValueExpression can access it because it
   * is a friend class of TransientValue.
   * @see TransientValue for why FromJson is made private
   */
  void FromJson(const nlohmann::json &j) override {
    AbstractExpression::FromJson(j);
    value_ = j.at("value").get<type::TransientValue>();
  }

 private:
  type::TransientValue value_;
};

DEFINE_JSON_DECLARATIONS(ConstantValueExpression);

}  // namespace terrier::parser
