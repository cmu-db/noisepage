#pragma once
#include <memory>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "type/type_id.h"

namespace terrier::parser {

/**
 * Represents a parameter's offset in an expression.
 */
class ParameterValueExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new ParameterValueExpression with the given offset.
   * @param value_idx the offset of the parameter
   */
  explicit ParameterValueExpression(const uint32_t value_idx)
      : AbstractExpression(ExpressionType::VALUE_PARAMETER, type::TypeId::INTEGER, {}), value_idx_(value_idx) {}

  /**
   * Default constructor for deserialization
   */
  ParameterValueExpression() = default;

  std::shared_ptr<AbstractExpression> Copy() const override {
    return std::make_shared<ParameterValueExpression>(*this);
  }

  /**
   * @return offset in the expression
   */
  uint32_t GetValueIdx() const { return value_idx_; }

  bool operator==(const AbstractExpression &rhs) const override {
    if (!AbstractExpression::operator==(rhs)) return false;
    auto const &other = dynamic_cast<const ParameterValueExpression &>(rhs);
    return GetValueIdx() == other.GetValueIdx();
  }

  /**
   * @return expression serialized to json
   */
  nlohmann::json ToJson() const override {
    nlohmann::json j = AbstractExpression::ToJson();
    j["value_idx"] = value_idx_;
    return j;
  }

  /**
   * @param j json to deserialize
   */
  void FromJson(const nlohmann::json &j) override {
    AbstractExpression::FromJson(j);
    value_idx_ = j.at("value_idx").get<uint32_t>();
  }

 private:
  // TODO(Tianyu): Can we get a better name for this?
  uint32_t value_idx_;
};

DEFINE_JSON_DECLARATIONS(ParameterValueExpression);

}  // namespace terrier::parser
