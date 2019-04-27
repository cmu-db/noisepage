#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"
#include "type/type_id.h"

namespace terrier::parser {
/**
 * Represents a type cast expression.
 */
class TypeCastExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new type cast expression.
   */
  TypeCastExpression(type::TypeId type, std::vector<std::shared_ptr<AbstractExpression>> &&children)
      : AbstractExpression(ExpressionType::OPERATOR_CAST, type, std::move(children)), type_(type) {}

  /**
   * Default constructor for deserialization
   */
  TypeCastExpression() = default;

  std::shared_ptr<AbstractExpression> Copy() const override { return std::make_shared<TypeCastExpression>(*this); }

  /**
   * @return The type this node casts to
   */
  type::TypeId GetType() const { return type_; }

  bool operator==(const AbstractExpression &rhs) const override {
    if (!AbstractExpression::operator==(rhs)) return false;
    auto const &other = dynamic_cast<const TypeCastExpression &>(rhs);
    return GetType() == other.GetType();
  }

  /**
   * @return expression serialized to json
   */
  nlohmann::json ToJson() const override {
    nlohmann::json j = AbstractExpression::ToJson();
    j["type"] = type_;
    return j;
  }

  /**
   * @param j json to deserialize
   */
  void FromJson(const nlohmann::json &j) override {
    AbstractExpression::FromJson(j);
    type_ = j.at("type").get<type::TypeId>();
  }

 private:
  type::TypeId type_;
};

DEFINE_JSON_DECLARATIONS(TypeCastExpression);

}  // namespace terrier::parser
