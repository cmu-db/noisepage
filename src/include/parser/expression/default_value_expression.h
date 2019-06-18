#pragma once

#include <memory>
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {

/**
 * Represents a default value, e.g. in an INSERT. Note that the return value type is unspecified,
 * this expression should be replaced by the binder.
 */
class DefaultValueExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new default value expression.
   */
  DefaultValueExpression() : AbstractExpression(ExpressionType::VALUE_DEFAULT, type::TypeId::INVALID, {}) {}

  const AbstractExpression* Copy() const override { return new DefaultValueExpression(); }

  const AbstractExpression* CopyWithChildren(std::vector<const AbstractExpression*> children) const override {
    TERRIER_ASSERT(children.empty(), "DefaultValueExpression should have 0 children");
    return Copy();
  }

  /**
   * @return expression serialized to json
   */
  nlohmann::json ToJson() const override {
    nlohmann::json j = AbstractExpression::ToJson();
    return j;
  }

  /**
   * @param j json to deserialize
   */
  void FromJson(const nlohmann::json &j) override { AbstractExpression::FromJson(j); }
};

DEFINE_JSON_DECLARATIONS(DefaultValueExpression);

}  // namespace terrier::parser
