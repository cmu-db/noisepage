#pragma once

#include <memory>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"
#include "type/type_id.h"

namespace terrier::parser {
/**
 * Represents a star, e.g. COUNT(*).
 */
class StarExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new star expression, e.g. as in COUNT(*)
   */
  StarExpression() : AbstractExpression(ExpressionType::STAR, type::TypeId::INVALID, {}) {}

  std::shared_ptr<AbstractExpression> Copy() const override {
    // TODO(Tianyu): This really should be a singleton object
    return std::make_shared<StarExpression>(*this);
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

DEFINE_JSON_DECLARATIONS(StarExpression);

}  // namespace terrier::parser
