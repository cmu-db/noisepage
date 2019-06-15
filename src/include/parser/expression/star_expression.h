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

  ~StarExpression() override = default;

  const AbstractExpression *Copy() const override {
    // TODO(Tianyu): This really should be a singleton object
    return new StarExpression();
  }

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   */
  const AbstractExpression *CopyWithChildren(std::vector<const AbstractExpression *> children) const override {
    TERRIER_ASSERT(children.empty(), "StarExpression should have 0 children");
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

DEFINE_JSON_DECLARATIONS(StarExpression);

}  // namespace terrier::parser
