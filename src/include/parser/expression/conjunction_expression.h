#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"
#include "type/type_id.h"

namespace terrier::parser {

/**
 * Represents a logical conjunction expression.
 */
class ConjunctionExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new conjunction expression.
   * @param cmp_type type of conjunction
   * @param children vector containing exactly two children, left then right
   */
  ConjunctionExpression(const ExpressionType cmp_type, std::vector<const AbstractExpression *> children)
      : AbstractExpression(cmp_type, type::TypeId::BOOLEAN, std::move(children)) {}

  /**
   * Default constructor for deserialization
   */
  ConjunctionExpression() = default;

  ~ConjunctionExpression() override = default;

  const AbstractExpression *Copy() const override {
    std::vector<const AbstractExpression *> children;
    for (const auto *child : children_) {
      children.emplace_back(child->Copy());
    }
    return new ConjunctionExpression(GetExpressionType(), children);
  }

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   */
  const AbstractExpression *CopyWithChildren(std::vector<const AbstractExpression *> children) const override {
    return new ConjunctionExpression(GetExpressionType(), children);
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

DEFINE_JSON_DECLARATIONS(ConjunctionExpression);

}  // namespace terrier::parser
