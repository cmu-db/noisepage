#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"

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
  ConjunctionExpression(const ExpressionType cmp_type, std::vector<std::shared_ptr<AbstractExpression>> &&children)
      : AbstractExpression(cmp_type, type::TypeId::BOOLEAN, std::move(children)) {}

  /**
   * Default constructor for deserialization
   */
  ConjunctionExpression() = default;

  std::shared_ptr<AbstractExpression> Copy() const override { return std::make_shared<ConjunctionExpression>(*this); }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }
};

DEFINE_JSON_DECLARATIONS(ConjunctionExpression);

}  // namespace terrier::parser
