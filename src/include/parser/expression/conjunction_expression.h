#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {

/**
 * ConjunctionExpression represents logical conjunctions like ANDs and ORs.
 */
class ConjunctionExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new conjunction expression.
   * @param cmp_type type of conjunction
   * @param children vector containing exactly two children, left then right
   */
  ConjunctionExpression(const ExpressionType cmp_type, std::vector<std::unique_ptr<AbstractExpression>> &&children)
      : AbstractExpression(cmp_type, type::TypeId::BOOLEAN, std::move(children)) {}

  /** Default constructor for deserialization. */
  ConjunctionExpression() = default;

  std::unique_ptr<AbstractExpression> Copy() const override {
    std::vector<std::unique_ptr<AbstractExpression>> children;
    for (const auto &child : GetChildren()) {
      children.emplace_back(child->Copy());
    }
    auto expr = std::make_unique<ConjunctionExpression>(GetExpressionType(), std::move(children));
    expr->SetMutableStateForCopy(*this);
    return expr;
  }

  void Accept(SqlNodeVisitor *v, ParseResult *parse_result) override { v->Visit(this, parse_result); }
};

DEFINE_JSON_DECLARATIONS(ConjunctionExpression);

}  // namespace terrier::parser
