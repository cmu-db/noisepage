#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression/expression_defs.h"
#include "type/type_id.h"

namespace terrier {
namespace parser {
namespace expression {

/**
 * Represents a logical comparison expression.
 */
class ComparisonExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new comparison expression.
   * @param cmp_type type of comparison
   * @param children vector containing exactly two children, left then right
   */
  explicit ComparisonExpression(const ExpressionType cmp_type,
                                std::vector<std::unique_ptr<AbstractExpression>> *children)
      : AbstractExpression(cmp_type, type::TypeId::BOOLEAN, std::move(*children)) {}

  AbstractExpression *Copy() const override {
    std::vector<std::unique_ptr<AbstractExpression>> children;
    children.emplace_back(std::unique_ptr<AbstractExpression>(GetChild(0)));
    children.emplace_back(std::unique_ptr<AbstractExpression>(GetChild(1)));
    return new ComparisonExpression(GetExpressionType(), &children);
  }
};

}  // namespace expression
}  // namespace parser
}  // namespace terrier
