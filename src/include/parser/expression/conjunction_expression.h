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
 * Represents a logical conjunction expression.
 */
class ConjunctionExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new conjunction expression.
   * @param cmp_type type of conjunction
   * @param left left operand
   * @param right right operand
   */
  explicit ConjunctionExpression(const ExpressionType cmp_type,
                                 std::vector<std::unique_ptr<AbstractExpression>> *children)
      : AbstractExpression(cmp_type, type::TypeId::BOOLEAN, std::move(*children)) {}

  AbstractExpression *Copy() const override {
    std::vector<std::unique_ptr<AbstractExpression>> children;
    children.emplace_back(std::unique_ptr<AbstractExpression>(GetChild(0)));
    children.emplace_back(std::unique_ptr<AbstractExpression>(GetChild(1)));
    return new ConjunctionExpression(GetExpressionType(), &children);
  }
};

}  // namespace expression
}  // namespace parser
}  // namespace terrier
