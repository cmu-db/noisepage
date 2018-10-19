#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression/expression_defs.h"
#include "type/type_id.h"

namespace terrier {
namespace parser {

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

  std::unique_ptr<AbstractExpression> Copy() const override {
    std::vector<std::shared_ptr<AbstractExpression>> children;
    children.emplace_back(GetChild(0)->Copy());
    children.emplace_back(GetChild(1)->Copy());
    return std::unique_ptr<AbstractExpression>(new ConjunctionExpression(GetExpressionType(), std::move(children)));
  }
};

}  // namespace parser
}  // namespace terrier
