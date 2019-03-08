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
      : AbstractExpression(ExpressionType::OPERATOR_CAST, type, std::move(children)) {}

  std::unique_ptr<AbstractExpression> Copy() const override { return std::make_unique<TypeCastExpression>(*this); }
};

}  // namespace terrier::parser
