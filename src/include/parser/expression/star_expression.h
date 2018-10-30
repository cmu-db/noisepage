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

  std::unique_ptr<AbstractExpression> Copy() const override {
    // TODO(Tianyu): This really should be a singleton object
    return std::make_unique<StarExpression>(*this);
  }

  std::shared_ptr<sql::SqlAbstractExpression> Accept(SqlNodeVisitor *v) override { v->Visit(this); }
};

}  // namespace terrier::parser
