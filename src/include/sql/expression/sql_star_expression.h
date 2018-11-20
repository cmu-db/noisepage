#pragma once

#include <memory>
#include <vector>
#include "parser/expression_defs.h"
#include "sql/expression/sql_abstract_expression.h"
#include "type/type_id.h"

namespace terrier::sql {
/**
 * Represents a star, e.g. COUNT(*).
 */
class SqlStarExpression : public SqlAbstractExpression {
 public:
  /**
   * Instantiates a new star expression, e.g. as in COUNT(*)
   */
  SqlStarExpression() : SqlAbstractExpression(parser::ExpressionType::STAR, type::TypeId::INVALID, {}) {}

  std::unique_ptr<SqlAbstractExpression> Copy() const override {
    // TODO(Tianyu): This really should be a singleton object
    return std::make_unique<SqlStarExpression>(*this);
  }

  class Builder : public SqlAbstractExpression::Builder<Builder> {
   public:
    std::shared_ptr<SqlStarExpression> Build() {
      return std::shared_ptr<SqlStarExpression>(
          new SqlStarExpression();
    }
  };
};

}  // namespace terrier::sql
