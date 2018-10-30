#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "sql/expression/sql_abstract_expression.h"
#include "parser/expression_defs.h"
#include "type/type_id.h"

namespace terrier::sql {

/**
 * Represents a logical conjunction expression.
 */
class ConjunctionExpression : public SqlAbstractExpression {
 public:
  /**
   * Instantiates a new conjunction expression.
   * @param cmp_type type of conjunction
   * @param children vector containing exactly two children, left then right
   */
  ConjunctionExpression(const parser::ExpressionType cmp_type, std::vector<std::shared_ptr<SqlAbstractExpression>> &&children)
      : SqlAbstractExpression(cmp_type, type::TypeId::BOOLEAN, std::move(children)) {}

  std::unique_ptr<SqlAbstractExpression> Copy() const override { return std::make_unique<ConjunctionExpression>(*this); }
};

}  // namespace terrier::sql
