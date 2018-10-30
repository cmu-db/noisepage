#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/sql_node_visitor.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"
#include "type/type_id.h"

namespace terrier::parser {

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
  ComparisonExpression(const ExpressionType cmp_type, std::vector<std::shared_ptr<AbstractExpression>> &&children)
      : AbstractExpression(cmp_type, type::TypeId::BOOLEAN, std::move(children)) {}

  std::unique_ptr<AbstractExpression> Copy() const override { return std::make_unique<ComparisonExpression>(*this); }

  std::vector<std::shared_ptr<sql::SqlAbstractExpression>> Accept(SqlNodeVisitor *v) override { return v->Visit(this); }
};

}  // namespace terrier::parser
