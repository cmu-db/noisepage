#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "binder/sql_node_visitor.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"
#include "type/type_id.h"

namespace terrier::parser {

/**
 * An AggregateExpression is only used for parsing, planning and optimizing.
 */
class AggregateExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new aggregate expression.
   * @param type type of aggregate expression
   * @param children children to be added
   */
  AggregateExpression(ExpressionType type, std::vector<std::shared_ptr<AbstractExpression>> &&children)
      : AbstractExpression(type, type::TypeId::INVALID, std::move(children)) {}

  std::unique_ptr<AbstractExpression> Copy() const override { return std::make_unique<AggregateExpression>(*this); }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }
};

}  // namespace terrier::parser
