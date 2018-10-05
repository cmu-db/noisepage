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
 * An AggregateExpression is only used for parsing, planning and optimizing.
 */
class AggregateExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new aggregate expression.
   * @param type type of aggregate expression
   * @param child child to be added
   */
  explicit AggregateExpression(ExpressionType type, std::vector<std::unique_ptr<AbstractExpression>> &&children)
      : AbstractExpression(type, type::TypeId::INVALID, std::move(children)) {}

  AbstractExpression *Copy() const override { return new AggregateExpression(*this); }
};

}  // namespace expression
}  // namespace parser
}  // namespace terrier
