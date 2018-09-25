#pragma once

#include "type/expression/abstract_expression.h"
#include "type/expression/expression_defs.h"
#include "type/type_id.h"

namespace terrier {
namespace type {
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
  explicit AggregateExpression(ExpressionType type, AbstractExpression *child)
      : AbstractExpression(type, TypeId::INVALID, child) {}

  AbstractExpression *Copy() const override { return new AggregateExpression(*this); }
  // TODO(WAN): worry about distinct later.
  // I suspect not all aggregators need a distinct flag, so maybe a separate class makes more sense?
};

}  // namespace expression
}  // namespace type
}  // namespace terrier
