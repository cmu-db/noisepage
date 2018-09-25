#pragma once

#include <string>
#include "type/expression/abstract_expression.h"
#include "type/expression/expression_defs.h"
#include "type/type_id.h"

namespace terrier {
namespace type {
namespace expression {

/**
 * Represents a logical tuple value.
 */
class TupleValueExpression : public AbstractExpression {
 public:
  /**
   * Creates a tuple value expression with the given column and table name.
   * TODO(WAN): I feel like this should be renamed. Maybe parameters reordered too.
   */
  explicit TupleValueExpression(const std::string &&col_name, std::string &&table_name)
      : AbstractExpression(ExpressionType::VALUE_TUPLE, TypeId::INVALID),
        col_name_(col_name),
        table_name_(table_name) {}

 private:
  const std::string col_name_;
  const std::string table_name_;
};

}  // namespace expression
}  // namespace type
}  // namespace terrier
