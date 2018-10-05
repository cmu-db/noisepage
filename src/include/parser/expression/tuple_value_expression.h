#pragma once

#include <memory>
#include <string>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression/expression_defs.h"
#include "type/type_id.h"

namespace terrier {
namespace parser {
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
      : AbstractExpression(ExpressionType::VALUE_TUPLE, type::TypeId::INVALID,
                           std::vector<std::unique_ptr<AbstractExpression>>()),
        col_name_(col_name),
        table_name_(table_name) {}

 private:
  const std::string col_name_;
  const std::string table_name_;
};

}  // namespace expression
}  // namespace parser
}  // namespace terrier
