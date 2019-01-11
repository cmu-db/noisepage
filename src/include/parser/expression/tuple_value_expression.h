#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"
#include "type/type_id.h"

namespace terrier::parser {

/**
 * Represents a logical tuple value.
 */
class TupleValueExpression : public AbstractExpression {
 public:
  // TODO(WAN): I feel like this should be renamed. Maybe parameters reordered too.
  /**
   * @param col_name column name
   * @param table_name table name
   */
  TupleValueExpression(std::string col_name, std::string table_name)
      : AbstractExpression(ExpressionType::VALUE_TUPLE, type::TypeId::INVALID, {}),
        col_name_(std::move(col_name)),
        table_name_(std::move(table_name)) {}

  /**
   * @return column name
   */
  std::string GetColumnName() { return col_name_; }

  /**
   * @return table name
   */
  std::string GetTableName() { return table_name_; }

  std::unique_ptr<AbstractExpression> Copy() const override { return std::make_unique<TupleValueExpression>(*this); }

 private:
  const std::string col_name_;
  const std::string table_name_;
};

}  // namespace terrier::parser
