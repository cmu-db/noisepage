#pragma once

#include <memory>
#include <string>
#include <utility>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "loggers/optimizer_logger.h"
#include "parser/expression_defs.h"

namespace noisepage::parser {
class ConstantValueExpression;
}

namespace noisepage::optimizer {
/**
 * ValueCondition
 * This class stores the expression type and value used in the WHERE clause as well as the
 * column name/id it affects. The optimizer uses this information to retrieve the stats
 * of the column, and compute the selectivity based on those stats, as well as the expression
 * type and the value.
 *
 * SELECT * FROM table WHERE [id = 1] <- ValueCondition
 */
class ValueCondition {
 public:
  /**
   * Constructor
   * @param column_id - the oid of the column used in the condition (e.g. col_oid_t(1))
   * @param column_name - the name of the column used in the condition (e.g. "id")
   * @param type - the type of the expression used in the condition (e.g. COMPARE_EQUAL ('=' operator))
   * @param value - the value used in the condition (e.g. 1)
   */
  ValueCondition(catalog::col_oid_t column_id, std::string column_name, parser::ExpressionType type,
                 std::unique_ptr<parser::ConstantValueExpression> value)
      : column_id_{column_id}, column_name_{std::move(column_name)}, type_{type}, value_{std::move(value)} {}

  /**
   * @return the column id
   */
  const catalog::col_oid_t &GetColumnID() const { return column_id_; }

  /**
   * @return the column name
   */
  const std::string &GetColumnName() const { return column_name_; }

  /**
   * @return the type of the expression
   */
  const parser::ExpressionType &GetType() const { return type_; }

  /**
   * @return the pointer to the value
   */
  common::ManagedPointer<parser::ConstantValueExpression> GetPointerToValue() const {
    return common::ManagedPointer(value_);
  }

 private:
  /**
   * Column ID
   */
  catalog::col_oid_t column_id_;

  /**
   * Column name
   */
  std::string column_name_;

  /**
   * Expression used in the condition
   */
  parser::ExpressionType type_;

  /**
   * Pointer to value stated in the condition
   */
  std::unique_ptr<parser::ConstantValueExpression> value_;
};
}  // namespace noisepage::optimizer
