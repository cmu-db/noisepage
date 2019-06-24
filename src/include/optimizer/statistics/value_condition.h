#pragma once

#include <memory>
#include <string>
#include <utility>

#include "catalog/catalog_defs.h"
#include "loggers/optimizer_logger.h"
#include "parser/expression_defs.h"
#include "type/transient_value.h"

namespace terrier::optimizer {
/**
 * ValueCondition
 * This class stores the expression type and value used in the WHERE clause as well as the
 * column name/id it affects.
 *
 * SELECT * FROM table WHERE [id = 1] <- ValueCondition
 */
class ValueCondition {
 public:
  /**
   * Constructor
   * @param column_id
   * @param column_name
   * @param type
   * @param value
   */
  ValueCondition(catalog::col_oid_t column_id, std::string column_name, parser::ExpressionType type,
                 std::shared_ptr<type::TransientValue> value)
      : column_id_{column_id}, column_name_{std::move(column_name)}, type_{type}, value_{std::move(value)} {}

  /**
   * Only with id. Default column_name to empty string.
   * @param column_id
   * @param type
   * @param value
   */
  ValueCondition(catalog::col_oid_t column_id, parser::ExpressionType type, std::shared_ptr<type::TransientValue> value)
      : ValueCondition(column_id, "", type, std::move(value)) {}

  /**
   * Only with column name. Default column_id to be 0.
   * @param column_name
   * @param type
   * @param value
   */
  ValueCondition(std::string column_name, parser::ExpressionType type, std::shared_ptr<type::TransientValue> value)
      : ValueCondition(catalog::col_oid_t(0), std::move(column_name), type, std::move(value)) {}

  /**
   * @return the column id
   */
  const catalog::col_oid_t &GetColumnID() const { return column_id_; }

  /**
   * @return the column name
   */
  const std::string &GetColumnName() const { return column_name_; }

  /**
   * @return the type
   */
  const parser::ExpressionType &GetType() const { return type_; }

  /**
   * @return the pointer to the value
   */
  const std::shared_ptr<type::TransientValue> &GetPointerToValue() const { return value_; }

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
   * Expression used in WHERE clause
   */
  parser::ExpressionType type_;

  /**
   * Pointer to value
   */
  std::shared_ptr<type::TransientValue> value_;
};
}  // namespace terrier::optimizer
