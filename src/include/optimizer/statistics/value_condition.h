#pragma once

#include <string>

#include "catalog/catalog_defs.h"
#include "loggers/optimizer_logger.h"
#include "parser/expression_defs.h"
#include "type/transient_value.h"

namespace terrier {
namespace optimizer {

/**
 * ValueCondition
 * SELECT * FROM table WHERE [id = 1] <- ValueCondition
 */

class ValueCondition {
 public:
  catalog::col_oid_t column_id_;
  std::string column_name_;
  parser::ExpressionType type_;
  type::TransientValue value_;

  /**
   * Constructor
   * @param column_id
   * @param column_name
   * @param type
   * @param value
   */
  ValueCondition(catalog::col_oid_t column_id, std::string column_name, parser::ExpressionType type,
                 const type::TransientValue &value)
      : column_id_{column_id}, column_name_{column_name}, type_{type}, value_{value} {}

  /** Only with id. Default column_name to empty string.
   *
   * @param column_id
   * @param type
   * @param value
   */
  ValueCondition(catalog::col_oid_t column_id, parser::ExpressionType type, const type::TransientValue &value)
      : ValueCondition(column_id_, "", type_, value_) {}

  /**
   * Only with column name. Default column_id to be 0.
   *
   * @param column_name
   * @param type
   * @param TransientValue
   */

  ValueCondition(std::string column_name, parser::ExpressionType type, const type::Value &TransientValue)
      : ValueCondition(0, column_name_, type_, value_) {}
};

}  // namespace optimizer
}  // namespace terrier
