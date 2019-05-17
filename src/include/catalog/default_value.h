#pragma once

#include <string>

#include "type/type_id.h"

namespace terrier::catalog {
/**
 * Wraps the concept of a default value expression into an object that can be
 * passed between the catalog and the consumers of these expressions.
 */
class DefaultValue {
 public:
  /**
   * Constructs a default value.  We perform a deep copy for now to make lifecycle
   * reasoning easier.  This may need to be updated if it becomes a bottleneck.
   * @param type_id of the default value
   * @param serialized_expression for calculating default value at runtime
   */
  explicit DefaultValue(type::TypeId type_id, std::string serialized_expression)
      : type_(type_id), serialized_expression_(std::move(serialized_expression)) {}

  /**
   * @return the type of the default value
   */
  const type::TypeId &GetType() { return type_; }

  /**
   * @return the expression that calculates the default value
   */
  const std::string &GetExpression() { return serialized_expression_; }

 private:
  type::TypeId type_;
  std::string serialized_expression_;
};
}  // namespace terrier::catalog
