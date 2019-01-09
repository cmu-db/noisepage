#pragma once

#include "common/macros.h"

namespace terrier::optimizer {

/**
 * Property types.
 */
enum class PropertyType : uint8_t {
  INVALID,
  COLUMNS,
  DISTINCT,
  SORT,
  LIMIT,
};
std::string PropertyTypeToString(PropertyType type);
PropertyType StringToPropertyType(const std::string &str);
std::ostream &operator<<(std::ostream &os, const PropertyType &type);

}  // namespace terrier::optimizer