#pragma once

#include <string>
#include "common/exception.h"
#include "common/strong_typedef.h"
#include "storage/block_layout.h"
#include "type/type_id.h"

namespace terrier::type {

/**
 * Static utility class for common functions in type
 */
class TypeUtil {
 public:
  /**
   * Get the size of the given type
   * @param type_id the type to get the size of
   * @return size in bytes used to represent the given type
   * @warning variable length types return 16 with sign bit flipped
   * @warning the implementation of ProjectedColumns assumes that all attribute sizes are an even power of two in its
   * implementation (see NUM_ATTR_BOUNDARIES in storage_defs.h).  The concept of boundary checks can be implemented
   * without this constraint, but it would likely incur a speed impact on creation of ProjectedColumns and RowViews.
   * @throw std::runtime_error if type is unknown
   */
  static uint8_t GetTypeSize(const TypeId type_id) {
    switch (type_id) {
      case TypeId::BOOLEAN:
      case TypeId::TINYINT:
        return 1;
      case TypeId::SMALLINT:
        return 2;
      case TypeId::INTEGER:
      case TypeId::DATE:
        return 4;
      case TypeId::BIGINT:
      case TypeId::DECIMAL:
      case TypeId::TIMESTAMP:
        return 8;
      case TypeId::VARCHAR:
      case TypeId::VARBINARY:
        return VARLEN_COLUMN;
      default:
        throw std::runtime_error("Unknown TypeId in terrier::type::TypeUtil::GetTypeSize().");
    }
  }

  /**
   * This function stringify the Types for getting expression name for the constant value expression
   * @param type_id the type to get the string version of
   * @return string representation of the type
   * @throw Conversion_Exception if the type is unknown
   */
  static std::string TypeIdToString(type::TypeId type_id) {
    switch (type_id) {
      case type::TypeId::INVALID:
        return "INVALID";
      case type::TypeId::BOOLEAN:
        return "BOOLEAN";
      case type::TypeId::TINYINT:
        return "TINYINT";
      case type::TypeId::SMALLINT:
        return "SMALLINT";
      case type::TypeId::INTEGER:
        return "INTEGER";
      case type::TypeId::BIGINT:
        return "BIGINT";
      case type::TypeId::DECIMAL:
        return "DECIMAL";
      case type::TypeId::TIMESTAMP:
        return "TIMESTAMP";
      case type::TypeId::DATE:
        return "DATE";
      case type::TypeId::VARCHAR:
        return "VARCHAR";
      case type::TypeId::VARBINARY:
        return "VARBINARY";
      default: {
        throw CONVERSION_EXCEPTION(
            ("No string conversion for TypeId value " + std::to_string(static_cast<int>(type_id))).c_str());
      }
    }
  }
};

}  // namespace terrier::type
