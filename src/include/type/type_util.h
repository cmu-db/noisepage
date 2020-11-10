#pragma once

#include <string>

#include "common/error/exception.h"
#include "common/strong_typedef.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"

namespace noisepage::type {

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
  static uint16_t GetTypeSize(const TypeId type_id) {
    switch (type_id) {
      case TypeId::BOOLEAN:
      case TypeId::TINYINT:
        return 1;
      case TypeId::SMALLINT:
        return 2;
      case TypeId::INTEGER:
      case TypeId::DATE:
      case TypeId::PARAMETER_OFFSET:
        return 4;
      case TypeId::BIGINT:
      case TypeId::DECIMAL:
      case TypeId::TIMESTAMP:
        return 8;
      case TypeId::VARCHAR:
      case TypeId::VARBINARY:
        return storage::VARLEN_COLUMN;
      default:
        throw std::runtime_error("Unknown TypeId in noisepage::type::TypeUtil::GetTypeSize().");
    }
  }

  /**
   * Get the true size of the given type (without flipping the sign bit)
   * @param type_id the type to get the size of
   * @return size in bytes used to represent the given type
   */
  static uint16_t GetTypeTrueSize(const TypeId type_id) { return GetTypeSize(type_id) & static_cast<uint8_t>(0x7f); }

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
      case type::TypeId::PARAMETER_OFFSET:
        return "PARAMETER_OFFSET";
      default: {
        throw CONVERSION_EXCEPTION(
            ("No string conversion for TypeId value " + std::to_string(static_cast<int>(type_id))).c_str());
      }
    }
  }

  /**
   * This function should act as the inverse of TypeIdToString, used for converting the recorded type back to Types
   * @param type_string string representation of the type, expected to be one of the returns in TypeIdToString
   * @return a type::TypeId type
   * @throw Conversion_Exception if the string is not one of the expected values
   */
  static type::TypeId TypeIdFromString(const std::string &type_string) {
    if (type_string == "INVALID") {
      return type::TypeId::INVALID;
    }
    if (type_string == "BOOLEAN") {
      return type::TypeId::BOOLEAN;
    }
    if (type_string == "TINYINT") {
      return type::TypeId::TINYINT;
    }
    if (type_string == "SMALLINT") {
      return type::TypeId::SMALLINT;
    }
    if (type_string == "INTEGER") {
      return type::TypeId::INTEGER;
    }
    if (type_string == "BIGINT") {
      return type::TypeId::BIGINT;
    }
    if (type_string == "DECIMAL") {
      return type::TypeId::DECIMAL;
    }
    if (type_string == "TIMESTAMP") {
      return type::TypeId::TIMESTAMP;
    }
    if (type_string == "DATE") {
      return type::TypeId::DATE;
    }
    if (type_string == "VARCHAR") {
      return type::TypeId::VARCHAR;
    }
    if (type_string == "VARBINARY") {
      return type::TypeId::VARBINARY;
    }
    if (type_string == "PARAMETER_OFFSET") {
      return type::TypeId::PARAMETER_OFFSET;
    }
    throw CONVERSION_EXCEPTION(("No type conversion for string value " + type_string).c_str());
  }
};

}  // namespace noisepage::type
