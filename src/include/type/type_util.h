#pragma once

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
   * @warning variable length types return 0. Handle this appropriately when calling this function and rememeber to use
   * the size of a pointer to point to the varlen entry
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
};

}  // namespace terrier::type
