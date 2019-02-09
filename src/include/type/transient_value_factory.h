#pragma once

#include <cstring>
#include "type/type_id.h"
#include "type/transient_value.h"

namespace terrier::type {
class TransientValueFactory {
 public:
  TransientValueFactory() = delete;

  static TransientValue GetBoolean(const bool value) { return {TypeId::BOOLEAN, value}; }

  static TransientValue GetTinyInt(const int8_t value) { return {TypeId::TINYINT, value}; }

  static TransientValue GetSmallInt(const int16_t value) { return {TypeId::SMALLINT, value}; }

  static TransientValue GetInteger(const int32_t value) { return {TypeId::INTEGER, value}; }

  static TransientValue GetBigInt(const int64_t value) { return {TypeId::BIGINT, value}; }

  static TransientValue GetDecimal(const double value) { return {TypeId::DECIMAL, value}; }

  static TransientValue GetTimestamp(const timestamp_t value) { return {TypeId::TIMESTAMP, value}; }

  static TransientValue GetDate(const date_t value) { return {TypeId::DATE, value}; }

  static TransientValue GetVarChar(const char *const value) {
    TERRIER_ASSERT(value != nullptr, "Cannot build VARCHAR from nullptr.");
    const auto length = static_cast<uint32_t>(std::strlen(value));
    auto *const varchar = new char[length + sizeof(uint32_t)];
    *(reinterpret_cast<uint32_t *const>(varchar)) = length;
    char *const varchar_contents = varchar + sizeof(uint32_t);
    std::memcpy(varchar_contents, value, length);
    return {TypeId::VARCHAR, varchar};
  }
};

}  // namespace terrier::type
