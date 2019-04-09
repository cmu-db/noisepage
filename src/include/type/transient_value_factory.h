#pragma once

#include <cstring>
#include "common/exception.h"
#include "type/transient_value.h"
#include "type/transient_value_peeker.h"
#include "type/type_id.h"

namespace terrier::type {

/**
 * TransientValueFactory is the static class for generating  SQL types stored in TransientValues from corresponding C
 * types.
 */
class TransientValueFactory {
 public:
  TransientValueFactory() = delete;

  /**
   * @param type type id of the NULL value.
   * @return TransientValue representing a NULL value
   */
  static TransientValue GetNull(TypeId type) { return TransientValue(type); }
  /**
   * @param value C type to generate a TransientValue with TypeId BOOLEAN. If you want a NULL with this TypeId, just
   * instantiate one with this factory method and then change its NULL value. @see TransientValue::SetNull().
   * @return TransientValue representing the value of the bool
   */
  static TransientValue GetBoolean(const bool value) { return {TypeId::BOOLEAN, value}; }

  /**
   * @param value C type to generate a TransientValue with TypeId TINYINT. If you want a NULL with this TypeId, just
   * instantiate one with this factory method and then change its NULL value. @see TransientValue::SetNull().
   * @return TransientValue representing the value of the int8_t
   */
  static TransientValue GetTinyInt(const int8_t value) { return {TypeId::TINYINT, value}; }

  /**
   * @param value C type to generate a TransientValue with TypeId SMALLINT. If you want a NULL with this TypeId, just
   * instantiate one with this factory method and then change its NULL value. @see TransientValue::SetNull().
   * @return TransientValue representing the value of the int16_t
   */
  static TransientValue GetSmallInt(const int16_t value) { return {TypeId::SMALLINT, value}; }

  /**
   * @param value C type to generate a TransientValue with TypeId INTEGER. If you want a NULL with this TypeId, just
   * instantiate one with this factory method and then change its NULL value. @see TransientValue::SetNull().
   * @return TransientValue representing the value of the int32_t
   */
  static TransientValue GetInteger(const int32_t value) { return {TypeId::INTEGER, value}; }

  /**
   * @param value C type to generate a TransientValue with TypeId BIGINT. If you want a NULL with this TypeId, just
   * instantiate one with this factory method and then change its NULL value. @see TransientValue::SetNull().
   * @return TransientValue representing the value of the int64_t
   */
  static TransientValue GetBigInt(const int64_t value) { return {TypeId::BIGINT, value}; }

  /**
   * @param value C type to generate a TransientValue with TypeId DECIMAL. If you want a NULL with this TypeId, just
   * instantiate one with this factory method and then change its NULL value. @see TransientValue::SetNull().
   * @return TransientValue representing the value of the double
   */
  static TransientValue GetDecimal(const double value) { return {TypeId::DECIMAL, value}; }

  /**
   * @param value C type to generate a TransientValue with TypeId TIMESTAMP. If you want a NULL with this TypeId, just
   * instantiate one with this factory method and then change its NULL value. @see TransientValue::SetNull().
   * @return TransientValue representing the value of the timestamp_t
   */
  static TransientValue GetTimestamp(const timestamp_t value) { return {TypeId::TIMESTAMP, value}; }

  /**
   * @param value C type to generate a TransientValue with TypeId DATE. If you want a NULL with this TypeId, just
   * instantiate one with this factory method and then change its NULL value. @see TransientValue::SetNull().
   * @return TransientValue representing the value of the date_t
   */
  static TransientValue GetDate(const date_t value) { return {TypeId::DATE, value}; }

  /**
   * @param value C type to generate a TransientValue with TypeId VARCHAR. If you want a NULL with this TypeId, just
   * instantiate one with this factory method and then change its NULL value. @see TransientValue::SetNull().
   * @return TransientValue representing the value of the null-terminated C string. The TransientValue has its own
   * internal buffer to store the VARCHAR, so the C string argument can safely be deallocated by the caller after
   * instantiating the TransientValue.
   */
  static TransientValue GetVarChar(const std::string_view value) {
    TERRIER_ASSERT(value.data() != nullptr, "Cannot build VARCHAR from nullptr.");
    size_t length = value.length();
    auto *varchar = new char[length + sizeof(uint32_t)];
    // assert length fits in uint32_t
    *(reinterpret_cast<uint32_t *const>(varchar)) = static_cast<uint32_t>(length);
    auto *varchar_contents = varchar + sizeof(uint32_t);
    std::memcpy(varchar_contents, value.data(), length);
    return {TypeId::VARCHAR, varchar};
  }

  /**
   * Get a full copy of a transient value.
   */
  static type::TransientValue GetCopy(const type::TransientValue &value) {
    // NOLINTNEXTLINE
    switch (value.Type()) {
      case type::TypeId::BOOLEAN:
        return type::TransientValueFactory::GetBoolean(type::TransientValuePeeker::PeekBoolean(value));
        break;
      case type::TypeId::TINYINT:
        return type::TransientValueFactory::GetTinyInt(type::TransientValuePeeker::PeekTinyInt(value));
        break;
      case type::TypeId::SMALLINT:
        return type::TransientValueFactory::GetSmallInt(type::TransientValuePeeker::PeekSmallInt(value));
        break;
      case type::TypeId::INTEGER:
        return type::TransientValueFactory::GetInteger(type::TransientValuePeeker::PeekInteger(value));
        break;
      case type::TypeId::BIGINT:
        return type::TransientValueFactory::GetBigInt(type::TransientValuePeeker::PeekBigInt(value));
        break;
      case type::TypeId::DECIMAL:
        return type::TransientValueFactory::GetDecimal(type::TransientValuePeeker::PeekDecimal(value));
        break;
      case type::TypeId::TIMESTAMP:
        return type::TransientValueFactory::GetTimestamp(type::TransientValuePeeker::PeekTimestamp(value));
        break;
      case type::TypeId::DATE:
        return type::TransientValueFactory::GetDate(type::TransientValuePeeker::PeekDate(value));
        break;
      case type::TypeId::VARCHAR:
        return type::TransientValueFactory::GetVarChar(type::TransientValuePeeker::PeekVarChar(value));
        break;
      default:
        throw NOT_IMPLEMENTED_EXCEPTION("invalid TransientValue copy.");
        break;
    }
  }
};

}  // namespace terrier::type
