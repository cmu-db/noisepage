#pragma once

#include "type/transient_value.h"
#include "type/type_id.h"
#include "util/time_util.h"

namespace terrier::type {

/**
 * TransientValuePeeker is the static class for generating C types from corresponding SQL types stored in
 * TransientValues.
 */
class TransientValuePeeker {
 public:
  TransientValuePeeker() = delete;

  /**
   * @param value TransientValue with TypeId BOOLEAN to generate a C type for
   * @return bool representing the value of the TransientValue
   * @warning TransientValue must be non-NULL. @see TransientValue::Null() first
   */
  static bool PeekBoolean(const TransientValue &value) {
    TERRIER_ASSERT(!value.Null(), "Doesn't make sense to peek a NULL value.");
    TERRIER_ASSERT(value.Type() == TypeId::BOOLEAN, "TypeId mismatch.");
    return value.GetAs<bool>();
  }

  /**
   * @param value TransientValue with TypeId TINYINT to generate a C type for
   * @return int8_t representing the value of the TransientValue
   * @warning TransientValue must be non-NULL. @see TransientValue::Null() first
   */
  static int8_t PeekTinyInt(const TransientValue &value) {
    TERRIER_ASSERT(!value.Null(), "Doesn't make sense to peek a NULL value.");
    TERRIER_ASSERT(value.Type() == TypeId::TINYINT, "TypeId mismatch.");
    return value.GetAs<int8_t>();
  }

  /**
   * @param value TransientValue with TypeId SMALLINT to generate a C type for
   * @return int16_t representing the value of the TransientValue
   * @warning TransientValue must be non-NULL. @see TransientValue::Null() first
   */
  static int16_t PeekSmallInt(const TransientValue &value) {
    TERRIER_ASSERT(!value.Null(), "Doesn't make sense to peek a NULL value.");
    TERRIER_ASSERT(value.Type() == TypeId::SMALLINT, "TypeId mismatch.");
    return value.GetAs<int16_t>();
  }

  /**
   * @param value TransientValue with TypeId INTEGER to generate a C type for
   * @return int32_t representing the value of the TransientValue
   * @warning TransientValue must be non-NULL. @see TransientValue::Null() first
   */
  static int32_t PeekInteger(const TransientValue &value) {
    TERRIER_ASSERT(!value.Null(), "Doesn't make sense to peek a NULL value.");
    TERRIER_ASSERT(value.Type() == TypeId::INTEGER, "TypeId mismatch.");
    return value.GetAs<int32_t>();
  }

  /**
   * @param value TransientValue with TypeId PARAMETER_OFFSET to generate a C type for
   * @return int32_t representing the value of the TransientValue
   * @warning TransientValue must be non-NULL. @see TransientValue::Null() first
   */
  static uint32_t PeekParameterOffset(const TransientValue &value) {
    TERRIER_ASSERT(!value.Null(), "Doesn't make sense to peek a NULL value.");
    TERRIER_ASSERT(value.Type() == TypeId::PARAMETER_OFFSET, "TypeId mismatch.");
    return value.GetAs<uint32_t>();
  }

  /**
   * @param value TransientValue with TypeId BIGINT to generate a C type for
   * @return int64_t representing the value of the TransientValue
   * @warning TransientValue must be non-NULL. @see TransientValue::Null() first
   */
  static int64_t PeekBigInt(const TransientValue &value) {
    TERRIER_ASSERT(!value.Null(), "Doesn't make sense to peek a NULL value.");
    TERRIER_ASSERT(value.Type() == TypeId::BIGINT, "TypeId mismatch.");
    return value.GetAs<int64_t>();
  }

  /**
   * @param value TransientValue with TypeId DECIMAL to generate a C type for
   * @return double representing the value of the TransientValue
   * @warning TransientValue must be non-NULL. @see TransientValue::Null() first
   */
  static double PeekDecimal(const TransientValue &value) {
    TERRIER_ASSERT(!value.Null(), "Doesn't make sense to peek a NULL value.");
    TERRIER_ASSERT(value.Type() == TypeId::DECIMAL, "TypeId mismatch.");
    return value.GetAs<double>();
  }

  /**
   * @param value TransientValue with TypeId TIMESTAMP to generate a C type for
   * @return timestamp_t representing the value of the TransientValue
   * @warning TransientValue must be non-NULL. @see TransientValue::Null() first
   */
  static timestamp_t PeekTimestamp(const TransientValue &value) {
    TERRIER_ASSERT(!value.Null(), "Doesn't make sense to peek a NULL value.");
    TERRIER_ASSERT(value.Type() == TypeId::TIMESTAMP, "TypeId mismatch.");
    return value.GetAs<timestamp_t>();
  }

  /**
   * @param value TransientValue with TypeId DATE to generate a C type for
   * @return date_t representing the value of the TransientValue
   * @warning TransientValue must be non-NULL. @see TransientValue::Null() first
   */
  static date_t PeekDate(const TransientValue &value) {
    TERRIER_ASSERT(!value.Null(), "Doesn't make sense to peek a NULL value.");
    TERRIER_ASSERT(value.Type() == TypeId::DATE, "TypeId mismatch.");
    return value.GetAs<date_t>();
  }

  /**
   * @param value TransientValue with TypeId VARCHAR to generate a C type for
   * @return C string representing the value of the TransientValue. Should use the underlying raw pointer with care
   * @warning TransientValue must be non-NULL. @see TransientValue::Null() first
   */
  static std::string_view PeekVarChar(const TransientValue &value) {
    TERRIER_ASSERT(!value.Null(), "Doesn't make sense to peek a NULL value.");
    TERRIER_ASSERT(value.Type() == TypeId::VARCHAR, "TypeId mismatch.");
    const auto *varchar = value.GetAs<const char *>();
    uint32_t length = *reinterpret_cast<const uint32_t *>(varchar);
    const auto *ptr = varchar + sizeof(uint32_t);
    return std::string_view(ptr, length);
  }

  /**
   * Convienient function that peaks the TransientValue's underlying value
   * @param transient_val
   * @param value_output
   * @return true if not null
   */
  static bool PeekValue(const type::TransientValue &transient_val, byte *value_output) {
    // Value output should be zero filled before calling
    if (transient_val.Null()) {
      // NullToSql(&expr) produces a NULL of expr's type.
      return false;
    }

    switch (transient_val.Type()) {
      case type::TypeId::BOOLEAN: {
        auto val = type::TransientValuePeeker::PeekBoolean(transient_val);
        memcpy(value_output, &val, type::TypeUtil::GetTypeSize(transient_val.Type()));
        break;
      }
      case type::TypeId::TINYINT: {
        auto val = type::TransientValuePeeker::PeekTinyInt(transient_val);
        memcpy(value_output, &val, type::TypeUtil::GetTypeSize(transient_val.Type()));
        break;
      }
      case type::TypeId::SMALLINT: {
        auto val = type::TransientValuePeeker::PeekSmallInt(transient_val);
        memcpy(value_output, &val, type::TypeUtil::GetTypeSize(transient_val.Type()));
        break;
      }
      case type::TypeId::INTEGER: {
        auto val = type::TransientValuePeeker::PeekInteger(transient_val);
        memcpy(value_output, &val, type::TypeUtil::GetTypeSize(transient_val.Type()));
        break;
      }
      case type::TypeId::BIGINT: {
        auto val = type::TransientValuePeeker::PeekBigInt(transient_val);
        memcpy(value_output, &val, type::TypeUtil::GetTypeSize(transient_val.Type()));
        break;
      }
      case type::TypeId::DATE: {
        // TODO(Schema-Change): find a way to handle it without codegen
        break;
      }
      case type::TypeId::TIMESTAMP: {
        auto val = type::TransientValuePeeker::PeekTimestamp(transient_val);
        auto julian_usec = util::TimeConvertor::ExtractJulianMicroseconds(val);
        memcpy(value_output, &julian_usec, type::TypeUtil::GetTypeSize(transient_val.Type()));
        break;
      }
      case type::TypeId::DECIMAL: {
        auto val = type::TransientValuePeeker::PeekDecimal(transient_val);
        memcpy(value_output, &val, type::TypeUtil::GetTypeSize(transient_val.Type()));
        break;
      }
      case type::TypeId::VARCHAR:
      case type::TypeId::VARBINARY: {
        auto val = terrier::type::TransientValuePeeker::PeekVarChar(transient_val);
        memcpy(value_output, &val, val.size());
        break;
      }
      default:
        // TODO(Amadou): Add support for these types.
        TERRIER_ASSERT(false, "Should not peek on given type!");
    }
    return true;
  }
};

}  // namespace terrier::type
