#pragma once

#include "type/type_id.h"
#include "type/value.h"

namespace terrier::type {
class ValuePeeker {
 public:
  ValuePeeker() = delete;

  static inline bool PeekBoolean(const TransientValue &value) {
    TERRIER_ASSERT(!value.Null(), "Doesn't make sense to peek a NULL value.");
    TERRIER_ASSERT(value.Type() == TypeId::BOOLEAN, "TypeId mismatch.");
    return value.GetAs<bool>();
  }

  static inline int8_t PeekTinyInt(const TransientValue &value) {
    TERRIER_ASSERT(!value.Null(), "Doesn't make sense to peek a NULL value.");
    TERRIER_ASSERT(value.Type() == TypeId::TINYINT, "TypeId mismatch.");
    return value.GetAs<int8_t>();
  }

  static inline int16_t PeekSmallInt(const TransientValue &value) {
    TERRIER_ASSERT(!value.Null(), "Doesn't make sense to peek a NULL value.");
    TERRIER_ASSERT(value.Type() == TypeId::SMALLINT, "TypeId mismatch.");
    return value.GetAs<int16_t>();
  }

  static inline int32_t PeekInteger(const TransientValue &value) {
    TERRIER_ASSERT(!value.Null(), "Doesn't make sense to peek a NULL value.");
    TERRIER_ASSERT(value.Type() == TypeId::INTEGER, "TypeId mismatch.");
    return value.GetAs<int32_t>();
  }

  static inline int64_t PeekBigInt(const TransientValue &value) {
    TERRIER_ASSERT(!value.Null(), "Doesn't make sense to peek a NULL value.");
    TERRIER_ASSERT(value.Type() == TypeId::BIGINT, "TypeId mismatch.");
    return value.GetAs<int64_t>();
  }

  static inline double PeekDecimal(const TransientValue &value) {
    TERRIER_ASSERT(!value.Null(), "Doesn't make sense to peek a NULL value.");
    TERRIER_ASSERT(value.Type() == TypeId::DECIMAL, "TypeId mismatch.");
    return value.GetAs<double>();
  }

  static inline timestamp_t PeekTimestamp(const TransientValue &value) {
    TERRIER_ASSERT(!value.Null(), "Doesn't make sense to peek a NULL value.");
    TERRIER_ASSERT(value.Type() == TypeId::TIMESTAMP, "TypeId mismatch.");
    return value.GetAs<timestamp_t>();
  }

  static inline date_t PeekDate(const TransientValue &value) {
    TERRIER_ASSERT(!value.Null(), "Doesn't make sense to peek a NULL value.");
    TERRIER_ASSERT(value.Type() == TypeId::DATE, "TypeId mismatch.");
    return value.GetAs<date_t>();
  }

  static inline const char *const PeekVarChar(const TransientValue &value) {
    TERRIER_ASSERT(!value.Null(), "Doesn't make sense to peek a NULL value.");
    TERRIER_ASSERT(value.Type() == TypeId::VARCHAR, "TypeId mismatch.");
    const char *const varchar = value.GetAs<const char *const>();
    const uint32_t length = *reinterpret_cast<const uint32_t *const>(varchar);
    char *const cstring = new char[length + 1];
    std::memcpy(cstring, varchar + sizeof(uint32_t), length);
    cstring[length] = '\0';
    return cstring;
  }
};

}  // namespace terrier::type
