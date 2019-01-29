#pragma once

#include "type/type_id.h"
#include "type/value.h"

namespace terrier::type {
class ValuePeeker {
 public:
  ValuePeeker() = delete;

  static inline bool PeekBoolean(const Value &value) {
    TERRIER_ASSERT(value.GetType() == TypeId::BOOLEAN, "TypeId mismatch.");
    return value.GetAs<bool>();
  }

  static inline int8_t PeekTinyInt(const Value &value) {
    TERRIER_ASSERT(value.GetType() == TypeId::TINYINT, "TypeId mismatch.");
    return value.GetAs<int8_t>();
  }

  static inline int16_t PeekSmallInt(const Value &value) {
    TERRIER_ASSERT(value.GetType() == TypeId::SMALLINT, "TypeId mismatch.");
    return value.GetAs<int16_t>();
  }

  static inline int32_t PeekInteger(const Value &value) {
    TERRIER_ASSERT(value.GetType() == TypeId::INTEGER, "TypeId mismatch.");
    return value.GetAs<int32_t>();
  }

  static inline int32_t PeekParameterOffset(const Value &value) {
    TERRIER_ASSERT(value.GetType() == TypeId::PARAMETER_OFFSET, "TypeId mismatch.");
    return value.GetAs<int32_t>();
  }

  static inline date_t PeekDate(const Value &value) {
    TERRIER_ASSERT(value.GetType() == TypeId::DATE, "TypeId mismatch.");
    return value.GetAs<date_t>();
  }

  static inline int64_t PeekBigInt(const Value &value) {
    TERRIER_ASSERT(value.GetType() == TypeId::BIGINT, "TypeId mismatch.");
    return value.GetAs<int64_t>();
  }

  static inline double PeekDecimal(const Value &value) {
    TERRIER_ASSERT(value.GetType() == TypeId::DECIMAL, "TypeId mismatch.");
    return value.GetAs<double>();
  }

  static inline timestamp_t PeekTimestamp(const Value &value) {
    TERRIER_ASSERT(value.GetType() == TypeId::TIMESTAMP, "TypeId mismatch.");
    return value.GetAs<timestamp_t>();
  }
};

}  // namespace terrier::type
