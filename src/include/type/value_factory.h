#pragma once

#include "type/type_id.h"
#include "type/value.h"

namespace terrier::type {
class ValueFactory {
 public:
  ValueFactory() = delete;

  static Value GetBoolean(const bool value) { return {TypeId::BOOLEAN, value}; }

  static Value GetTinyInt(const int8_t value) { return {TypeId::TINYINT, value}; }

  static Value GetSmallInt(const int16_t value) { return {TypeId::SMALLINT, value}; }

  static Value GetInteger(const int32_t value) { return {TypeId::INTEGER, value}; }

  static Value GetBigInt(const int64_t value) { return {TypeId::BIGINT, value}; }

  static Value GetDecimal(const double value) { return {TypeId::DECIMAL, value}; }

  static Value GetTimestamp(const timestamp_t value) { return {TypeId::TIMESTAMP, value}; }

  static Value GetDate(const date_t value) { return {TypeId::DATE, value}; }
};

}  // namespace terrier::type
