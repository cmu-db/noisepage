#pragma once

#include "type/type_id.h"
#include "type/value.h"

namespace terrier::type {
class ValueWrapper {
 public:
  ValueWrapper() = delete;

  static Value WrapBoolean(byte *const data) { return {TypeId::BOOLEAN, data}; }

  static Value WrapTinyInt(byte *const data) { return {TypeId::TINYINT, data}; }

  static Value WrapSmallInt(byte *const data) { return {TypeId::SMALLINT, data}; }

  static Value WrapInteger(byte *const data) { return {TypeId::INTEGER, data}; }

  static Value WrapBigInt(byte *const data) { return {TypeId::BIGINT, data}; }

  static Value WrapDecimal(byte *const data) { return {TypeId::DECIMAL, data}; }

  static Value WrapTimestamp(byte *const data) { return {TypeId::TIMESTAMP, data}; }

  static Value WrapDate(byte *const data) { return {TypeId::DATE, data}; }
};

}  // namespace terrier::type
