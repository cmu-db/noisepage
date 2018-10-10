#pragma once

#include <cstdint>
#include <string>
#include "common/typedefs.h"
#include "type/type_id.h"
#include "type/value.h"

namespace terrier::type {

class ValueFactory {
 public:
  /**
   * Create a boolean Value
   * @param value
   * @return boolean value instance
   */
  static Value GetBooleanValue(bool value) { return Value(TypeId::BOOLEAN, value); }

  /**
   * Create an int8_t Value
   * @param value
   * @return int8_t value instance
   */
  static Value GetTinyIntValue(int8_t value) { return Value(value); }

  static Value GetSmallIntValue(int16_t value) { return Value(value); }

  static Value GetIntegerValue(int32_t value) { return Value(value); }

  static Value GetBigIntValue(int64_t value) { return Value(value); }

  static Value GetDecimalValue(double value) { return Value(value); }

  static Value GetDateValue(date_t value) { return Value(value); }

  static Value GetTimeStampValue(timestamp_t value) { return Value(value); }
};

}  // namespace terrier::type
