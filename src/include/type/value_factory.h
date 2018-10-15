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

  /**
   * Create an int16_t Value
   * @param value
   * @return int16_t value instance
   */

  static Value GetSmallIntValue(int16_t value) { return Value(value); }

  /**
   * Create an int32_t Value
   * @param value
   * @return int32_t value instance
   */
  static Value GetIntegerValue(int32_t value) { return Value(value); }

  /**
   * Create an int64_t Value
   * @param value
   * @return int64_t value instance
   */
  static Value GetBigIntValue(int64_t value) { return Value(value); }

  /**
   * Create a decimal Value
   * @param value
   * @return decimal value instance
   */
  static Value GetDecimalValue(double value) { return Value(value); }

  /**
   * Create a date_t Value
   * @param value
   * @return date_t value instance
   */
  static Value GetDateValue(date_t value) { return Value(value); }

  /**
   * Create a timestamp_t Value
   * @param value
   * @return timestamp_t value instance
   */
  static Value GetTimeStampValue(timestamp_t value) { return Value(value); }
};

}  // namespace terrier::type
