#pragma once

#include <cstdint>
#include <string>
#include "common/typedefs.h"
#include "type/type_id.h"
#include "type/value.h"

namespace terrier::type {

/**
 * Factory class for creating Value instances.
 */

class ValueFactory {
 public:
  // TODO(Tianyu): May want this to be a concrete instance if this is to be a separate class. If instance-based, some
  // level of reuse can be achieved between value objects (e.g. many expressions can share the value 0), and no static
  // ugliness is required
  ValueFactory() = delete;
  /**
   * Create a boolean Value
   * @param value
   * @return boolean value instance
   */
  static Value GetBooleanValue(bool value) { return {TypeId::BOOLEAN, Value::Val{.boolean_ = value}}; }

  /**
   * Create an int8_t Value
   * @param value
   * @return int8_t value instance
   */
  static Value GetTinyIntValue(int8_t value) { return {TypeId::TINYINT, Value::Val{.tinyint_ = value}}; }

  /**
   * Create an int16_t Value
   * @param value
   * @return int16_t value instance
   */

  static Value GetSmallIntValue(int16_t value) { return {TypeId::SMALLINT, Value::Val{.smallint_ = value}}; }

  /**
   * Create an int32_t Value
   * @param value
   * @return int32_t value instance
   */
  static Value GetIntegerValue(int32_t value) { return {TypeId::INTEGER, Value::Val{.integer_ = value}}; }

  /**
   * Create an int64_t Value
   * @param value
   * @return int64_t value instance
   */
  static Value GetBigIntValue(int64_t value) { return {TypeId::BIGINT, Value::Val{.bigint_ = value}}; }

  /**
   * Create a decimal Value
   * @param value
   * @return decimal value instance
   */
  static Value GetDecimalValue(double value) { return {TypeId::DECIMAL, Value::Val{.decimal_ = value}}; }

  /**
   * Create a date_t Value
   * @param value
   * @return date_t value instance
   */
  static Value GetDateValue(date_t value) { return {TypeId::DATE, Value::Val{.date_ = value}}; }

  /**
   * Create a timestamp_t Value
   * @param value
   * @return timestamp_t value instance
   */
  static Value GetTimeStampValue(timestamp_t value) { return {TypeId::TIMESTAMP, Value::Val{.timestamp_ = value}}; }
};

}  // namespace terrier::type
