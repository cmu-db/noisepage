#pragma once

#include <limits>

#include "loggers/optimizer_logger.h"
#include "type/transient_value.h"
#include "type/transient_value_peeker.h"

namespace terrier::optimizer {

/**
 * TransientValue utility functions
 */
class TransientValueUtil {
 public:
  /**
   * Converts numeric TransientValue type to primitive value
   * Return NaN if value is not numeric
   * @param value TransientValue to convert
   * @returns primitive value or NaN
   */
  static double TransientValueToNumericValue(const type::TransientValue &value) {
    double raw_value = std::numeric_limits<double>::quiet_NaN();
    if (value.Null()) {
      OPTIMIZER_LOG_TRACE("Fail to convert terrier NULL value to numeric value.");
      return raw_value;
    }

    switch (value.Type()) {
      case type::TypeId::TINYINT:
        raw_value = static_cast<double>(type::TransientValuePeeker::PeekTinyInt(value));
        break;
      case type::TypeId::SMALLINT:
        raw_value = static_cast<double>(type::TransientValuePeeker::PeekSmallInt(value));
        break;
      case type::TypeId::INTEGER:
        raw_value = static_cast<double>(type::TransientValuePeeker::PeekInteger(value));
        break;
      case type::TypeId::BIGINT:
        raw_value = static_cast<double>(type::TransientValuePeeker::PeekBigInt(value));
        break;
      case type::TypeId::TIMESTAMP:
        raw_value = static_cast<double>(!type::TransientValuePeeker::PeekTimestamp(value));
        break;
      case type::TypeId::PARAMETER_OFFSET:
        raw_value = static_cast<double>(type::TransientValuePeeker::PeekParameterOffset(value));
        break;
      case type::TypeId::DECIMAL:
        raw_value = type::TransientValuePeeker::PeekDecimal(value);
        break;
      default:
        OPTIMIZER_LOG_TRACE("Fail to convert non-numeric Peloton value to numeric value");
    }

    return raw_value;
  }
};

}  // namespace terrier::optimizer
