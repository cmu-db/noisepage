#pragma once

#include "common/strong_typedef.h"
#include "date/date.h"

namespace terrier::type {

/**
 * Julian date.
 * Precision: days
 * Range: 0 (Nov 24, -4713) to 2^31-1 (Jun 03, 5874898).
 */
STRONG_TYPEDEF(date_t, uint32_t);

/**
 * Julian timestamp.
 * Precision: microseconds, 14 digits
 * Range: 4713 BC to 294276 AD
 */
STRONG_TYPEDEF(timestamp_t, uint64_t);

enum class TypeId : uint8_t {
  INVALID = 0,
  BOOLEAN,
  TINYINT,
  SMALLINT,
  INTEGER,
  BIGINT,
  DECIMAL,
  TIMESTAMP,
  DATE,
  VARCHAR,
  VARBINARY,
  PARAMETER_OFFSET
};
}  // namespace terrier::type
