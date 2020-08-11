#pragma once

#include "common/strong_typedef.h"

namespace terrier::type {

/**
 * Julian date.
 * Precision: days
 * Range: 0 (Nov 24, -4713) to 2^31-1 (Jun 03, 5874898).
 */
STRONG_TYPEDEF_HEADER(date_t, uint32_t);

/**
 * Julian timestamp.
 * Precision: microseconds, 14 digits
 * Range: 4713 BC to 294276 AD
 */
STRONG_TYPEDEF_HEADER(timestamp_t, uint64_t);

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

enum class DATE_PART : uint32_t {
  INVALID = 0,
  CENTURY = 1,
  DAY = 2,
  DAYS = 2,
  DECADE = 3,
  DECADES = 3,
  DOW = 4,
  DOY = 5,
  HOUR = 7,
  HOURS = 7,
  MICROSECOND = 10,
  MICROSECONDS = 10,
  MILLENNIUM = 11,
  MILLISECOND = 12,
  MILLISECONDS = 12,
  MINUTE = 13,
  MINUTES = 13,
  MONTH = 14,
  MONTHS = 14,
  QUARTER = 15,
  QUARTERS = 15,
  SECOND = 16,
  SECONDS = 16,
  WEEK = 20,
  WEEKS = 20,
  YEAR = 21,
  YEARS = 21,
};

}  // namespace terrier::type
