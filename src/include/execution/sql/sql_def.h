#pragma once

#include <cstring>
#include <string>

namespace noisepage::execution::sql {

enum class DatePartType : uint32_t {
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

}  // namespace noisepage::execution::sql
