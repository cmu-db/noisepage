#pragma once

#include "execution/sql/value.h"

namespace noisepage::execution::sql {

/**
 * Date/timestamp functions.
 */
class DateTimeFunctions {
 public:
  /** This class cannot be copied or moved. */
  DISALLOW_COPY_AND_MOVE(DateTimeFunctions);

  /**
   * Compute the century in which the input SQL timestamp @em time falls into.
   * @param[out] result Where the result is written to.
   * @param time The input timestamp.
   * @return The century the SQL date falls into.
   */
  static void Century(Integer *result, const TimestampVal &time) noexcept;

  /**
   * Compute the decade the NULL-able SQL date falls into. This is the year field divided by 10.
   * @param[out] result Where the result is written to.
   * @param time The input timestamp.
   */
  static void Decade(Integer *result, const TimestampVal &time) noexcept;

  /**
   * Compute the year component of the NULL-able SQL timestamp.
   * @param[out] result Where the result is written to.
   * @param time The input timestamp.
   */
  static void Year(Integer *result, const TimestampVal &time) noexcept;

  /**
   * Compute the quarter (1-4) the NULL-able SQL timestamp falls into.
   * @param[out] result Where the result is written to.
   * @param time The input timestamp.
   */
  static void Quarter(Integer *result, const TimestampVal &time) noexcept;

  /**
   * Compute the month component (1-12) of the NULL-able SQL timestamp. Note the returned month is
   * 1-based, NOT 0-based.
   * @param[out] result Where the result is written to.
   * @param time The input timestamp.
   */
  static void Month(Integer *result, const TimestampVal &time) noexcept;

  /**
   * Compute the date component (1-31) of the NULL-able SQL timestamp. Note that the returned day is
   * 1-based, NOT 0-based.
   * @param[out] result Where the result is written to.
   * @param time The input timestamp.
   */
  static void Day(Integer *result, const TimestampVal &time) noexcept;

  /**
   * Compute the day-of-the-week (0-Sun, 1-Mon, 2-Tue, 3-Wed, 4-Thu, 5-Fri, 6-Sat) of the NULL-able
   * SQL timestamp.
   * @param[out] result Where the result is written to.
   * @param time The input timestamp.
   */
  static void DayOfWeek(Integer *result, const TimestampVal &time) noexcept;

  /**
   * Compute the day-of-the-year (1-366) of the NULL-able SQL timestamp.
   * @param[out] result Where the result is written to.
   * @param time The input timestamp.
   */
  static void DayOfYear(Integer *result, const TimestampVal &time) noexcept;

  /**
   * Compute the hour component (0-23) of the NULL-able SQL timestamp.
   * @param[out] result Where the result is written to.
   * @param time The input timestamp.
   */
  static void Hour(Integer *result, const TimestampVal &time) noexcept;

  /**
   * Compute the minute component (0-59) of the NULL-able SQL timestamp.
   * @param[out] result Where the result is written to.
   * @param time The input timestamp.
   */
  static void Minute(Integer *result, const TimestampVal &time) noexcept;

  /**
   * Compute the second component (0-59) of the NULL-able SQL timestamp.
   * @param[out] result Where the result is written to.
   * @param time The input timestamp.
   */
  static void Second(Integer *result, const TimestampVal &time) noexcept;

  /**
   * Compute the milliseconds component of the NULL-able SQL timestamp. This is the seconds field
   * plus fractional seconds, multiplied by 1000.
   * @param[out] result Where the result is written to.
   * @param time The input timestamp.
   */
  static void Millisecond(Integer *result, const TimestampVal &time) noexcept;

  /**
   * Compute the microseconds component of the NULL-able SQL timestamp. This is the seconds field
   * plus the fractional seconds, multiplied by 1000000.
   * @param[out] result Where the result is written to.
   * @param time The input timestamp.
   */
  static void Microseconds(Integer *result, const TimestampVal &time) noexcept;
};

}  // namespace noisepage::execution::sql
