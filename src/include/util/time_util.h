#pragma once

#include <chrono>  // NOLINT
#include <string>
#include <utility>
#include "type/type_id.h"

namespace date {
class year_month_day;
}

namespace terrier::util {

/**
 * TimeConvertor handles time conversions between strings and the shared representation used by storage and execution.
 *
 * Internally, we store DATE and TIMESTAMP just like PostgreSQL does.
 * - DATE, 4 bytes, Julian days
 * - TIMESTAMP, 8 bytes, Julian microseconds
 */
class TimeConvertor {
  // Note that C++20 greatly simplifies formatting of date and time. Punting on pretty formatting.

 public:
  /** Convert @p ymd into the internal date representation. */
  static type::date_t DateFromYMD(date::year_month_day ymd);

  /** Convert @p date into a year_month_day object. */
  static date::year_month_day YMDFromDate(type::date_t date);

  /** Instantiate a timestamp with the given parameters. */
  static type::timestamp_t TimestampFromHMSu(int32_t year, uint32_t month, uint32_t day, uint8_t hour, uint8_t minute,
                                             uint8_t sec, uint64_t usec);

  /** Convert @p date into a year_month_day object. */
  static type::timestamp_t TimestampFromDate(type::date_t date) {
    return type::timestamp_t{!date * MICROSECONDS_PER_DAY};
  }

  /** Convert @p timestamp into a date. */
  static type::date_t DateFromTimestamp(type::timestamp_t timestamp) {
    return type::date_t{static_cast<uint32_t>(!timestamp / MICROSECONDS_PER_DAY)};
  }

  /** Extract the number of microseconds with respect to Julian time from @p timestamp. */
  static uint64_t ExtractJulianMicroseconds(type::timestamp_t timestamp) { return !timestamp; }

  /**
   * Attempt to parse @p str into the internal date representation.
   * @param str The string to be parsed.
   * @return (True, parse result) if parse succeeded; (False, undefined) otherwise
   */
  static std::pair<bool, type::date_t> ParseDate(const std::string &str);

  /**
   * Attempt to parse @p str into the internal timestamp representation.
   * @param str The string to be parsed.
   * @return (True, parse result) if parse succeeded; (False, undefined) otherwise
   */
  static std::pair<bool, type::timestamp_t> ParseTimestamp(const std::string &str);

  /** @return The @p date formatted as a string. */
  static std::string FormatDate(type::date_t date);

  /** @return The @p timestamp formatted as a string. */
  static std::string FormatTimestamp(type::timestamp_t timestamp);

  /** PostgreSQL function for serializing dates to 32-bit Julian days. */
  static uint32_t PostgresDate2J(int32_t year, uint32_t month, uint32_t day) {
    /*
     * PostgreSQL backend/utils/adt/datetime.c date2j()
     * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
     * Portions Copyright (c) 1994, Regents of the University of California
     */
    // Yoinked from Postgres. Overflow-safe serialization of a date to Julian uint32_t.

    if (month > 2) {
      month += 1;
      year += 4800;
    } else {
      month += 13;
      year += 4799;
    }

    uint32_t century = year / 100;
    uint32_t julian = year * 365 - 32167;
    julian += year / 4 - century + century / 4;
    julian += 7834 * month / 256 + day;
    return julian;
  }

  /** PostgreSQL functino for deserializing 32-bit Julian days to a date. */
  static date::year_month_day PostgresJ2Date(uint32_t julian_days);

 private:
  static constexpr uint64_t MICROSECONDS_PER_SECOND = 1000 * 1000;
  static constexpr uint64_t MICROSECONDS_PER_MINUTE = 60UL * MICROSECONDS_PER_SECOND;
  static constexpr uint64_t MICROSECONDS_PER_HOUR = 60UL * MICROSECONDS_PER_MINUTE;
  static constexpr uint64_t MICROSECONDS_PER_DAY = 24UL * MICROSECONDS_PER_HOUR;

  /**
   * Parse the provided string @p str according to the format string @p fmt, storing the result in @p tp.
   * @param fmt Format string. See howardhinnant.github.io/date/date.html for format specifiers.
   * @param str String to be parsed.
   * @param[out] tp Result from parsing the string.
   * @return True if the parse was successful, false otherwise.
   */

  /**
   * Note that time_output is equivalent to date::sys_time<std::chrono::microseconds>, but we use the former
   * to avoid including date/date.h in this header file.
   */
  using time_output = std::chrono::time_point<std::chrono::system_clock, std::chrono::microseconds>;
  static bool Parse(const std::string &fmt, const std::string &str, time_output *tp);
};

}  // namespace terrier::util
