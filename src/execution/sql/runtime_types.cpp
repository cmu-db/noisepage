#include "execution/sql/runtime_types.h"

#include <string>

#include "common/error/exception.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::sql {

namespace {

constexpr int64_t K_MONTHS_PER_YEAR = 12;
constexpr int32_t K_DAYS_PER_MONTH[2][12] = {{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
                                             {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}};
constexpr int64_t K_HOURS_PER_DAY = 24;
constexpr int64_t K_MINUTES_PER_HOUR = 60;
constexpr int64_t K_SECONDS_PER_MINUTE = 60;
constexpr int64_t K_MILLISECONDS_PER_SECOND = 1000;
constexpr int64_t K_MICROSECONDS_PER_MILLISECOND = 1000;

constexpr std::string_view K_TIMESTAMP_SUFFIX = "::timestamp";

// Like Postgres, TPL stores dates as Julian Date Numbers. Julian dates are
// commonly used in astronomical applications and in software since it's
// numerically accurate and computationally simple. BuildJulianDate() and
// SplitJulianDate() correctly convert between Julian day and Gregorian
// calendar for all non-negative Julian days (i.e., from 4714-11-24 BC to
// 5874898-06-03 AD). Though the JDN number is unsigned, it's physically
// stored as a signed 32-bit integer, and comparison functions also use
// signed integer logic.
//
// Many of the conversion functions are adapted from implementations in
// Postgres. Specifically, we use the algorithms date2j() and j2date()
// in src/backend/utils/adt/datetime.c.

constexpr int64_t K_JULIAN_MIN_YEAR = -4713;
constexpr int64_t K_JULIAN_MIN_MONTH = 11;
constexpr int64_t K_JULIAN_MAX_YEAR = 5874898;
constexpr int64_t K_JULIAN_MAX_MONTH = 6;

// Is the provided year a leap year?
bool IsLeapYear(int32_t year) { return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0); }

// Does the provided date fall into the Julian date range?
bool IsValidJulianDate(int32_t y, int32_t m, int32_t d) {
  return (y > K_JULIAN_MIN_YEAR || (y == K_JULIAN_MIN_YEAR && m >= K_JULIAN_MIN_MONTH)) &&
         (y < K_JULIAN_MAX_YEAR || (y == K_JULIAN_MAX_YEAR && m < K_JULIAN_MAX_MONTH));
}

// Is the provided date a valid calendar date?
bool IsValidCalendarDate(int32_t year, int32_t month, int32_t day) {
  // There isn't a year 0. We represent 1 BC as year zero, 2 BC as -1, etc.
  if (year == 0) return false;

  // Month.
  if (month < 1 || month > K_MONTHS_PER_YEAR) return false;

  // Day.
  if (day < 1 || day > K_DAYS_PER_MONTH[IsLeapYear(year)][month - 1]) return false;

  // Looks good.
  return true;
}

// Based on date2j().
uint32_t BuildJulianDate(uint32_t year, uint32_t month, uint32_t day) {
  if (month > 2) {
    month += 1;
    year += 4800;
  } else {
    month += 13;
    year += 4799;
  }

  int32_t century = year / 100;
  int32_t julian = year * 365 - 32167;
  julian += year / 4 - century + century / 4;
  julian += 7834 * month / 256 + day;

  return julian;
}

// Based on j2date().
void SplitJulianDate(int32_t jd, int32_t *year, int32_t *month, int32_t *day) {
  uint32_t julian = jd;
  julian += 32044;
  uint32_t quad = julian / 146097;
  uint32_t extra = (julian - quad * 146097) * 4 + 3;
  julian += 60 + quad * 3 + extra / 146097;
  quad = julian / 1461;
  julian -= quad * 1461;
  int32_t y = julian * 4 / 1461;
  julian = ((y != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366)) + 123;
  y += quad * 4;
  *year = y - 4800;
  quad = julian * 2141 / 65536;
  *day = julian - 7834 * quad / 256;
  *month = (quad + 10) % K_MONTHS_PER_YEAR + 1;
}

// Split a Julian time (i.e., Julian date in microseconds) into a time and date
// component.
void StripTime(int64_t jd, int64_t *date, int64_t *time) {
  *date = jd / K_MICRO_SECONDS_PER_DAY;
  *time = jd - (*date * K_MICRO_SECONDS_PER_DAY);
}

// Given hour, minute, second, millisecond, and microsecond components, build a time in microseconds.
int64_t BuildTime(int32_t hour, int32_t min, int32_t sec, int32_t milli = 0, int32_t micro = 0) {
  return (((hour * K_MINUTES_PER_HOUR + min) * K_SECONDS_PER_MINUTE) * K_MICRO_SECONDS_PER_SECOND) +
         sec * K_MICRO_SECONDS_PER_SECOND + milli * K_MILLISECONDS_PER_SECOND + micro;
}

// Given a time in microseconds, split it into hour, minute, second, and
// fractional second components.
void SplitTime(int64_t jd, int32_t *hour, int32_t *min, int32_t *sec, int32_t *millisec, int32_t *microsec) {
  int64_t time = jd;

  *hour = time / K_MICRO_SECONDS_PER_HOUR;
  time -= (*hour) * K_MICRO_SECONDS_PER_HOUR;
  *min = time / K_MICRO_SECONDS_PER_MINUTE;
  time -= (*min) * K_MICRO_SECONDS_PER_MINUTE;
  *sec = time / K_MICRO_SECONDS_PER_SECOND;
  int32_t fsec = time - (*sec * K_MICRO_SECONDS_PER_SECOND);
  *millisec = fsec / 1000;
  *microsec = fsec % 1000;
}

// Check if a string value ends with string ending
bool EndsWith(const char *str, std::size_t len, const char *suffix, std::size_t suffix_len) {
  if (suffix_len > len) return false;
  return (strncmp(str + len - suffix_len, suffix, suffix_len) == 0);
}

}  // namespace

//===----------------------------------------------------------------------===//
//
// Date
//
//===----------------------------------------------------------------------===//

bool Date::IsValid() const {
  int32_t year, month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return IsValidJulianDate(year, month, day);
}

std::string Date::ToString() const {
  int32_t year, month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return fmt::format("{}-{:02}-{:02}", year, month, day);
}

int32_t Date::ExtractYear() const {
  int32_t year, month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return year;
}

int32_t Date::ExtractMonth() const {
  int32_t year, month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return month;
}

int32_t Date::ExtractDay() const {
  int32_t year, month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return day;
}

void Date::ExtractComponents(int32_t *year, int32_t *month, int32_t *day) { SplitJulianDate(value_, year, month, day); }

Date::NativeType Date::ToNative() const { return value_; }

Date Date::FromNative(Date::NativeType val) { return Date{val}; }

Date Date::FromString(const char *str, std::size_t len) {
  const char *ptr = str, *limit = ptr + len;

  // Trim leading and trailing whitespace
  while (ptr != limit && static_cast<bool>(std::isspace(*ptr))) ptr++;
  while (ptr != limit && static_cast<bool>(std::isspace(*(limit - 1)))) limit--;

  uint32_t year = 0, month = 0, day = 0;

#define DATE_ERROR throw CONVERSION_EXCEPTION(fmt::format("{} is not a valid date", std::string(str, len)));

  // Year
  while (true) {
    if (ptr == limit) DATE_ERROR;
    char c = *ptr++;
    if (static_cast<bool>(std::isdigit(c))) {
      year = year * 10 + (c - '0');
    } else if (c == '-') {
      break;
    } else {
      DATE_ERROR;
    }
  }

  // Month
  while (true) {
    if (ptr == limit) DATE_ERROR;
    char c = *ptr++;
    if (static_cast<bool>(std::isdigit(c))) {
      month = month * 10 + (c - '0');
    } else if (c == '-') {
      break;
    } else {
      DATE_ERROR;
    }
  }

  // Day
  while (true) {
    if (ptr == limit) break;
    char c = *ptr++;
    if (static_cast<bool>(std::isdigit(c))) {
      day = day * 10 + (c - '0');
    } else {
      DATE_ERROR;
    }
  }

  return Date::FromYMD(year, month, day);
}

Date Date::FromYMD(int32_t year, int32_t month, int32_t day) {
  // Check calendar date.
  if (!IsValidCalendarDate(year, month, day)) {
    throw CONVERSION_EXCEPTION(fmt::format("{}-{}-{} is not a valid date", year, month, day));
  }

  // Check if date would overflow Julian calendar.
  if (!IsValidJulianDate(year, month, day)) {
    throw CONVERSION_EXCEPTION(fmt::format("{}-{}-{} is not a valid date", year, month, day));
  }

  return Date(BuildJulianDate(year, month, day));
}

bool Date::IsValidDate(int32_t year, int32_t month, int32_t day) { return IsValidJulianDate(year, month, day); }

//===----------------------------------------------------------------------===//
//
// Timestamp
//
//===----------------------------------------------------------------------===//

int32_t Timestamp::ExtractYear() const {
  // Extract date component.
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Extract year from date.
  int32_t year, month, day;
  SplitJulianDate(date, &year, &month, &day);
  return year;
}

int32_t Timestamp::ExtractMonth() const {
  // Extract date component.
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Extract month from date.
  int32_t year, month, day;
  SplitJulianDate(date, &year, &month, &day);
  return month;
}

int32_t Timestamp::ExtractDay() const {
  // Extract date component.
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Extract day from date.
  int32_t year, month, day;
  SplitJulianDate(date, &year, &month, &day);
  return day;
}

int32_t Timestamp::ExtractHour() const {
  // Extract date component.
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Extract hour from time.
  int32_t hour, min, sec, millisec, microsec;
  SplitTime(time, &hour, &min, &sec, &millisec, &microsec);
  return hour;
}

int32_t Timestamp::ExtractMinute() const {
  // Extract date component.
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Extract minute from time.
  int32_t hour, min, sec, millisec, microsec;
  SplitTime(time, &hour, &min, &sec, &millisec, &microsec);
  return min;
}

int32_t Timestamp::ExtractSecond() const {
  // Extract date component.
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Extract second from time.
  int32_t hour, min, sec, millisec, microsec;
  SplitTime(time, &hour, &min, &sec, &millisec, &microsec);
  return sec;
}

int32_t Timestamp::ExtractMillis() const {
  // Extract date component.
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Extract millisecond from time.
  int32_t hour, min, sec, millisec, microsec;
  SplitTime(time, &hour, &min, &sec, &millisec, &microsec);
  return millisec;
}

int32_t Timestamp::ExtractMicros() const {
  // Extract date component.
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Extract microsecond from time.
  int32_t hour, min, sec, millisec, microsec;
  SplitTime(time, &hour, &min, &sec, &millisec, &microsec);
  return microsec;
}

int32_t Timestamp::ExtractDayOfWeek() const {
  int64_t date, time;
  StripTime(value_, &date, &time);

  date += 1;
  date %= 7;
  if (date < 0) date += 7;
  return date;
}

int32_t Timestamp::ExtractDayOfYear() const {
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Split date components.
  int32_t year, month, day;
  SplitJulianDate(date, &year, &month, &day);

  // Compute date of year.
  return BuildJulianDate(year, month, day) - BuildJulianDate(year, 1, 1) + 1;
}

void Timestamp::ExtractComponents(int32_t *year, int32_t *month, int32_t *day, int32_t *hour, int32_t *min,
                                  int32_t *sec, int32_t *millisec, int32_t *microsec) const {
  int64_t date, time;
  StripTime(value_, &date, &time);

  SplitJulianDate(date, year, month, day);
  SplitTime(time, hour, min, sec, millisec, microsec);
}

uint64_t Timestamp::ToNative() const { return value_; }

Timestamp Timestamp::FromNative(Timestamp::NativeType val) { return Timestamp{val}; }

std::string Timestamp::ToString() const {
  int64_t date, time;
  StripTime(value_, &date, &time);

  int32_t year, month, day, hour, min, sec, millisec, microsec;
  ExtractComponents(&year, &month, &day, &hour, &min, &sec, &millisec, &microsec);

  return fmt::format("{}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}", year, month, day, hour, min, sec,
                     millisec * 1000 + microsec);
}

Timestamp Timestamp::FromString(const char *str, std::size_t len) {
  const char *ptr = str, *limit = ptr + len;

  // Trim leading and trailing whitespace
  while (ptr != limit && static_cast<bool>(std::isspace(*ptr))) ptr++;
  while (ptr != limit && static_cast<bool>(std::isspace(*(limit - 1)))) limit--;

  if (EndsWith(ptr, static_cast<size_t>(limit - ptr), K_TIMESTAMP_SUFFIX.data(), K_TIMESTAMP_SUFFIX.size()))
    limit -= K_TIMESTAMP_SUFFIX.size();

  uint32_t year = 0, month = 0, day = 0, hour = 0, min = 0, sec = 0, milli = 0, micro = 0;

#define TS_ERROR throw CONVERSION_EXCEPTION(fmt::format("{} is not a valid timestamp", std::string(str, len)));

  // Year
  while (true) {
    if (ptr == limit) TS_ERROR;
    char c = *ptr++;
    if (static_cast<bool>(std::isdigit(c))) {
      year = year * 10 + (c - '0');
    } else if (c == '-') {
      break;
    } else {
      TS_ERROR;
    }
  }

  // Month
  while (true) {
    if (ptr == limit) TS_ERROR;
    char c = *ptr++;
    if (static_cast<bool>(std::isdigit(c))) {
      month = month * 10 + (c - '0');
    } else if (c == '-') {
      break;
    } else {
      TS_ERROR;
    }
  }

  // Day
  while (true) {
    if (ptr == limit) {
      auto date = Date::FromYMD(year, month, day);
      return date.ConvertToTimestamp();
    }
    char c = *ptr++;
    if (static_cast<bool>(std::isdigit(c))) {
      day = day * 10 + (c - '0');
    } else if (c == ' ' || c == 'T') {
      break;
    } else {
      TS_ERROR;
    }
  }

  // Hour
  while (true) {
    if (ptr == limit) TS_ERROR;
    char c = *ptr++;
    if (static_cast<bool>(std::isdigit(c))) {
      hour = hour * 10 + (c - '0');
    } else if (c == ':') {
      break;
    } else {
      TS_ERROR;
    }
  }

  // Minute
  while (true) {
    if (ptr == limit) TS_ERROR;
    char c = *ptr++;
    if (static_cast<bool>(std::isdigit(c))) {
      min = min * 10 + (c - '0');
    } else if (c == ':') {
      break;
    } else {
      TS_ERROR;
    }
  }

  // Second
  while (true) {
    if (ptr == limit) {
      return FromYMDHMS(year, month, day, hour, min, sec);
    }
    char c = *ptr++;
    if (static_cast<bool>(std::isdigit(c))) {
      sec = sec * 10 + (c - '0');
    } else if (c == '.') {
      break;
    } else if (c == 'Z') {
      return FromYMDHMSMU(year, month, day, hour, min, sec, milli, micro);
    } else if (c == '-' || c == '+') {
      return AdjustTimeZone(c, year, month, day, hour, min, sec, milli, micro, ptr, limit);
    } else {
      TS_ERROR;
    }
  }

  // Millisecond
  uint8_t count = 0;
  while (count < 3) {
    if (ptr == limit) {
      return FromYMDHMSMU(year, month, day, hour, min, sec, milli, micro);
    }
    char c = *ptr++;
    if (static_cast<bool>(std::isdigit(c))) {
      milli = milli + (c - '0') * pow(10, 2 - count);
    } else if (c == 'Z') {
      return FromYMDHMSMU(year, month, day, hour, min, sec, milli, micro);
    } else if (c == '-' || c == '+') {
      return AdjustTimeZone(c, year, month, day, hour, min, sec, milli, micro, ptr, limit);
    } else {
      TS_ERROR;
    }
    count++;
  }

  // Microsecond
  count = 0;
  while (count < 3) {
    if (ptr == limit) {
      return FromYMDHMSMU(year, month, day, hour, min, sec, milli, micro);
    }
    char c = *ptr++;
    if (static_cast<bool>(std::isdigit(c))) {
      micro = micro + (c - '0') * pow(10, 2 - count);
    } else if (c == 'Z') {
      return FromYMDHMSMU(year, month, day, hour, min, sec, milli, micro);
    } else if (c == '-' || c == '+') {
      return AdjustTimeZone(c, year, month, day, hour, min, sec, milli, micro, ptr, limit);
    } else {
      TS_ERROR;
    }
    count++;
  }

  char c = *ptr;
  if (c == '-' || c == '+') {
    ptr++;
    return AdjustTimeZone(c, year, month, day, hour, min, sec, milli, micro, ptr, limit);
  }
  if (ptr == limit) {
    return FromYMDHMSMU(year, month, day, hour, min, sec, milli, micro);
  }
  TS_ERROR;
}

Timestamp Timestamp::AdjustTimeZone(char c, int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min,
                                    int32_t sec, int32_t milli, int32_t micro, const char *ptr, const char *limit) {
  bool sign = false;
  if (c == '+') sign = true;
  int32_t timezone_diff = 0;

  // Parse timezone
  while (true) {
    if (ptr == limit) break;
    c = *ptr++;
    if (static_cast<bool>(std::isdigit(c))) {
      timezone_diff = timezone_diff * 10 + (c - '0');
    } else {
      throw CONVERSION_EXCEPTION(fmt::format("invalid timezone"));
    }
  }

  // If sign is + then must subtract hours to arrive at UTC, otherwise must add hours
  if (sign) {
    // Check valid timezone
    if (timezone_diff > 14 || timezone_diff < 0)
      throw CONVERSION_EXCEPTION(fmt::format("timezone +{} out of range", timezone_diff));

    hour -= timezone_diff;

    // Deal with overflow from timezone difference
    if (hour < 0) {
      hour = K_HOURS_PER_DAY + hour;
      day--;
      if (day < 1) {
        month--;
        if (month < 1) {
          month = K_MONTHS_PER_YEAR;
          year--;
        }
        day = K_DAYS_PER_MONTH[IsLeapYear(year)][month];
      }
    }
  } else {
    // Check valid timezone
    if (timezone_diff > 12 || timezone_diff < 0)
      throw CONVERSION_EXCEPTION(fmt::format("timezone -{} out of range", timezone_diff));

    hour += timezone_diff;

    // Deal with overflow from timezone difference
    if (hour >= K_HOURS_PER_DAY) {
      hour = hour - K_HOURS_PER_DAY;
      day++;
      if (day > K_DAYS_PER_MONTH[IsLeapYear(year)][month - 1]) {
        day = 1;
        month++;
        if (month > K_MONTHS_PER_YEAR) {
          month = 1;
          year++;
        }
      }
    }
  }

  // Construct updated timestamp
  return FromYMDHMSMU(year, month, day, hour, min, sec, milli, micro);
}

Timestamp Timestamp::FromYMDHMS(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min, int32_t sec) {
  // Check date component.
  if (!IsValidCalendarDate(year, month, day) || !IsValidJulianDate(year, month, day)) {
    throw CONVERSION_EXCEPTION(fmt::format("date field {}-{}-{} out of range", year, month, day));
  }

  // Check time component.
  if (hour < 0 || hour > K_HOURS_PER_DAY || min < 0 || min >= K_MINUTES_PER_HOUR || sec < 0 ||
      sec >= K_SECONDS_PER_MINUTE ||
      // Check for > 24:00:00.
      (hour == K_HOURS_PER_DAY && (min > 0 || sec > 0))) {
    throw CONVERSION_EXCEPTION(fmt::format("time field {}:{}:{} out of range", hour, min, sec));
  }

  const int64_t date = BuildJulianDate(year, month, day);
  const int64_t time = BuildTime(hour, min, sec);
  const int64_t result = date * K_MICRO_SECONDS_PER_DAY + time;

  // Check for major overflow.
  if ((result - time) / K_MICRO_SECONDS_PER_DAY != date) {
    throw CONVERSION_EXCEPTION(
        fmt::format("timestamp out of range {}-{}-{} {}:{}:{} out of range", year, month, day, hour, min, sec));
  }

  // Looks good.
  return Timestamp(result);
}

Timestamp Timestamp::FromYMDHMSMU(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min, int32_t sec,
                                  int32_t milli, int32_t micro) {
  // Check date component.
  if (!IsValidCalendarDate(year, month, day) || !IsValidJulianDate(year, month, day)) {
    throw CONVERSION_EXCEPTION(fmt::format("date field {}-{}-{} out of range", year, month, day));
  }

  // Check time component.
  if (hour < 0 || hour > K_HOURS_PER_DAY || min < 0 || min >= K_MINUTES_PER_HOUR || sec < 0 ||
      sec >= K_SECONDS_PER_MINUTE || milli < 0 || milli >= K_MILLISECONDS_PER_SECOND || micro < 0 ||
      micro >= K_MICROSECONDS_PER_MILLISECOND ||
      // Check for > 24:00:00.
      (hour == K_HOURS_PER_DAY && (min > 0 || sec > 0 || milli > 0 || micro > 0))) {
    throw CONVERSION_EXCEPTION(fmt::format("time field {}:{}:{}.{}{} out of range", hour, min, sec, milli, micro));
  }

  const int64_t date = BuildJulianDate(year, month, day);
  const int64_t time = BuildTime(hour, min, sec, milli, micro);
  const int64_t result = date * K_MICRO_SECONDS_PER_DAY + time;

  // Check for major overflow.
  if ((result - time) / K_MICRO_SECONDS_PER_DAY != date) {
    throw CONVERSION_EXCEPTION(fmt::format("timestamp out of range {}-{}-{} {}:{}:{}.{}{} out of range", year, month,
                                           day, hour, min, sec, milli, micro));
  }

  // Looks good.
  return Timestamp(result);
}

}  // namespace noisepage::execution::sql
