#include "execution/sql/runtime_types.h"

#include <string>

#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "execution/sql/decimal_magic_numbers.h"
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

//===----------------------------------------------------------------------===//
//
// Decimal
//
//===----------------------------------------------------------------------===//

// TODO(WAN): The Decimal code below has been left as-is, but could probably be simplified and cleaned up further.
//  I am leaving it alone because I don't think people will need to modify or look at this code often, assuming that
//  works (it does contain parsing logic etc. that may require changes in the future).

template <typename T>
void Decimal<T>::RoundUpAndSet(std::string input, uint32_t precision) {
  value_ = 0;

  if (input.empty()) return;

  uint32_t pos = 0;

  bool is_negative = false;
  if (input[pos] == '-') {
    pos++;
    is_negative = true;
  }

  while (pos < input.size() && input[pos] != '.') {
    value_ += input[pos] - '0';
    value_ *= 10;
    pos++;
  }

  if (precision == 0) {
    value_ /= 10;
    if (pos != input.size()) {
      if (pos + 1 < input.size()) {
        pos++;
        if (input[pos] - '0' > 5) {
          value_ += 1;
        } else if (input[pos] - '0' == 5 && value_ % 2 == 1) {
          value_ += 1;
        }
      }
    }
    if (is_negative) {
      value_ = -value_;
    }
    return;
  }

  // No decimal point case
  if (pos == input.size()) {
    for (uint32_t i = 0; i < precision - 1; i++) {
      value_ *= 10;
    }
    if (is_negative) {
      value_ = -value_;
    }
    return;
  }
  // Skip decimal point
  pos++;
  // Nothing after decimal point case
  if (pos == input.size()) {
    for (uint32_t i = 0; i < precision - 1; i++) {
      value_ *= 10;
    }
    if (is_negative) {
      value_ = -value_;
    }
    return;
  }

  for (uint32_t i = 1; i < precision; i++) {
    if (pos < input.size()) {
      value_ += input[pos] - '0';
      value_ *= 10;
      pos++;
    } else {
      for (uint32_t j = i; j < precision; j++) {
        value_ *= 10;
      }
      if (is_negative) {
        value_ = -value_;
      }
      return;
    }
  }

  if (pos == input.size()) {
    if (is_negative) {
      value_ = -value_;
    }
    return;
  }

  if (pos == input.size() - 1) {
    // No Rounding required
    value_ += input[pos] - '0';
  } else {
    if (input[pos + 1] - '0' > 5) {
      // Round Up
      value_ += input[pos] - '0' + 1;
    } else if (input[pos + 1] - '0' < 5) {
      // No Rounding will happen
      value_ += input[pos] - '0';
    } else {
      if ((input[pos] - '0') % 2 == 0) {
        // Round up if ODD
        value_ += input[pos] - '0';
      } else {
        // Round up if ODD
        value_ += input[pos] - '0' + 1;
      }
    }
  }

  if (is_negative) {
    value_ = -value_;
  }
}

void CalculateMultiWordProduct128(const uint128_t *const half_words_a, const uint128_t *const half_words_b,
                                  uint128_t *half_words_result, uint32_t m, uint32_t n) {
  uint128_t k, t;
  uint32_t i, j;
  constexpr const uint128_t bottom_mask = (uint128_t{1} << 64) - 1;
  for (i = 0; i < m; i++) half_words_result[i] = 0;
  for (j = 0; j < n; j++) {
    k = 0;
    for (i = 0; i < m; i++) {
      t = half_words_a[i] * half_words_b[j] + half_words_result[i + j] + k;
      half_words_result[i + j] = t & bottom_mask;
      k = t >> 64;
    }
    half_words_result[j + m] = k;
  }
}

int Nlz128(uint128_t x) {
  // Hacker's Delight [2E Figure 5-19] has a method for computing the number of
  // leading zeroes, but their method is not applicable as we need 128 bits.
  constexpr uint128_t a = (static_cast<uint128_t>(0x0000000000000000) << 64) | 0xFFFFFFFFFFFFFFFF;
  constexpr uint128_t b = (static_cast<uint128_t>(0x00000000FFFFFFFF) << 64) | 0xFFFFFFFFFFFFFFFF;
  constexpr uint128_t c = (static_cast<uint128_t>(0x0000FFFFFFFFFFFF) << 64) | 0xFFFFFFFFFFFFFFFF;
  constexpr uint128_t d = (static_cast<uint128_t>(0x00FFFFFFFFFFFFFF) << 64) | 0xFFFFFFFFFFFFFFFF;
  constexpr uint128_t e = (static_cast<uint128_t>(0x0FFFFFFFFFFFFFFF) << 64) | 0xFFFFFFFFFFFFFFFF;
  constexpr uint128_t f = (static_cast<uint128_t>(0x3FFFFFFFFFFFFFFF) << 64) | 0xFFFFFFFFFFFFFFFF;
  constexpr uint128_t g = (static_cast<uint128_t>(0x7FFFFFFFFFFFFFFF) << 64) | 0xFFFFFFFFFFFFFFFF;

  // TODO(WAN): clang believes that this can result in a shift of 128 bits, which would result in undefined behavior.
  //  clang has historically been reliable, so let's double-check this one eventually.
  // clang made me comment this
  // if (x == 0) return (128);
  int n = 0;
  if (x <= a) {
    n = n + 64;
    x = x << 64;
  }
  if (x <= b) {
    n = n + 32;
    x = x << 32;
  }
  if (x <= c) {
    n = n + 16;
    x = x << 16;
  }
  if (x <= d) {
    n = n + 8;
    x = x << 8;
  }
  if (x <= e) {
    n = n + 4;
    x = x << 4;
  }
  if (x <= f) {
    n = n + 2;
    x = x << 2;
  }
  if (x <= g) {
    n = n + 1;
  }
  return n;
}

uint128_t CalculateUnsignedLongDivision128(uint128_t u1, uint128_t u0, uint128_t v) {
  if (u1 >= v) {
    // Result will overflow from 128 bits
    throw EXECUTION_EXCEPTION(fmt::format("Decimal Overflow from 128 bits"), common::ErrorCode::ERRCODE_DATA_EXCEPTION);
  }

  // Base 2^64
  uint128_t b = 1;
  b = b << 64;

  uint128_t un1, un0, vn1, vn0, q1, q0, un32, un21, un10, rhat;
  int128_t s = Nlz128Fast(v);

  // Normalize everything
  v = v << s;
  vn1 = v >> 64;
  vn0 = v & 0xFFFFFFFFFFFFFFFF;

  un32 = (u1 << s) | ((u0 >> (128 - s)) & ((-s) >> 127));
  un10 = u0 << s;
  un1 = un10 >> 64;
  un0 = un10 & 0xFFFFFFFFFFFFFFFF;

  q1 = un32 / vn1;
  rhat = un32 - q1 * vn1;

  do {
    if ((q1 >= b) || (q1 * vn0 > b * rhat + un1)) {
      q1 = q1 - 1;
      rhat = rhat + vn1;
    } else {
      break;
    }
  } while (rhat < b);

  un21 = un32 * b + un1 - q1 * v;

  q0 = un21 / vn1;
  rhat = un21 - q0 * vn1;

  do {
    if ((q0 >= b) || (q0 * vn0 > b * rhat + un0)) {
      q0 = q0 - 1;
      rhat = rhat + vn1;
    } else {
      break;
    }
  } while (rhat < b);

  return q1 * b + q0;
}

template <typename T>
void Decimal<T>::MultiplyAndSet(const Decimal<T> &input, uint32_t precision) {
  // 1. Multiply with the overflow check.
  // 2. If overflow, divide by 10^precision using 256-bit magic number division.
  // 3. If no overflow, divide by 10^precision using 128-bit magic number division.
  constexpr const uint128_t bottom_mask = (uint128_t{1} << 64) - 1;
  constexpr const uint128_t top_mask = ~bottom_mask;

  // First input
  uint128_t a = value_;
  // Second input
  uint128_t b = input.GetValue();
  // Split into half words
  uint128_t half_words_a[2];
  uint128_t half_words_b[2];

  half_words_a[0] = a & bottom_mask;
  half_words_a[1] = (a & top_mask) >> 64;

  half_words_b[0] = b & bottom_mask;
  half_words_b[1] = (b & top_mask) >> 64;

  // Calculate 256 bit result
  uint128_t half_words_result[4];
  CalculateMultiWordProduct128(half_words_a, half_words_b, half_words_result, 2, 2);

  if (half_words_result[2] == 0 && half_words_result[3] == 0) {
    // TODO(Rohan): Optimize by sending in an array of half words
    value_ = half_words_result[0] | (half_words_result[1] << 64);
    UnsignedDivideConstant128BitPowerOfTen(precision);
    return;
  }

  // Magic number half words
  uint128_t magic[4];
  magic[0] = MAGIC_ARRAY[precision][3];
  magic[1] = MAGIC_ARRAY[precision][2];
  magic[2] = MAGIC_ARRAY[precision][1];
  magic[3] = MAGIC_ARRAY[precision][0];

  uint32_t magic_p = MAGIC_P_AND_ALGO_ARRAY[precision][0] - 256;

  if (MAGIC_P_AND_ALGO_ARRAY[precision][1] == 0) {
    // Overflow Algorithm 1 - Magic number is < 2^256

    // Magic Result
    uint128_t half_words_magic_result[8];
    // TODO(Rohan): Make optimization to calculate only upper half of the word
    CalculateMultiWordProduct128(half_words_result, magic, half_words_magic_result, 4, 4);
    // Get the higher order result
    uint128_t result_lower = half_words_magic_result[4] | (half_words_magic_result[5] << 64);
    uint128_t result_upper = half_words_magic_result[6] | (half_words_magic_result[7] << 64);

    uint128_t overflow_checker = result_upper >> magic_p;
    if (overflow_checker > 0) {
      // Result will overflow from 128 bits
      throw EXECUTION_EXCEPTION(fmt::format("Result overflow > 128 bits"), common::ErrorCode::ERRCODE_DATA_EXCEPTION);
    }

    result_lower = result_lower >> magic_p;
    result_upper = result_upper << (128 - magic_p);
    value_ = result_lower | result_upper;
    return;
  }
  // Overflow Algorithm 2 - Magic number is > 2^256

  // Magic Result
  uint128_t half_words_magic_result[8];
  // TODO(Rohan): Make optimization to calculate only upper half of the word
  CalculateMultiWordProduct128(half_words_result, magic, half_words_magic_result, 4, 4);
  // Get the higher order result
  uint128_t result_lower = half_words_result[0] | (half_words_result[1] << 64);
  uint128_t result_upper = half_words_result[2] | (half_words_result[3] << 64);

  uint128_t add_lower = half_words_magic_result[4] | (half_words_magic_result[5] << 64);
  uint128_t add_upper = half_words_magic_result[6] | (half_words_magic_result[7] << 64);

  // Perform addition
  result_lower += add_lower;
  result_upper += add_upper;
  // carry bit using conditional instructions
  result_upper += static_cast<uint128_t>(result_lower < add_lower);

  uint128_t overflow_checker = result_upper >> magic_p;
  if ((overflow_checker > 0) || (result_upper < add_upper)) {
    // Result will overflow from 128 bits
    throw EXECUTION_EXCEPTION(fmt::format("Result overflow > 128 bits"), common::ErrorCode::ERRCODE_DATA_EXCEPTION);
  }

  // We know that we only retain the lower 128 bits so there is no need of shri.
  // We can safely drop the additional carry bit.
  result_lower = result_lower >> magic_p;
  result_upper = result_upper << (128 - magic_p);
  value_ = result_lower | result_upper;
}

template <typename T>
void Decimal<T>::UnsignedDivideConstant128BitPowerOfTen(uint32_t power) {
  constexpr const uint128_t bottom_mask = (uint128_t{1} << 64) - 1;
  constexpr const uint128_t top_mask = ~bottom_mask;

  // First input
  uint128_t a = value_;

  // Split into half words
  uint128_t half_words_a[2];
  uint128_t half_words_b[2];

  half_words_a[0] = a & bottom_mask;
  half_words_a[1] = (a & top_mask) >> 64;

  half_words_b[0] = magic_map128_bit_power_ten[power].lower_;
  half_words_b[1] = magic_map128_bit_power_ten[power].upper_;

  // Calculate 256 bit result
  uint128_t half_words_result[4];
  // TODO(Rohan): Calculate only upper half
  CalculateMultiWordProduct128(half_words_a, half_words_b, half_words_result, 2, 2);

  uint32_t magic_p = magic_map128_bit_power_ten[power].p_ - 128;

  if (magic_map128_bit_power_ten[power].algo_ == 0) {
    // Overflow Algorithm 1 - Magic number is < 2^128

    uint128_t result_upper = half_words_result[2] | (half_words_result[3] << 64);
    value_ = result_upper >> magic_p;
  } else {
    // Overflow Algorithm 2 - Magic number is > 2^128

    uint128_t result_upper = half_words_result[2] | (half_words_result[3] << 64);
    uint128_t add_upper = value_;

    /*Perform addition*/
    result_upper += add_upper;

    auto carry = static_cast<uint128_t>(result_upper < add_upper);
    carry = carry << 127;
    // shrxi 1
    result_upper = result_upper >> 1;
    result_upper |= carry;

    value_ = result_upper >> (magic_p - 1);
  }
}

template <typename T>
void Decimal<T>::UnsignedDivideConstant128Bit(uint128_t constant) {
  if (constant == 1) {
    return;
  }

  constexpr const uint128_t bottom_mask = (uint128_t{1} << 64) - 1;
  constexpr const uint128_t top_mask = ~bottom_mask;

  // Power of 2
  if ((constant & (constant - 1)) == 0) {
    uint32_t power_of_two = power_two[constant];
    uint128_t numerator = value_;
    numerator = numerator >> power_of_two;
    value_ = numerator;
    return;
  }

  // Cannot optimize if we do not have the magic number with us
  if (magic_map128_bit_constant_division.count(constant) == 0) {
    uint128_t numerator = value_;
    numerator = numerator / constant;
    value_ = numerator;
    return;
  }

  // First input
  uint128_t a = value_;

  // Split into half words
  uint128_t half_words_a[2];
  uint128_t half_words_b[2];

  half_words_a[0] = a & bottom_mask;
  half_words_a[1] = (a & top_mask) >> 64;

  half_words_b[0] = magic_map128_bit_constant_division[constant].lower_;
  half_words_b[1] = magic_map128_bit_constant_division[constant].upper_;

  // Calculate 256 bit result
  uint128_t half_words_result[4];
  // TODO(Rohan): Calculate only upper half
  CalculateMultiWordProduct128(half_words_a, half_words_b, half_words_result, 2, 2);

  uint32_t magic_p = magic_map128_bit_constant_division[constant].p_ - 128;

  if (magic_map128_bit_constant_division[constant].algo_ == 0) {
    // Overflow Algorithm 1 - Magic number is < 2^128

    uint128_t result_upper = half_words_result[2] | (half_words_result[3] << 64);
    value_ = result_upper >> magic_p;
  } else {
    // Overflow Algorithm 2 - Magic number is > 2^128

    uint128_t result_upper = half_words_result[2] | (half_words_result[3] << 64);
    uint128_t add_upper = value_;

    /*Perform addition*/
    result_upper += add_upper;

    auto carry = static_cast<uint128_t>(result_upper < add_upper);
    carry = carry << 127;
    // shrxi 1
    result_upper = result_upper >> 1;
    result_upper |= carry;

    value_ = result_upper >> (magic_p - 1);
  }
}

template <typename T>
void Decimal<T>::SignedMultiplyWithDecimal(Decimal<T> input, uint32_t lower_precision) {
  bool negative_result = (value_ < 0) != (input.GetValue() < 0);

  // The method in Hacker Delight 2-14 is not used because shift needs to be agnostic of underlying T
  // Will be needed to change in the future when storage optimizations happen
  if (value_ < 0) {
    value_ = 0 - value_;
  }

  if (input.GetValue() < 0) {
    input.SetValue(-input.GetValue());
  }

  MultiplyAndSet(input, lower_precision);

  if (negative_result) {
    value_ = 0 - value_;
  }
}

template <typename T>
void Decimal<T>::SignedMultiplyWithConstant(int64_t input) {
  bool negative_result = (value_ < 0) != (input < 0);

  // The method in Hacker Delight 2-14 is not used because shift needs to be agnostic of underlying T
  // Will be needed to change in the future when storage optimizations happen
  if (value_ < 0) {
    value_ = 0 - value_;
  }

  constexpr const uint128_t bottom_mask = (uint128_t{1} << 64) - 1;
  constexpr const uint128_t top_mask = ~bottom_mask;

  // First input
  uint128_t a = value_;

  // Second input
  uint128_t b;
  if (input < 0) {
    b = input;
  } else {
    b = -input;
  }

  // Split into half words
  uint128_t half_words_a[2];
  uint128_t half_words_b[2];

  half_words_a[0] = a & bottom_mask;
  half_words_a[1] = (a & top_mask) >> 64;

  half_words_b[0] = b & bottom_mask;
  half_words_b[1] = (b & top_mask) >> 64;

  // Calculate 256 bit result
  uint128_t half_words_result[4];
  CalculateMultiWordProduct128(half_words_a, half_words_b, half_words_result, 2, 2);

  if (half_words_result[2] == 0 && half_words_result[3] == 0) {
    value_ = half_words_result[0] | (half_words_result[1] << 64);
  } else {
    throw EXECUTION_EXCEPTION(fmt::format("Result overflow > 128 bits"), common::ErrorCode::ERRCODE_DATA_EXCEPTION);
  }

  if (negative_result) {
    value_ = 0 - value_;
  }
}

template <typename T>
void Decimal<T>::SignedDivideWithConstant(int64_t input) {
  bool negative_result = (value_ < 0) != (input < 0);

  // The method in Hacker Delight 2-14 is not used because shift needs to be agnostic of underlying T
  // Will be needed to change in the future when storage optimizations happen
  if (value_ < 0) {
    value_ = 0 - value_;
  }

  uint128_t constant;
  if (input < 0) {
    constant = -input;
  } else {
    constant = input;
  }

  UnsignedDivideConstant128Bit(constant);

  if (negative_result) {
    value_ = 0 - value_;
  }
}

template <typename T>
void Decimal<T>::SignedDivideWithDecimal(Decimal<T> input, uint32_t denominator_precision) {
  constexpr const uint128_t bottom_mask = (uint128_t{1} << 64) - 1;
  constexpr const uint128_t top_mask = ~bottom_mask;

  bool negative_result = (value_ < 0) != (input.GetValue() < 0);

  // The method in Hacker Delight 2-14 is not used because shift needs to be agnostic of underlying T
  // Will be needed to change in the future when storage optimizations happen
  if (value_ < 0) {
    value_ = 0 - value_;
  }

  uint128_t constant;
  if (input < 0) {
    constant = -input.GetValue();
  } else {
    constant = input.GetValue();
  }

  // Always keep the result in numerator precision
  // Multiply with 10^(denominator precision)
  // Split into half words
  uint128_t half_words_a[2];
  uint128_t half_words_b[2];

  half_words_a[0] = value_ & bottom_mask;
  half_words_a[1] = (value_ & top_mask) >> 64;

  half_words_b[0] = power_of_ten[denominator_precision][1];
  half_words_b[1] = power_of_ten[denominator_precision][0];

  uint128_t half_words_result[4];
  CalculateMultiWordProduct128(half_words_a, half_words_b, half_words_result, 2, 2);

  if (half_words_result[2] == 0 && half_words_result[3] == 0) {
    value_ = half_words_result[0] | (half_words_result[1] << 64);
    UnsignedDivideConstant128Bit(constant);
  } else {
    if (magic_map256_bit_constant_division.count(constant) > 0) {
      value_ = UnsignedMagicDivideConstantNumerator256Bit(half_words_result, constant);
    } else {
      value_ = CalculateUnsignedLongDivision128(half_words_result[2] | (half_words_result[3] << 64),
                                                half_words_result[0] | (half_words_result[1] << 64), constant);
    }
  }

  if (negative_result) {
    value_ = 0 - value_;
  }
}

template <typename T>
uint128_t Decimal<T>::UnsignedMagicDivideConstantNumerator256Bit(uint128_t *dividend, uint128_t constant) {
  // Magic number half words
  uint128_t magic[4];

  magic[0] = magic_map256_bit_constant_division[constant].d_;
  magic[1] = magic_map256_bit_constant_division[constant].c_;
  magic[2] = magic_map256_bit_constant_division[constant].b_;
  magic[3] = magic_map256_bit_constant_division[constant].a_;

  uint32_t magic_p = magic_map256_bit_constant_division[constant].p_ - 256;

  if (magic_map256_bit_constant_division[constant].algo_ == 0) {
    // Overflow Algorithm 1 - Magic number is < 2^256

    // Magic Result
    uint128_t half_words_magic_result[8];
    // TODO(Rohan): Make optimization to calculate only upper half of the word
    CalculateMultiWordProduct128(dividend, magic, half_words_magic_result, 4, 4);
    // Get the higher order result
    uint128_t result_lower = half_words_magic_result[4] | (half_words_magic_result[5] << 64);
    uint128_t result_upper = half_words_magic_result[6] | (half_words_magic_result[7] << 64);

    uint128_t overflow_checker = result_upper >> magic_p;
    if (overflow_checker > 0) {
      // Result will overflow from 128 bits
      throw EXECUTION_EXCEPTION(fmt::format("Result overflow > 128 bits"), common::ErrorCode::ERRCODE_DATA_EXCEPTION);
    }

    result_lower = result_lower >> magic_p;
    result_upper = result_upper << (128 - magic_p);
    return result_lower | result_upper;
  }
  // Overflow Algorithm 2 - Magic number is > 2^256

  // Magic Result
  uint128_t half_words_magic_result[8];
  // TODO(Rohan): Make optimization to calculate only upper half of the word
  CalculateMultiWordProduct128(dividend, magic, half_words_magic_result, 4, 4);
  // Get the higher order result
  uint128_t result_lower = dividend[0] | (dividend[1] << 64);
  uint128_t result_upper = dividend[2] | (dividend[3] << 64);

  uint128_t add_lower = half_words_magic_result[4] | (half_words_magic_result[5] << 64);
  uint128_t add_upper = half_words_magic_result[6] | (half_words_magic_result[7] << 64);

  // Perform addition
  result_lower += add_lower;
  result_upper += add_upper;
  // carry bit using conditional instructions
  result_upper += static_cast<uint128_t>(result_lower < add_lower);

  uint128_t overflow_checker = result_upper >> magic_p;
  if ((overflow_checker > 0) || (result_upper < add_upper)) {
    // Result will overflow from 128 bits
    throw EXECUTION_EXCEPTION(fmt::format("Result overflow > 128 bits"), common::ErrorCode::ERRCODE_DATA_EXCEPTION);
  }

  // We know that we only retain the lower 128 bits so there is no need of shri
  // We can safely drop the additional carry bit
  result_lower = result_lower >> magic_p;
  result_upper = result_upper << (128 - magic_p);
  return result_lower | result_upper;
}

template <typename T>
int Decimal<T>::SetMaxmPrecision(std::string input) {
  value_ = 0;

  if (input.empty()) return 0;

  uint32_t pos = 0;

  bool is_negative = false;
  if (input[pos] == '-') {
    pos++;
    is_negative = true;
  }

  while (pos < input.size() && input[pos] != '.') {
    value_ += input[pos] - '0';
    if (pos < input.size() - 1) {
      value_ *= 10;
    }
    pos++;
  }

  if (pos == input.size()) {
    if (is_negative) {
      value_ = -value_;
    }
    return 0;
  }
  pos++;

  if (pos == input.size()) {
    value_ /= 10;
    if (is_negative) {
      value_ = -value_;
    }
    return 0;
  }

  int precision = 0;
  while (pos < input.size()) {
    value_ += input[pos] - '0';
    if (pos < input.size() - 1) {
      value_ *= 10;
    }
    pos++;
    precision++;
  }

  if (is_negative) {
    value_ = -value_;
  }
  return precision;
}

template class Decimal<int128_t>;
template class Decimal<int64_t>;
template class Decimal<int32_t>;
}  // namespace noisepage::execution::sql
