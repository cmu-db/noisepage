#include "execution/sql/runtime_types.h"

#include <string>

#include "spdlog/fmt/fmt.h"
#include "common/exception.h"

namespace terrier::execution::sql {

namespace {

constexpr int64_t kMonthsPerYear = 12;
constexpr int32_t kDaysPerMonth[2][12] = {{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
                                          {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}};
constexpr const char *const kMonthNames[] = {"January",   "February", "March",    "April",
                                             "May",       "June",     "July",     "August",
                                             "September", "October",  "November", "December"};
constexpr const char *const kShortMonthNames[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
constexpr const char *const kShortDayNames[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
constexpr const char *const kDayNames[] = {"Sunday",   "Monday", "Tuesday", "Wednesday",
                                                "Thursday", "Friday", "Saturday"};

constexpr int64_t kHoursPerDay = 24;
constexpr int64_t kMinutesPerHour = 60;
constexpr int64_t kSecondsPerMinute = 60;
constexpr int64_t kMillisecondsPerSecond = 1000;
constexpr int64_t kMicrosecondsPerMillisecond = 1000;

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

constexpr int64_t kJulianMinYear = -4713;
constexpr int64_t kJulianMinMonth = 11;
constexpr int64_t kJulianMinDay = 24;
constexpr int64_t kJulianMaxYear = 5874898;
constexpr int64_t kJulianMaxMonth = 6;
constexpr int64_t kJulianMaxDay = 3;

// Is the provided year a leap year?
bool IsLeapYear(int32_t year) { return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0); }

// Does the provided date fall into the Julian date range?
bool IsValidJulianDate(int32_t y, int32_t m, int32_t d) {
  return (y > kJulianMinYear || (y == kJulianMinYear && m >= kJulianMinMonth)) &&
      (y < kJulianMaxYear || (y == kJulianMaxYear && m < kJulianMaxMonth));
}

// Is the provided date a valid calendar date?
bool IsValidCalendarDate(int32_t year, int32_t month, int32_t day) {
  // There isn't a year 0. We represent 1 BC as year zero, 2 BC as -1, etc.
  if (year == 0) return false;

  // Month.
  if (month < 1 || month > kMonthsPerYear) return false;

  // Day.
  if (day < 1 || day > kDaysPerMonth[IsLeapYear(year)][month - 1]) return false;

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
  *month = (quad + 10) % kMonthsPerYear + 1;
}

// Split a Julian time (i.e., Julian date in microseconds) into a time and date
// component.
void StripTime(int64_t jd, int64_t *date, int64_t *time) {
  *date = jd / kMicroSecondsPerDay;
  *time = jd - (*date * kMicroSecondsPerDay);
}

// Given hour, minute, second, millisecond, and microsecond components, build a time in microseconds.
int64_t BuildTime(int32_t hour, int32_t min, int32_t sec, int32_t milli = 0, int32_t micro = 0) {
  return (((hour * kMinutesPerHour + min) * kSecondsPerMinute) * kMicroSecondsPerSecond) +
      sec * kMicroSecondsPerSecond + milli * kMillisecondsPerSecond + micro;
}

// Given a time in microseconds, split it into hour, minute, second, and
// fractional second components.
void SplitTime(int64_t jd, int32_t *hour, int32_t *min, int32_t *sec, double *fsec) {
  int64_t time = jd;

  *hour = time / kMicroSecondsPerHour;
  time -= (*hour) * kMicroSecondsPerHour;
  *min = time / kMicroSecondsPerMinute;
  time -= (*min) * kMicroSecondsPerMinute;
  *sec = time / kMicroSecondsPerSecond;
  *fsec = time - (*sec * kMicroSecondsPerSecond);
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

void Date::ExtractComponents(int32_t *year, int32_t *month, int32_t *day) {
  SplitJulianDate(value_, year, month, day);
}

Date::NativeType Date::ToNative() const { return value_; }

Date Date::FromNative(Date::NativeType val) { return Date {val}; }

std::pair<bool, Date> Date::FromString(const char *str, std::size_t len) {
  const char *ptr = str, *limit = ptr + len;

  // Trim leading and trailing whitespace
  while (ptr != limit && std::isspace(*ptr)) ptr++;
  while (ptr != limit && std::isspace(*(limit - 1))) limit--;

  uint32_t year = 0, month = 0, day = 0;

  // Year
  while (true) {
    if (ptr == limit) return {false, {}};
    char c = *ptr++;
    if (std::isdigit(c)) {
      year = year * 10 + (c - '0');
    } else if (c == '-') {
      break;
    } else {
      return {false, {}};
    }
  }

  // Month
  while (true) {
    if (ptr == limit) return {false, {}};
    char c = *ptr++;
    if (std::isdigit(c)) {
      month = month * 10 + (c - '0');
    } else if (c == '-') {
      break;
    } else {
      return {false, {}};
    }
  }

  // Day
  while (true) {
    if (ptr == limit) break;
    char c = *ptr++;
    if (std::isdigit(c)) {
      day = day * 10 + (c - '0');
    } else {
      return {false, {}};
    }
  }

  return Date::FromYMD(year, month, day);
}

std::pair<bool, Date> Date::FromYMD(int32_t year, int32_t month, int32_t day) {
  // Check calendar date.
  if (!IsValidCalendarDate(year, month, day)) {
    return {false, {}};
  }

  // Check if date would overflow Julian calendar.
  if (!IsValidJulianDate(year, month, day)) {
    return {false, {}};
  }

  return {true, Date(BuildJulianDate(year, month, day))};
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

  // Extract month from date.
  int32_t hour, min, sec;
  double fsec;
  SplitTime(time, &hour, &min, &sec, &fsec);
  return hour;
}

int32_t Timestamp::ExtractMinute() const {
  // Extract date component.
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Extract month from date.
  int32_t hour, min, sec;
  double fsec;
  SplitTime(time, &hour, &min, &sec, &fsec);
  return min;
}

int32_t Timestamp::ExtractSecond() const {
  // Extract date component.
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Extract month from date.
  int32_t hour, min, sec;
  double fsec;
  SplitTime(time, &hour, &min, &sec, &fsec);
  return sec;
}

int32_t Timestamp::ExtractMillis() const {
  // Extract date component.
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Extract month from date.
  int32_t hour, min, sec;
  double fsec;
  SplitTime(time, &hour, &min, &sec, &fsec);
  return sec * 1000.0 + fsec / 1000.0;
}

int32_t Timestamp::ExtractMicros() const {
  // Extract date component.
  int64_t date, time;
  StripTime(value_, &date, &time);

  // Extract month from date.
  int32_t hour, min, sec;
  double fsec;
  SplitTime(time, &hour, &min, &sec, &fsec);
  return sec + fsec / 1000000.0;
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
                                  int32_t *sec, double *fsec){
  int64_t date, time;
  StripTime(value_, &date, &time);

  SplitJulianDate(date, year, month, day);
  SplitTime(time, hour, min, sec, fsec);
}

uint64_t Timestamp::ToNative() const { return value_; }

Timestamp Timestamp::FromNative(Timestamp::NativeType val) { return Timestamp{val}; }

std::string Timestamp::ToString() const {
  int64_t date, time;
  StripTime(value_, &date, &time);

  int32_t year, month, day;
  SplitJulianDate(date, &year, &month, &day);

  int32_t hour, min, sec;
  double fsec;
  SplitTime(time, &hour, &min, &sec, &fsec);

  return fmt::format("{}-{:02}-{:02} {:02}:{:02}:{:02}", year, month, day, hour, min, sec);
}

std::pair<bool, Timestamp> Timestamp::FromString(const char *str, std::size_t len) {
  const char *ptr = str, *limit = ptr + len;

  // Trim leading and trailing whitespace
  while (ptr != limit && std::isspace(*ptr)) ptr++;
  while (ptr != limit && std::isspace(*(limit - 1))) limit--;

  uint32_t year = 0, month = 0, day = 0, hour = 0, min = 0, sec = 0, milli = 0, micro= 0;

  // Year
  while (true) {
    if (ptr == limit) return {false, {}};
    char c = *ptr++;
    if (std::isdigit(c)) {
      year = year * 10 + (c - '0');
    } else if (c == '-') {
      break;
    } else {
      return {false, {}};
    }
  }

  // Month
  while (true) {
    if (ptr == limit) return {false, {}};
    char c = *ptr++;
    if (std::isdigit(c)) {
      month = month * 10 + (c - '0');
    } else if (c == '-') {
      break;
    } else {
      return {false, {}};
    }
  }

  // Day
  while (true) {
    if (ptr == limit) {
      return (Date::FromYMD(year, month, day).second).ConvertToTimestamp();
    }
    char c = *ptr++;
    if (std::isdigit(c)) {
      day = day * 10 + (c - '0');
    } else if (c == ' ' || c == 'T') {
      break;
    } else {
      return {false, {}};
    }
  }

  // Hour
  while (true) {
    if (ptr == limit) return {false, {}};
    char c = *ptr++;
    if (std::isdigit(c)) {
      hour = hour * 10 + (c - '0');
    } else if (c == ':') {
      break;
    } else {
      return {false, {}};
    }
  }

  // Minute
  while (true) {
    if (ptr == limit) return {false, {}};
    char c = *ptr++;
    if (std::isdigit(c)) {
      min = min * 10 + (c - '0');
    } else if (c == ':') {
      break;
    } else {
      return {false, {}};
    }
  }

  // Second
  while (true) {
    if (ptr == limit) {
      return FromYMDHMS(year, month, day, hour, min, sec);
    }
    char c = *ptr++;
    if (std::isdigit(c)) {
      sec = sec * 10 + (c - '0');
    } else if (c == '.') {
      break;
    } else if (c == '-' || c == 'Z') {
      return FromYMDHMS(year, month, day, hour, min, sec);
    } else {
      return {false, {}};
    }
  }

  // Millisecond
  uint8_t count = 0;
  while (count < 3) {
    if (ptr == limit) {
      return FromYMDHMSMU(year, month, day, hour, min, sec, milli, micro);
    }
    char c = *ptr++;
    if (std::isdigit(c)) {
      milli = milli + (c - '0') * pow(10, 2 - count);
    } else if (c == '-' || c == 'Z') {
      return FromYMDHMS(year, month, day, hour, min, sec);
    } else {
      return {false, {}};
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
    if (std::isdigit(c)) {
      micro = micro + (c - '0') * pow(10, 2 - count);
    } else if (c == '-' || c == 'Z') {
      return FromYMDHMSMU(year, month, day, hour, min, sec, milli, micro);
    } else {
      return {false, {}};
    }
    count++;
  }

  return {false, {}};
}

std::pair<bool, Timestamp> Timestamp::FromYMDHMS(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min,
                                int32_t sec) {
  // Check date component.
  if (!IsValidCalendarDate(year, month, day) || !IsValidJulianDate(year, month, day)) {
    return {false, {}};
  }

  // Check time component.
  if (hour < 0 || min < 0 || min > kMinutesPerHour - 1 || sec < 0 || sec > kSecondsPerMinute ||
      hour > kHoursPerDay ||
      // Check for > 24:00:00.
      (hour == kHoursPerDay && (min > 0 || sec > 0))) {
    return {false, {}};
  }

  const int64_t date = BuildJulianDate(year, month, day);
  const int64_t time = BuildTime(hour, min, sec);
  const int64_t result = date * kMicroSecondsPerDay + time;

  // Check for major overflow.
  if ((result - time) / kMicroSecondsPerDay != date) {
    return {false, {}};
  }

  // Looks good.
  return {true, Timestamp(result)};
}


std::pair<bool, Timestamp> Timestamp::FromYMDHMSMU(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min,
                                  int32_t sec, int32_t milli, int32_t micro) {
  // Check date component.
  if (!IsValidCalendarDate(year, month, day) || !IsValidJulianDate(year, month, day)) {
    return {false, {}};
  }

  // Check time component.
  if (hour < 0 || hour > kHoursPerDay || min < 0 || min > kMinutesPerHour - 1 || sec < 0 ||
      sec > kSecondsPerMinute - 1 || milli < 0 || milli > kMillisecondsPerSecond - 1 || micro < 0 ||
      micro > kMicrosecondsPerMillisecond - 1 ||
      // Check for > 24:00:00.
      (hour == kHoursPerDay && (min > 0 || sec > 0 || milli > 0 || micro > 0))) {
    return {false, {}};
  }

  const int64_t date = BuildJulianDate(year, month, day);
  const int64_t time = BuildTime(hour, min, sec, milli, micro);
  const int64_t result = date * kMicroSecondsPerDay + time;

  // Check for major overflow.
  if ((result - time) / kMicroSecondsPerDay != date) {
    return {false, {}};
  }

  // Looks good.
  return {true, Timestamp(result)};
}

}  // namespace terrier::execution::sql
