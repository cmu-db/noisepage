#include "execution/sql/runtime_types.h"

#include <string>

#include "common/exception.h"
#include "util/time_util.h"

namespace terrier::execution::sql {

namespace {

bool IsValidJulianDate(int32_t year, uint32_t month, uint32_t day) {
  return year <= 9999 && month >= 1 && month <= 12 && day >= 1 && day <= 31;
}

uint32_t BuildJulianDate(int32_t year, uint32_t month, uint32_t day) {
  return terrier::util::TimeConvertor::PostgresDate2J(year, month, day);
}

uint32_t BuildJulianDate(date::year_month_day ymd) {
  auto year = static_cast<int32_t>(ymd.year());
  auto month = static_cast<uint32_t>(ymd.month());
  auto day = static_cast<uint32_t>(ymd.day());
  return BuildJulianDate(year, month, day);
}

void SplitJulianDate(uint32_t julian_date, int32_t *year, uint32_t *month, uint32_t *day) {
  auto ymd = terrier::util::TimeConvertor::PostgresJ2Date(julian_date);

  *year = static_cast<int32_t>(ymd.year());
  *month = static_cast<uint32_t>(ymd.month());
  *day = static_cast<uint32_t>(ymd.day());
}

}  // namespace

bool Date::IsValid() const noexcept {
  int32_t year;
  uint32_t month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return IsValidJulianDate(year, month, day);
}

std::string Date::ToString() const { return terrier::util::TimeConvertor::FormatDate(type::date_t{value_}); }

int32_t Date::ExtractYear() const noexcept {
  int32_t year;
  uint32_t month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return year;
}

uint32_t Date::ExtractMonth() const noexcept {
  int32_t year;
  uint32_t month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return month;
}

uint32_t Date::ExtractDay() const noexcept {
  int32_t year;
  uint32_t month, day;
  SplitJulianDate(value_, &year, &month, &day);
  return day;
}

void Date::ExtractComponents(int32_t *year, uint32_t *month, uint32_t *day) {
  SplitJulianDate(value_, year, month, day);
}

uint32_t Date::ToNative() const noexcept { return value_; }

Date Date::FromNative(Date::NativeType val) { return Date{val}; }

Date Date::FromString(const std::string &str) {
  auto result = terrier::util::TimeConvertor::ParseDate(str);
  if (!result.first) {
    throw CONVERSION_EXCEPTION("Invalid date.");
  }
  return Date(BuildJulianDate(terrier::util::TimeConvertor::YMDFromDate(result.second)));
}

Date Date::FromYMD(int32_t year, uint32_t month, uint32_t day) {
  if (!IsValidJulianDate(year, month, day)) {
    throw CONVERSION_EXCEPTION("Invalid date.");
  }
  return Date(BuildJulianDate(year, month, day));
}

bool Date::IsValidDate(int32_t year, uint32_t month, uint32_t day) { return IsValidJulianDate(year, month, day); }

std::string Timestamp::ToString() const {
  return terrier::util::TimeConvertor::FormatTimestamp(type::timestamp_t{value_});
}

uint64_t Timestamp::ToNative() const noexcept { return value_; }

Timestamp Timestamp::FromNative(Timestamp::NativeType val) { return Timestamp{val}; }

Timestamp Timestamp::FromHMSu(int32_t year, uint32_t month, uint32_t day, uint8_t hour, uint8_t minute, uint8_t sec,
                              uint64_t usec) {
  return Timestamp{!terrier::util::TimeConvertor::TimestampFromHMSu(year, month, day, hour, minute, sec, usec)};
}

}  // namespace terrier::execution::sql
