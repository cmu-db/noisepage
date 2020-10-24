#include "execution/sql/functions/date_time_functions.h"

namespace noisepage::execution::sql {

void DateTimeFunctions::Century(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null_) {
    *result = Integer::Null();
    return;
  }

  const auto year = time.val_.ExtractYear();
  if (year > 0) {
    *result = Integer((year + 99) / 100);
  } else {
    *result = Integer(-((99 - (year - 1)) / 100));
  }
}

void DateTimeFunctions::Decade(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null_) {
    *result = Integer::Null();
    return;
  }

  const auto year = time.val_.ExtractYear();
  if (year >= 0) {
    *result = Integer(year / 10);
  } else {
    *result = Integer(-((8 - (year - 1)) / 10));
  }
}

void DateTimeFunctions::Year(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null_) {
    *result = Integer::Null();
    return;
  }
  *result = Integer(time.val_.ExtractYear());
}

void DateTimeFunctions::Quarter(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null_) {
    *result = Integer::Null();
    return;
  }
  const auto month = time.val_.ExtractMonth();
  *result = Integer((month - 1) / 3 + 1);
}

void DateTimeFunctions::Month(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null_) {
    *result = Integer::Null();
    return;
  }
  *result = Integer(time.val_.ExtractMonth());
}

void DateTimeFunctions::Day(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null_) {
    *result = Integer::Null();
    return;
  }
  *result = Integer(time.val_.ExtractDay());
}

void DateTimeFunctions::DayOfWeek(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null_) {
    *result = Integer::Null();
    return;
  }
  *result = Integer(time.val_.ExtractDayOfWeek());
}

void DateTimeFunctions::DayOfYear(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null_) {
    *result = Integer::Null();
    return;
  }
  *result = Integer(time.val_.ExtractDayOfYear());
}

void DateTimeFunctions::Hour(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null_) {
    *result = Integer::Null();
    return;
  }
  *result = Integer(time.val_.ExtractHour());
}

void DateTimeFunctions::Minute(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null_) {
    *result = Integer::Null();
    return;
  }
  *result = Integer(time.val_.ExtractMinute());
}

void DateTimeFunctions::Second(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null_) {
    *result = Integer::Null();
    return;
  }
  *result = Integer(time.val_.ExtractSecond());
}

void DateTimeFunctions::Millisecond(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null_) {
    *result = Integer::Null();
    return;
  }
  *result = Integer(time.val_.ExtractMillis());
}

void DateTimeFunctions::Microseconds(Integer *result, const TimestampVal &time) noexcept {
  if (time.is_null_) {
    *result = Integer::Null();
    return;
  }
  *result = Integer(time.val_.ExtractMicros());
}

}  // namespace noisepage::execution::sql
