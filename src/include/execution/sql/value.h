#pragma once

#include <sstream>
#include <string>
#include "date/date.h"
#include "execution/exec/execution_context.h"
#include "execution/util/common.h"
#include "execution/util/macros.h"
#include "execution/util/math_util.h"
#include "type/type_id.h"

namespace terrier::execution::sql {

/**
 * A generic base catch-all SQL value
 */
struct Val {
  /**
   * Whether the value is null
   */
  bool is_null;

  /**
   * Constructs a generic value
   * @param is_null whether the value is null
   */
  explicit Val(bool is_null = false) noexcept : is_null(is_null) {}
};

/**
 * A SQL boolean value
 */
struct BoolVal : public Val {
  /**
   * raw boolean value
   */
  bool val;

  /**
   * Non-null constructor
   * @param val value of the boolean
   */
  explicit BoolVal(bool val) noexcept : Val(false), val(val) {}

  /**
   * Convert this SQL boolean into a primitive boolean. Thanks to SQL's
   * three-valued logic, we implement the following truth table:
   *
   *   Value | NULL? | Output
   * +-------+-------+--------+
   * | false | false | false  |
   * | false | true  | false  |
   * | true  | false | true   |
   * | true  | true  | false  |
   * +-------+-------+--------+
   *
   * @return converted value
   */
  bool ForceTruth() const noexcept { return !is_null && val; }

  /**
   * @return a NULL bool value
   */
  static BoolVal Null() {
    BoolVal val(false);
    val.is_null = true;
    return val;
  }
};

/**
 * An integral SQL value
 */
struct Integer : public Val {
  /**
   * raw integer value
   */
  i64 val;

  /**
   * Non-Null constructor
   * @param val raw int value
   */
  explicit Integer(i64 val) noexcept : Integer(false, val) {}

  /**
   * Generic constructor
   * @param null whether the value is NULL or not
   * @param val the raw int value
   */
  explicit Integer(bool null, i64 val) noexcept : Val(null), val(val) {}

  /**
   * Create a NULL integer
   */
  static Integer Null() {
    Integer val(0);
    val.is_null = true;
    return val;
  }
};

/**
 * Real
 */
struct Real : public Val {
  /**
   * raw double value
   */
  double val;

  /**
   * Non-null float constructor
   * @param val value of the real
   */
  explicit Real(float val) noexcept : Val(false), val(val) {}

  /**
   * Non-null double constructor
   * @param val value of the double
   */
  explicit Real(double val) noexcept : Val(false), val(val) {}

  /**
   * Generic constructor
   * @param null whether the value is NULL or not
   * @param val the raw float value
   */
  explicit Real(bool null, float val) noexcept : Val(null), val(val) {}

  /**
   * Generic constructor
   * @param null whether the value is NULL or not
   * @param val the raw double value
   */
  explicit Real(bool null, double val) noexcept : Val(null), val(val) {}

  /**
   * @return a NULL real value
   */
  static Real Null() {
    Real real(0.0);
    real.is_null = true;
    return real;
  }
};

/**
 * A decimal SQL value
 */
struct Decimal : public Val {
  // TODO(Amadou): Check with Prashant to be sure of the meaning of val
  /**
   * bit representaion
   */
  u64 val;
  /**
   * Precision of the decimal
   */
  u32 precision;
  /**
   * Scale of the decimal
   */
  u32 scale;

  /**
   * Constructor
   * @param val bit representation
   * @param precision precision of the decimal
   * @param scale scale of the decimal
   */
  Decimal(u64 val, u32 precision, u32 scale) noexcept : Val(false), val(val), precision(precision), scale(scale) {}

  /**
   * @return a NULL decimal value
   */
  static Decimal Null() {
    Decimal val(0, 0, 0);
    val.is_null = true;
    return val;
  }
};

/**
 * A SQL string
 * TODO(Amadou): Check if the object's layout is optimal
 */
struct StringVal : public Val {
  /**
   * Maximum string length
   */
  static constexpr std::size_t kMaxStingLen = 1 * GB;

  /**
   * Padding for inlining
   */
  char prefix_[sizeof(uint64_t) - sizeof(bool)];

  /**
   * Raw string
   */
  const char *ptr;

  /**
   * String length
   */
  u32 len;

  /**
   * Create a string value (i.e., a view) over the given potentially non-null
   * terminated byte sequence.
   * @param str The byte sequence.
   * @param len The length of the sequence.
   */
  StringVal(const char *str, u32 len) noexcept : Val(str == nullptr), len(len) {
    if (!is_null) {
      if (len <= InlineThreshold()) {
        std::memcpy(prefix_, str, len);
      } else {
        ptr = str;
      }
    }
  }

  /**
   * Create a string value (i.e., view) over the C-style null-terminated string.
   * Note that no copy is made.
   * @param str The C-string.
   */
  explicit StringVal(const char *str) noexcept : StringVal(str, u32(strlen(str))) {}

  /**
   * Compare if this (potentially nullable) string value is equivalent to
   * another string value, taking NULLness into account.
   * @param that The string value to compare with.
   * @return True if equivalent; false otherwise.
   */
  bool operator==(const StringVal &that) const {
    if (is_null != that.is_null) {
      return false;
    }
    if (is_null) {
      return true;
    }
    if (len != that.len) {
      return false;
    }
    if (len <= InlineThreshold()) {
      return memcmp(prefix_, that.prefix_, len) == 0;
    }
    return ptr == that.ptr || memcmp(ptr, that.ptr, len) == 0;
  }

  /**
   * Is this string not equivalent to another?
   * @param that The string value to compare with.
   * @return True if not equivalent; false otherwise.
   */
  bool operator!=(const StringVal &that) const { return !(*this == that); }

  /**
   * Create a NULL varchar/string
   */
  static StringVal Null() { return StringVal(static_cast<char *>(nullptr), 0); }

  /**
   * @return the raw content
   */
  const char *Content() const {
    if (len <= InlineThreshold()) {
      return prefix_;
    }
    return ptr;
  }

  /**
   * Preallocate a StringVal whose content will be filled afterwards.
   * @param result what to allocate
   * @param memory allocator to use
   * @param len length of the string
   * @return a buffer that can be filled
   */
  static char *PreAllocate(StringVal *result, exec::ExecutionContext::StringAllocator *memory, uint32_t len) {
    // Inlined
    if (len <= InlineThreshold()) {
      *result = StringVal(len);
      return result->prefix_;
    }
    // Out of line
    if (TPL_UNLIKELY(len > kMaxStingLen)) {
      return nullptr;
    }
    auto *ptr = memory->Allocate(len);
    *result = StringVal(ptr, len);
    return ptr;
  }

  /**
   * @return threshold for inlining.
   */
  static uint32_t InlineThreshold() { return storage::VarlenEntry::InlineThreshold(); }

 private:
  // Used to pre allocated inlined strings
  explicit StringVal(uint32_t len) : Val(false), len(len) {}
};

/**
 * Date
 */
struct Date : public Val {
  /**
   * Date value
   * Can be represented by an int32 (for the storage layer), or by a year-month-day struct.
   */
  union {
    date::year_month_day ymd;
    u32 int_val;
  };

  /**
   * Constructor
   * @param date date value
   */
  explicit Date(u32 date) noexcept : Val(false), int_val{date} {}

  /**
   * Constructor
   * @param date date value from a TransientValue
   */
  explicit Date(type::date_t date) noexcept : Val(false), int_val{!date} {}

  /**
   * Constructor
   * @param year year value
   * @param month month value
   * @param day day value
   */
  Date(date::year year, date::month month, date::day day) noexcept : Val(false), ymd{year, month, day} {}

  /**
   * Constructor
   * @param year year value
   * @param month month value
   * @param day day value
   */
  Date(i16 year, u8 month, u8 day) noexcept : Val(false), ymd{date::year(year) / date::month(month) / date::day(day)} {}

  /**
   * @return a NULL Date.
   */
  static Date Null() {
    Date date(0);
    date.is_null = true;
    return date;
  }
};

/**
 * Timestamp
 */
struct Timestamp : public Val {
  /**
   * Time value
   */
  timespec time;

  /**
   * Constructor
   * @param time time value
   */
  explicit Timestamp(timespec time) noexcept : Val(false), time(time) {}

  /**
   * @return a NULL Timestamp
   */
  static Timestamp Null() {
    Timestamp timestamp({0, 0});
    timestamp.is_null = true;
    return timestamp;
  }
};

/**
 * Utility functions for sql values
 */
struct ValUtil {
  /**
   * @param type a terrier type
   * @return the size of the corresponding sql type
   */
  static u32 GetSqlSize(type::TypeId type) {
    switch (type) {
      case type::TypeId::TINYINT:
      case type::TypeId::SMALLINT:
      case type::TypeId::INTEGER:
      case type::TypeId::BIGINT:
        return static_cast<u32>(util::MathUtil::AlignTo(sizeof(Integer), 8));
      case type::TypeId::BOOLEAN:
        return static_cast<u32>(util::MathUtil::AlignTo(sizeof(BoolVal), 8));
      case type::TypeId::DATE:
      case type::TypeId::TIMESTAMP:
        return static_cast<u32>(util::MathUtil::AlignTo(sizeof(Date), 8));
      case type::TypeId::DECIMAL:
        // TODO(Amadou): We only support reals for now. Switch to Decima once it's implemented
        return static_cast<u32>(util::MathUtil::AlignTo(sizeof(Real), 8));
      case type::TypeId::VARCHAR:
      case type::TypeId::VARBINARY:
        return static_cast<u32>(util::MathUtil::AlignTo(sizeof(StringVal), 8));
      default:
        return 0;
    }
  }

  /**
   * Convert a date to a string
   * @param date date to convert
   * @return the date in the yyyy-mm-dd format
   */
  static std::string DateToString(const Date &date) {
    std::stringstream ss;
    ss << date.ymd;
    return ss.str();
  }

  /**
   * Return the year part of the date
   */
  static i16 ExtractYear(const Date &date) { return i16(static_cast<int>(date.ymd.year())); }

  /**
   * Return the month part of the date
   */
  static u8 ExtractMonth(const Date &date) { return u8(static_cast<unsigned>(date.ymd.month())); }

  /**
   * Return the day part of the date
   */
  static u8 ExtractDay(const Date &date) { return u8(static_cast<unsigned>(date.ymd.day())); }

  /**
   * Construct a date object from a string
   * @param str string representation of the date
   * @return the constructed date object
   */
  static Date StringToDate(const std::string &str) {
    std::stringstream ss(str);
    i16 year, month, day;
    // Token to dismiss the '-' character
    // Am using the i16 type to prevent the string stream from returning ascii codes.
    char tok;
    ss >> year >> tok;
    ss >> month >> tok;
    ss >> day;
    return Date(i16(year), u8(month), u8(day));
  }
};

}  // namespace terrier::execution::sql
