#pragma once

#include <sstream>
#include <string>
#include "common/macros.h"
#include "common/math_util.h"
#include "date/date.h"
#include "execution/exec/execution_context.h"
#include "execution/util/execution_common.h"
#include "type/type_id.h"

namespace terrier::execution::sql {

/**
 * A generic base catch-all SQL value
 */
struct Val {
  /**
   * Whether the value is null
   */
  bool is_null_;

  /**
   * Constructs a generic value
   * @param is_null whether the value is null
   */
  explicit Val(bool is_null = false) noexcept : is_null_(is_null) {}
};

/**
 * A SQL boolean value
 */
struct BoolVal : public Val {
  /**
   * raw boolean value
   */
  bool val_;

  /**
   * Non-null constructor
   * @param val value of the boolean
   */
  explicit BoolVal(bool val) noexcept : Val(false), val_(val) {}

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
  bool ForceTruth() const noexcept { return !is_null_ && val_; }

  /**
   * @return a NULL bool value
   */
  static BoolVal Null() {
    BoolVal val(false);
    val.is_null_ = true;
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
  int64_t val_;

  /**
   * Non-Null constructor
   * @param val raw int value
   */
  explicit Integer(int64_t val) noexcept : Integer(false, val) {}

  /**
   * Generic constructor
   * @param null whether the value is NULL or not
   * @param val the raw int value
   */
  explicit Integer(bool null, int64_t val) noexcept : Val(null), val_(val) {}

  /**
   * Create a NULL integer
   */
  static Integer Null() {
    Integer val(0);
    val.is_null_ = true;
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
  double val_;

  /**
   * Non-null float constructor
   * @param val value of the real
   */
  explicit Real(float val) noexcept : Val(false), val_(val) {}

  /**
   * Non-null double constructor
   * @param val value of the double
   */
  explicit Real(double val) noexcept : Val(false), val_(val) {}

  /**
   * Generic constructor
   * @param null whether the value is NULL or not
   * @param val the raw float value
   */
  explicit Real(bool null, float val) noexcept : Val(null), val_(val) {}

  /**
   * Generic constructor
   * @param null whether the value is NULL or not
   * @param val the raw double value
   */
  explicit Real(bool null, double val) noexcept : Val(null), val_(val) {}

  /**
   * @return a NULL real value
   */
  static Real Null() {
    Real real(0.0);
    real.is_null_ = true;
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
  uint64_t val_;
  /**
   * Precision of the decimal
   */
  uint32_t precision_;
  /**
   * Scale of the decimal
   */
  uint32_t scale_;

  /**
   * Constructor
   * @param val bit representation
   * @param precision precision of the decimal
   * @param scale scale of the decimal
   */
  Decimal(uint64_t val, uint32_t precision, uint32_t scale) noexcept
      : Val(false), val_(val), precision_(precision), scale_(scale) {}

  /**
   * @return a NULL decimal value
   */
  static Decimal Null() {
    Decimal val(0, 0, 0);
    val.is_null_ = true;
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
  static constexpr std::size_t K_MAX_STING_LEN = 1 * common::Constants::GB;

  /**
   * Padding for inlining
   */
  char prefix_[sizeof(uint64_t) - sizeof(bool)];

  /**
   * Raw string
   */
  const char *ptr_;

  /**
   * String length
   */
  uint32_t len_;

  /**
   * Create a string value (i.e., a view) over the given potentially non-null
   * terminated byte sequence.
   * @param str The byte sequence.
   * @param len The length of the sequence.
   */
  StringVal(const char *str, uint32_t len) noexcept : Val(str == nullptr), len_(len) {
    if (!is_null_) {
      if (len <= InlineThreshold()) {
        std::memcpy(prefix_, str, len);
      } else {
        ptr_ = str;
      }
    }
  }

  /**
   * Create a string value (i.e., view) over the C-style null-terminated string.
   * Note that no copy is made.
   * @param str The C-string.
   */
  explicit StringVal(const char *str) noexcept : StringVal(str, uint32_t(strlen(str))) {}

  /**
   * Compare if this (potentially nullable) string value is equivalent to
   * another string value, taking NULLness into account.
   * @param that The string value to compare with.
   * @return True if equivalent; false otherwise.
   */
  bool operator==(const StringVal &that) const {
    if (is_null_ != that.is_null_) {
      return false;
    }
    if (is_null_) {
      return true;
    }
    if (len_ != that.len_) {
      return false;
    }
    if (len_ <= InlineThreshold()) {
      return memcmp(prefix_, that.prefix_, len_) == 0;
    }
    return ptr_ == that.ptr_ || memcmp(ptr_, that.ptr_, len_) == 0;
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
    if (len_ <= InlineThreshold()) {
      return prefix_;
    }
    return ptr_;
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
    if (UNLIKELY(len > K_MAX_STING_LEN)) {
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
  explicit StringVal(uint32_t len) : Val(false), len_(len) {}
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
    date::year_month_day ymd_;
    uint32_t int_val_;
  };

  /**
   * Constructor
   * @param date date value
   */
  explicit Date(uint32_t date) noexcept : Val(false), int_val_{date} {}

  /**
   * Constructor
   * @param date date value from a TransientValue
   */
  explicit Date(type::date_t date) noexcept : Val(false), int_val_{!date} {}

  /**
   * Constructor
   * @param year year value
   * @param month month value
   * @param day day value
   */
  Date(date::year year, date::month month, date::day day) noexcept : Val(false), ymd_{year, month, day} {}

  /**
   * Constructor
   * @param year year value
   * @param month month value
   * @param day day value
   */
  Date(int16_t year, uint8_t month, uint8_t day) noexcept
      : Val(false), ymd_{date::year(year) / date::month(month) / date::day(day)} {}

  /**
   * @return a NULL Date.
   */
  static Date Null() {
    Date date(0);
    date.is_null_ = true;
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
  timespec time_;

  /**
   * Constructor
   * @param time time value
   */
  explicit Timestamp(timespec time) noexcept : Val(false), time_(time) {}

  /**
   * @return a NULL Timestamp
   */
  static Timestamp Null() {
    Timestamp timestamp({0, 0});
    timestamp.is_null_ = true;
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
  static uint32_t GetSqlSize(type::TypeId type) {
    switch (type) {
      case type::TypeId::TINYINT:
      case type::TypeId::SMALLINT:
      case type::TypeId::INTEGER:
      case type::TypeId::BIGINT:
        return static_cast<uint32_t>(common::MathUtil::AlignTo(sizeof(Integer), 8));
      case type::TypeId::BOOLEAN:
        return static_cast<uint32_t>(common::MathUtil::AlignTo(sizeof(BoolVal), 8));
      case type::TypeId::DATE:
      case type::TypeId::TIMESTAMP:
        return static_cast<uint32_t>(common::MathUtil::AlignTo(sizeof(Date), 8));
      case type::TypeId::DECIMAL:
        // TODO(Amadou): We only support reals for now. Switch to Decima once it's implemented
        return static_cast<uint32_t>(common::MathUtil::AlignTo(sizeof(Real), 8));
      case type::TypeId::VARCHAR:
      case type::TypeId::VARBINARY:
        return static_cast<uint32_t>(common::MathUtil::AlignTo(sizeof(StringVal), 8));
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
    ss << date.ymd_;
    return ss.str();
  }

  /**
   * Return the year part of the date
   */
  static int16_t ExtractYear(const Date &date) { return int16_t(static_cast<int>(date.ymd_.year())); }

  /**
   * Return the month part of the date
   */
  static uint8_t ExtractMonth(const Date &date) { return uint8_t(static_cast<unsigned>(date.ymd_.month())); }

  /**
   * Return the day part of the date
   */
  static uint8_t ExtractDay(const Date &date) { return uint8_t(static_cast<unsigned>(date.ymd_.day())); }

  /**
   * Construct a date object from a string
   * @param str string representation of the date
   * @return the constructed date object
   */
  static Date StringToDate(const std::string &str) {
    std::stringstream ss(str);
    int16_t year, month, day;
    // Token to dismiss the '-' character
    // Am using the int16_t type to prevent the string stream from returning ascii codes.
    char tok;
    ss >> year >> tok;
    ss >> month >> tok;
    ss >> day;
    return Date(int16_t(year), uint8_t(month), uint8_t(day));
  }
};

}  // namespace terrier::execution::sql
