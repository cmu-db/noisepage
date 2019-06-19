#pragma once

#include "execution/exec/execution_context.h"
#include "execution/util/common.h"
#include "execution/util/macros.h"
#include "execution/util/math_util.h"
#include "execution/exec/execution_context.h"
#include "type/type_id.h"

namespace tpl::sql {

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
 */
struct StringVal : public Val {
  /**
   * Maximum string length
   */
  static constexpr std::size_t kMaxStingLen = 1 * GB;

  /**
   * Raw string
   */
  char *ptr;

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
  StringVal(char *str, u32 len) noexcept
      : Val(str == nullptr), ptr(str), len(len) {}

  /**
   * Create a string value (i.e., view) over the C-style null-terminated string.
   * Note that no copy is made.
   * @param str The C-string.
   */
  explicit StringVal(const char *str) noexcept
      : StringVal(const_cast<char *>(str), u32(strlen(str))) {}

  /**
   * Create a new string using the given memory pool and length.
   * @param memory The memory pool to allocate this string's contents from
   * @param len The size of the string
   */
  StringVal(exec::ExecutionContext::StringAllocator *memory, std::size_t len)
      : ptr(nullptr), len(u32(len)) {
    if (TPL_UNLIKELY(len > kMaxStingLen)) {
      len = 0;
      is_null = true;
    } else {
      ptr = reinterpret_cast<char *>(memory->Allocate(len));
    }
  }

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
};

/**
 * Date
 */
struct Date : public Val {
  /**
   * Date value
   */
  i32 date_val;

  /**
   * Constructor
   * @param date date value
   */
  explicit Date(i32 date) noexcept : Val(false), date_val(date) {}

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
  static u32 GetSqlSize(terrier::type::TypeId type) {
    switch (type) {
      case terrier::type::TypeId::TINYINT:
      case terrier::type::TypeId::SMALLINT:
      case terrier::type::TypeId::INTEGER:
      case terrier::type::TypeId::BIGINT:
        return static_cast<u32>(util::MathUtil::AlignTo(sizeof(Integer), 8));
      case terrier::type::TypeId::BOOLEAN:
        return static_cast<u32>(util::MathUtil::AlignTo(sizeof(BoolVal), 8));
      case terrier::type::TypeId::DATE:
      case terrier::type::TypeId::TIMESTAMP:
        return static_cast<u32>(util::MathUtil::AlignTo(sizeof(Date), 8));
      case terrier::type::TypeId::DECIMAL:
        return static_cast<u32>(util::MathUtil::AlignTo(sizeof(Decimal), 8));
      case terrier::type::TypeId::VARCHAR:
      case terrier::type::TypeId::VARBINARY:
        return static_cast<u32>(util::MathUtil::AlignTo(sizeof(StringVal), 8));
      default:
        return 0;
    }
  }
};

}  // namespace tpl::sql
