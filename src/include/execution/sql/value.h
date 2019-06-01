#pragma once

#include "execution/util/common.h"
#include "execution/util/macros.h"
#include "execution/util/math_util.h"
#include "type/type_id.h"

namespace tpl::sql {

#define AVG_PRECISION 3
#define AVG_SCALE 6

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

  /**
   * Perform addition
   * @param that value to add
   * @param[out] overflow whether an overflow occur
   * @return result of addition
   */
  Integer Add(const Integer &that, bool *overflow) const {
    i64 result;
    *overflow = __builtin_add_overflow(val, that.val, &result);
    return Integer(is_null || that.is_null, result);
  }

  /**
   * Perform subtraction
   * @param that value to subtract
   * @param[out] overflow whether an overflow occur
   * @return result of subtraction
   */
  Integer Sub(const Integer &that, bool *overflow) const {
    i64 result;
    *overflow = __builtin_sub_overflow(val, that.val, &result);
    return Integer(is_null || that.is_null, result);
  }

  /**
   * Perform multiplication
   * @param that value to multiply by
   * @param[out] overflow whether an overflow occur
   * @return result of multiplication
   */
  Integer Multiply(const Integer &that, bool *overflow) const {
    i64 result;
    *overflow = __builtin_mul_overflow(val, that.val, &result);
    return Integer(is_null || that.is_null, result);
  }

  /**
   * Perform division
   * @param that value to divide by
   * @return result of division
   */
  Integer Divide(const Integer &that) const {
    Integer result(0);
    if (that.val == 0) {
      result.is_null = true;
    } else {
      result.val = (val / that.val);
      result.is_null = false;
    }
    return result;
  }

  /**
   * Perform modulo
   * @param that value to mod by
   * @return result of modulo
   */
  Integer Modulo(const Integer &that) const {
    Integer result(0);
    if (that.val == 0) {
      result.is_null = true;
    } else {
      result.val = (val % that.val);
      result.is_null = false;
    }
    return result;
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
struct VarBuffer : public Val {
  /**
   * raw string
   */
  u8 *str;
  /**
   * length of the string
   */
  u32 len;

  /**
   * Constructor
   * @param str raw string
   * @param len length of the string
   */
  VarBuffer(u8 *str, u32 len) noexcept : Val(str == nullptr), str(str), len(len) {}

  /**
   * @return a NULL varchar/string
   */
  static VarBuffer Null() { return VarBuffer(nullptr, 0); }
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
        return static_cast<u32>(util::MathUtil::AlignTo(sizeof(VarBuffer), 8));
      default:
        return 0;
    }
  }
};

}  // namespace tpl::sql
