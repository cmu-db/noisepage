#pragma once

#include "execution/util/common.h"
#include "execution/util/macros.h"

namespace tpl::sql {

#define AVG_PRECISION 3
#define AVG_SCALE 6

/// A generic base catch-all SQL value
struct Val {
  bool is_null;

  explicit Val(bool is_null = false) noexcept : is_null(is_null) {}
};

// ---------------------------------------------------------
// Boolean
// ---------------------------------------------------------

/// A SQL boolean value
struct BoolVal : public Val {
  bool val;

  explicit BoolVal(bool val) noexcept : Val(false), val(val) {}

  /// Convert this SQL boolean into a primitive boolean. Thanks to SQL's
  /// three-valued logic, we implement the following truth table:
  ///
  ///   Value | NULL? | Output
  /// +-------+-------+--------+
  /// | false | false | false  |
  /// | false | true  | false  |
  /// | true  | false | true   |
  /// | true  | true  | false  |
  /// +-------+-------+--------+
  ///
  bool ForceTruth() const noexcept { return !is_null && val; }

  /// Return a NULL boolean value
  static BoolVal Null() {
    BoolVal val(false);
    val.is_null = true;
    return val;
  }
};

// ---------------------------------------------------------
// Integer
// ---------------------------------------------------------

/// An integral SQL value
struct Integer : public Val {
  i64 val;

  explicit Integer(i64 val) noexcept : Val(false), val(val) {}

  /// Return a NULL integer value
  static Integer Null() {
    Integer val(0);
    val.is_null = true;
    return val;
  }

  /// dumb division for now
  Integer Divide(const Integer &denom) { return Integer(this->val / denom.val); }
};

// ---------------------------------------------------------
// Real
// ---------------------------------------------------------

struct Real : public Val {
  double val;

  explicit Real(float val) noexcept : Val(false), val(val) {}
  explicit Real(double val) noexcept : Val(false), val(val) {}

  static Real Null() {
    Real real(0.0);
    real.is_null = true;
    return real;
  }
};

// ---------------------------------------------------------
// Decimal
// ---------------------------------------------------------

/// A decimal SQL value
struct Decimal : public Val {
  u64 val;
  u32 precision;
  u32 scale;

  Decimal(u64 val, u32 precision, u32 scale) noexcept : Val(false), val(val), precision(precision), scale(scale) {}

  /// Return a NULL decimal value
  static Decimal Null() {
    Decimal val(0, 0, 0);
    val.is_null = true;
    return val;
  }
};

// ---------------------------------------------------------
// Strings
// ---------------------------------------------------------

/// A SQL string
struct VarBuffer : public Val {
  u8 *str;
  u32 len;

  VarBuffer(u8 *str, u32 len) noexcept : Val(str == nullptr), str(str), len(len) {}

  /// Return a NULL varchar/string
  static VarBuffer Null() { return VarBuffer(nullptr, 0); }
};

// ---------------------------------------------------------
// Date
// ---------------------------------------------------------

struct Date : public Val {
  i32 date_val;

  explicit Date(i32 date) noexcept : Val(false), date_val(date) {}

  static Date Null() {
    Date date(0);
    date.is_null = true;
    return date;
  }
};

// ---------------------------------------------------------
// Timestamp
// ---------------------------------------------------------

struct Timestamp : public Val {
  timespec time;

  explicit Timestamp(timespec time) noexcept : Val(false), time(time) {}

  static Timestamp Null() {
    Timestamp timestamp({0, 0});
    timestamp.is_null = true;
    return timestamp;
  }
};

}  // namespace tpl::sql
