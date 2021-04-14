#pragma once

#include <cstring>

#include "common/macros.h"
#include "execution/sql/runtime_types.h"
#include "execution/util/string_heap.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"

namespace noisepage::execution::sql {

/**
 * A generic base catch-all SQL value. Used to represent a NULL-able SQL value.
 */
struct Val {
  /** NULL indication flag. */
  bool is_null_;

  /**
   * Construct a value with the given NULL indication.
   * @param is_null Whether the SQL value is NULL.
   */
  explicit Val(bool is_null) noexcept : is_null_(is_null) {}
};

/**
 * A NULL-able SQL boolean value.
 */
struct BoolVal : public Val {
  /** The raw boolean value. */
  bool val_;

  /**
   * Construct a non-NULL boolean with the given value.
   * @param val The value of the boolean.
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
   * @return The primitive boolean value corresponding to this SQL Boolean.
   */
  bool ForceTruth() const noexcept { return !is_null_ && val_; }

  /**
   * @return A NULL boolean value.
   */
  static BoolVal Null() {
    BoolVal val(false);
    val.is_null_ = true;
    return val;
  }
};

/**
 * A NULL-able integral SQL value. Captures tinyint, smallint, integer and bigint.
 */
struct Integer : public Val {
  /** The raw integer value. */
  int64_t val_;

  /**
   * Construct a non-NULL integer with the given value.
   * @param val The value to set.
   */
  explicit Integer(int64_t val) noexcept : Val(false), val_(val) {}

  /**
   * @return A NULL integer.
   */
  static Integer Null() {
    Integer val(0);
    val.is_null_ = true;
    return val;
  }
};

/**
 * A NULL-able single- and double-precision floating point SQL value.
 */
struct Real : public Val {
  /** The raw double value. */
  double val_;

  /**
   * Construct a non-NULL real value from a 32-bit floating point value.
   * @param val The initial value.
   */
  explicit Real(float val) noexcept : Val(false), val_(val) {}

  /**
   * Construct a non-NULL real value from a 64-bit floating point value
   * @param val The initial value.
   */
  explicit Real(double val) noexcept : Val(false), val_(val) {}

  /**
   * @return A NULL Real value.
   */
  static Real Null() {
    Real real(0.0);
    real.is_null_ = true;
    return real;
  }
};

/**
 * A NULL-able fixed-point decimal SQL value.
 */
struct DecimalVal : public Val {
  /** The internal decimal representation. */
  Decimal64 val_;

  /**
   * Construct a non-NULL decimal value from the given 64-bit decimal value.
   * @param val The decimal value.
   */
  explicit DecimalVal(Decimal64 val) noexcept : Val(false), val_(val) {}

  /**
   * Construct a non-NULL decimal value from the given 64-bit decimal value.
   * @param val The raw decimal value.
   */
  explicit DecimalVal(Decimal64::NativeType val) noexcept : DecimalVal(Decimal64{val}) {}

  /**
   * @return A NULL decimal value.
   */
  static DecimalVal Null() {
    DecimalVal val(0);
    val.is_null_ = true;
    return val;
  }
};

/**
 * A NULL-able SQL string. These strings are always <b>views</b> onto externally managed memory.
 * They never own the memory they point to! They're a very thin wrapper around storage::VarlenEntry
 * used for string processing.
 */
struct StringVal : public Val {
  /** The VarlenEntry being wrapped. */
  storage::VarlenEntry val_;

  /**
   * Construct a non-NULL string from the given string value.
   * @param val The string.
   */
  explicit StringVal(storage::VarlenEntry val) noexcept : Val(false), val_(val) {}

  /**
   * Create a non-NULL string value (i.e., a view) over the given (potentially non-null terminated)
   * string.
   * @param str The character sequence.
   * @param len The length of the sequence.
   */
  StringVal(const char *str, uint32_t len) noexcept
      : Val(false),
        val_(len <= storage::VarlenEntry::InlineThreshold()
                 ? storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *>(str), len)
                 : storage::VarlenEntry::Create(reinterpret_cast<const byte *>(str), len, false)) {
    NOISEPAGE_ASSERT(str != nullptr, "String input cannot be NULL");
  }

  /**
   * @return std::string_view of StringVal's contents
   */
  std::string_view StringView() const {
    NOISEPAGE_ASSERT(
        !is_null_, "You should be doing a NULL check before attempting to generate a std::string_view of a StringVal.");
    return val_.StringView();
  }

  /**
   * @return Threshold for inlining.
   */
  static uint32_t InlineThreshold() { return storage::VarlenEntry::InlineThreshold(); }

  /**
   * Helper method to create a VarlenEntry from a StringVal.
   * @param str The input to be turned into a VarlenEntry.
   * @param own Whether the VarlenEntry should own the resulting string.
   * @return A VarlenEntry with the contents of the StringVal.
   */
  static storage::VarlenEntry CreateVarlen(const StringVal &str, bool own) {
    if (str.is_null_) {
      // TODO(WAN): matt points out that this is rather strange, but it currently exists in upstream/master. Fix later.
      return noisepage::storage::VarlenEntry::CreateInline(static_cast<const noisepage::byte *>(nullptr), 0);
    }
    if (str.GetLength() > storage::VarlenEntry::InlineThreshold()) {
      if (own) {
        // TODO(WAN): smarter allocation?
        byte *contents = common::AllocationUtil::AllocateAligned(str.GetLength());
        std::memcpy(contents, str.GetContent(), str.GetLength());
        return noisepage::storage::VarlenEntry::Create(contents, str.GetLength(), true);
      }
      return noisepage::storage::VarlenEntry::Create(reinterpret_cast<const noisepage::byte *>(str.GetContent()),
                                                     str.GetLength(), false);
    }
    return noisepage::storage::VarlenEntry::CreateInline(reinterpret_cast<const noisepage::byte *>(str.GetContent()),
                                                         str.GetLength());
  }

  /**
   * Create a non-NULL string value (i.e., view) over the C-style null-terminated string.
   * @param str The C-string.
   */
  explicit StringVal(const char *str) noexcept : StringVal(const_cast<char *>(str), strlen(str)) {}

  /**
   * Get the length of the string value.
   * @return The length of the string in bytes.
   */
  std::size_t GetLength() const noexcept { return val_.Size(); }

  /**
   * Return a pointer to the bytes underlying the string.
   * @return A pointer to the underlying content.
   */
  const char *GetContent() const noexcept { return reinterpret_cast<const char *>(val_.Content()); }

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
    return storage::VarlenEntry::CompareEqualOrNot<true>(val_, that.val_);
  }

  /**
   * Is this string not equivalent to another?
   * @param that The string value to compare with.
   * @return True if not equivalent; false otherwise.
   */
  bool operator!=(const StringVal &that) const { return !(*this == that); }

  /**
   * @return A NULL varchar/string.
   */
  static StringVal Null() {
    StringVal result("");
    result.is_null_ = true;
    return result;
  }
};
/**
 * A NULL-able SQL date value.
 */
struct DateVal : public Val {
  /** The internal date value. **/
  Date val_;

  /**
   * Construct a non-NULL date with the given date value.
   * @param val The date value.
   */
  explicit DateVal(Date val) noexcept : Val(false), val_(val) {}

  /**
   * Construct a non-NULL date with the given date value.
   * @param val The raw date value.
   */
  explicit DateVal(Date::NativeType val) noexcept : DateVal(Date{val}) {}

  /**
   * @return A NULL date.
   */
  static DateVal Null() {
    DateVal date(Date{});
    date.is_null_ = true;
    return date;
  }
};

/**
 * A NULL-able SQL timestamp value.
 */
struct TimestampVal : public Val {
  /** The internal timestamp value. */
  Timestamp val_;

  /**
   * Construct a non-NULL timestamp with the given value.
   * @param val The timestamp value.
   */
  explicit TimestampVal(Timestamp val) noexcept : Val(false), val_(val) {}

  /**
   * Construct a non-NULL timestamp with the given raw timestamp value
   * @param val The raw timestamp value.
   */
  explicit TimestampVal(Timestamp::NativeType val) noexcept : TimestampVal(Timestamp{val}) {}

  /**
   * @return A NULL timestamp.
   */
  static TimestampVal Null() {
    TimestampVal timestamp(Timestamp{0});
    timestamp.is_null_ = true;
    return timestamp;
  }
};

/**
 * Utility functions for sql values
 */
struct ValUtil {
  /**
   * @param type a noisepage type
   * @return the size of the corresponding sql type
   */
  static uint32_t GetSqlSize(type::TypeId type) {
    switch (type) {
      case type::TypeId::TINYINT:
      case type::TypeId::SMALLINT:
      case type::TypeId::INTEGER:
      case type::TypeId::BIGINT:
        return static_cast<uint32_t>(sizeof(Integer));
      case type::TypeId::BOOLEAN:
        return static_cast<uint32_t>(sizeof(BoolVal));
      case type::TypeId::DATE:
        return static_cast<uint32_t>(sizeof(DateVal));
      case type::TypeId::TIMESTAMP:
        return static_cast<uint32_t>(sizeof(TimestampVal));
      case type::TypeId::REAL:
        return static_cast<uint32_t>(sizeof(Real));
      case type::TypeId::DECIMAL:
        return static_cast<uint32_t>(sizeof(DecimalVal));
      case type::TypeId::VARCHAR:
      case type::TypeId::VARBINARY:
        return static_cast<uint32_t>(sizeof(StringVal));
      default:
        return 0;
    }
  }

  /**
   * @param type A noisepage type.
   * @return The alignment for this type in the execution engine.
   */
  static uint32_t GetSqlAlignment(type::TypeId type) {
    switch (type) {
      case type::TypeId::TINYINT:
      case type::TypeId::SMALLINT:
      case type::TypeId::INTEGER:
      case type::TypeId::BIGINT:
        return static_cast<uint32_t>(alignof(Integer));
      case type::TypeId::BOOLEAN:
        return static_cast<uint32_t>(alignof(BoolVal));
      case type::TypeId::DATE:
        return static_cast<uint32_t>(alignof(DateVal));
      case type::TypeId::TIMESTAMP:
        return static_cast<uint32_t>(alignof(TimestampVal));
      case type::TypeId::REAL:
        return static_cast<uint32_t>(alignof(Real));
      case type::TypeId::DECIMAL:
        return static_cast<uint32_t>(alignof(DecimalVal));
      case type::TypeId::VARCHAR:
      case type::TypeId::VARBINARY:
        return static_cast<uint32_t>(alignof(StringVal));
      default:
        return 0;
    }
  }
};

}  // namespace noisepage::execution::sql
