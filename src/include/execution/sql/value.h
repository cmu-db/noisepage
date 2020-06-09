#pragma once

#include <sstream>
#include <string>
#include <string_view>

#include "common/macros.h"
#include "common/math_util.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/runtime_types.h"
#include "execution/util/execution_common.h"
#include "type/type_id.h"
#include "util/time_util.h"

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

  /**
   * @return a NULL SQL value
   */
  static Val Null() { return Val(true); }
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
  static constexpr std::size_t K_MAX_STRING_LEN = 1 * common::Constants::GB;

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
   * @return std::string_view of StringVal's contents
   */
  std::string_view StringView() const {
    TERRIER_ASSERT(!is_null_,
                   "You should be doing a NULL check before attempting to generate a std::string_view of a StringVal.");
    return std::string_view(Content(), len_);
  }

  /**
   * Helper method to turn a StringVal into a VarlenEntry.
   * @param str input to be turned into a VarlenEntry
   * @param own whether the varlen entry to own the string
   * @return VarlenEntry representing StringVal
   */
  static storage::VarlenEntry CreateVarlen(const StringVal &str, bool own) {
    if (str.is_null_) {
      return terrier::storage::VarlenEntry::CreateInline(static_cast<const terrier::byte *>(nullptr), 0);
    }
    if (str.len_ > storage::VarlenEntry::InlineThreshold()) {
      if (own) {
        byte *contents = common::AllocationUtil::AllocateAligned(str.len_);
        std::memcpy(contents, str.Content(), str.len_);
        return terrier::storage::VarlenEntry::Create(contents, str.len_, true);
      }
      return terrier::storage::VarlenEntry::Create(reinterpret_cast<const terrier::byte *>(str.Content()), str.len_,
                                                   false);
    }
    return terrier::storage::VarlenEntry::CreateInline(reinterpret_cast<const terrier::byte *>(str.Content()),
                                                       str.len_);
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
    if (UNLIKELY(len > K_MAX_STRING_LEN)) {
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
 * A NULL-able SQL date value.
 */
struct DateVal : public Val {
  /** The date value. */
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
  /** The timestamp value. */
  Timestamp val_;

  /**
   * Construct a non-NULL timestamp with the given value.
   * @param val The timestamp value.
   */
  explicit TimestampVal(Timestamp val) noexcept : Val(false), val_(val) {}

  /**
   * Construct a non-NULL timestamp with the given raw timestamp value.
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
        return static_cast<uint32_t>(common::MathUtil::AlignTo(sizeof(DateVal), 8));
      case type::TypeId::TIMESTAMP:
        return static_cast<uint32_t>(common::MathUtil::AlignTo(sizeof(TimestampVal), 8));
      case type::TypeId::DECIMAL:
        // TODO(Amadou): We only support reals for now. Switch to Decimal once it's implemented
        return static_cast<uint32_t>(common::MathUtil::AlignTo(sizeof(Real), 8));
      case type::TypeId::VARCHAR:
      case type::TypeId::VARBINARY:
        return static_cast<uint32_t>(common::MathUtil::AlignTo(sizeof(StringVal), 8));
      default:
        return 0;
    }
  }
};

}  // namespace terrier::execution::sql
