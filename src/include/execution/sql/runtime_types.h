#pragma once

#include <algorithm>
#include <string>
#include <utility>

#include "execution/util/string_heap.h"
#include "storage/storage_defs.h"

namespace noisepage::execution::sql {

class Timestamp;

static constexpr int64_t K_MICRO_SECONDS_PER_SECOND = 1000 * 1000;
static constexpr int64_t K_MICRO_SECONDS_PER_MINUTE = 60UL * K_MICRO_SECONDS_PER_SECOND;
static constexpr int64_t K_MICRO_SECONDS_PER_HOUR = 60UL * K_MICRO_SECONDS_PER_MINUTE;
static constexpr int64_t K_MICRO_SECONDS_PER_DAY = 24UL * K_MICRO_SECONDS_PER_HOUR;

/** A SQL date. */
class EXPORT Date {
 public:
  /**
   * The internal representation of a SQL date.
   */
  using NativeType = int32_t;

  /**
   * Empty constructor.
   */
  Date() = default;

  /**
   * @return True if this is a valid date instance; false otherwise.
   */
  bool IsValid() const;

  /**
   * @return A string representation of this date in the form "YYYY-MM-MM".
   */
  std::string ToString() const;

  /**
   * @return The year of this date.
   */
  int32_t ExtractYear() const;

  /**
   * @return The month of this date.
   */
  int32_t ExtractMonth() const;

  /**
   * @return The day of this date.
   */
  int32_t ExtractDay() const;

  /**
   * Convert this date object into its year, month, and day parts.
   * @param[out] year The year corresponding to this date.
   * @param[out] month The month corresponding to this date.
   * @param[out] day The day corresponding to this date.
   */
  void ExtractComponents(int32_t *year, int32_t *month, int32_t *day);

  /**
   * Convert this date instance into a timestamp instance.
   * @return The timestamp instance representing this date.
   */
  Timestamp ConvertToTimestamp() const;

  /**
   * Compute the hash value of this date instance.
   * @param seed The value to seed the hash with.
   * @return The hash value for this date instance.
   */
  hash_t Hash(const hash_t seed) const { return common::HashUtil::HashCrc(value_, seed); }

  /**
   * @return The hash value of this date instance.
   */
  hash_t Hash() const { return Hash(0); }

  /**
   * @return True if this date equals @em that date; false otherwise.
   */
  bool operator==(const Date &that) const { return value_ == that.value_; }

  /**
   * @return True if this date is not equal to @em that date; false otherwise.
   */
  bool operator!=(const Date &that) const { return value_ != that.value_; }

  /**
   * @return True if this data occurs before @em that date; false otherwise.
   */
  bool operator<(const Date &that) const { return value_ < that.value_; }

  /**
   * @return True if this data occurs before or is the same as @em that date; false otherwise.
   */
  bool operator<=(const Date &that) const { return value_ <= that.value_; }

  /**
   * @return True if this date occurs after @em that date; false otherwise.
   */
  bool operator>(const Date &that) const { return value_ > that.value_; }

  /**
   * @return True if this date occurs after or is equal to @em that date; false otherwise.
   */
  bool operator>=(const Date &that) const { return value_ >= that.value_; }

  /** @return The native representation of the date (julian microseconds). */
  Date::NativeType ToNative() const;

  /**
   * Construct a Date with the specified native representation.
   * @param val The native representation of a date.
   * @return The constructed Date.
   */
  static Date FromNative(Date::NativeType val);

  /**
   * Convert a C-style string of the form "YYYY-MM-DD" into a date instance. Will attempt to convert
   * the first date-like object it sees, skipping any leading whitespace.
   * @param str The string to convert.
   * @param len The length of the string.
   * @return The constructed date. May be invalid.
   */
  static Date FromString(const char *str, std::size_t len);

  /**
   * Convert a string of the form "YYYY-MM-DD" into a date instance. Will attempt to convert the
   * first date-like object it sees, skipping any leading whitespace.
   * @param str The string to convert.
   * @return The constructed Date. May be invalid.
   */
  static Date FromString(std::string_view str) { return FromString(str.data(), str.size()); }

  /**
   * Create a Date instance from a specified year, month, and day.
   * @param year The year of the date.
   * @param month The month of the date.
   * @param day The day of the date.
   * @return The constructed date. May be invalid.
   */
  static Date FromYMD(int32_t year, int32_t month, int32_t day);

  /**
   * Is the date corresponding to the given year, month, and day a valid date?
   * @param year The year of the date.
   * @param month The month of the date.
   * @param day The day of the date.
   * @return True if valid date.
   */
  static bool IsValidDate(int32_t year, int32_t month, int32_t day);

 private:
  friend class Timestamp;
  friend struct DateVal;
  friend class PostgresPacketWriter;

  // Private constructor to force static factories.
  explicit Date(NativeType value) : value_(value) {}

 private:
  // Date value
  NativeType value_;
};

//===----------------------------------------------------------------------===//
//
// Timestamps
//
//===----------------------------------------------------------------------===//

/**
 * A SQL timestamp.
 */
class EXPORT Timestamp {
 public:
  /** The internal representation of a SQL timestamp. */
  using NativeType = uint64_t;

  /**
   * Empty constructor.
   */
  Timestamp() = default;

  /**
   * @return The year of this timestamp.
   */
  int32_t ExtractYear() const;

  /**
   * @return The month of this timestamp.
   */
  int32_t ExtractMonth() const;

  /**
   * @return The day of this timestamp.
   */
  int32_t ExtractDay() const;

  /**
   * @return The year of this timestamp.
   */
  int32_t ExtractHour() const;

  /**
   * @return The month of this timestamp.
   */
  int32_t ExtractMinute() const;

  /**
   * @return The day of this timestamp.
   */
  int32_t ExtractSecond() const;

  /**
   * @return The milliseconds of this timestamp.
   */
  int32_t ExtractMillis() const;

  /**
   * @return The microseconds of this timestamp.
   */
  int32_t ExtractMicros() const;

  /**
   * @return The day-of-the-week (0-6 Sun-Sat) this timestamp falls on.
   */
  int32_t ExtractDayOfWeek() const;

  /**
   * @return THe day-of-the-year this timestamp falls on.
   */
  int32_t ExtractDayOfYear() const;

  /**
   * Extract all components of this timestamp
   * @param[out] year The year corresponding to this date.
   * @param[out] month The month corresponding to this date.
   * @param[out] day The day corresponding to this date.
   * @param[out] hour The hour corresponding to this date.
   * @param[out] min The minute corresponding to this date.
   * @param[out] sec The second corresponding to this date.
   * @param[out] millisec The millisecond corresponding to this date.
   * @param[out] microsec The millisecond corresponding to this date.
   */
  void ExtractComponents(int32_t *year, int32_t *month, int32_t *day, int32_t *hour, int32_t *min, int32_t *sec,
                         int32_t *millisec, int32_t *microsec) const;

  /**
   * Convert this timestamp instance into a date instance.
   * @return The date instance representing this timestamp.
   */
  Date ConvertToDate() const;

  /**
   * Compute the hash value of this timestamp instance.
   * @param seed The value to seed the hash with.
   * @return The hash value for this timestamp instance.
   */
  hash_t Hash(const hash_t seed) const { return common::HashUtil::HashCrc(value_, seed); }

  /**
   * @return The hash value of this timestamp instance.
   */
  hash_t Hash() const { return Hash(0); }

  /**
   * @return True if this timestamp equals @em that timestamp; false otherwise.
   */
  bool operator==(const Timestamp &that) const { return value_ == that.value_; }

  /**
   * @return True if this timestamp is not equal to @em that timestamp; false otherwise.
   */
  bool operator!=(const Timestamp &that) const { return value_ != that.value_; }

  /**
   * @return True if this data occurs before @em that timestamp; false otherwise.
   */
  bool operator<(const Timestamp &that) const { return value_ < that.value_; }

  /**
   * @return True if this data occurs before or is the same as @em that timestamp; false otherwise.
   */
  bool operator<=(const Timestamp &that) const { return value_ <= that.value_; }

  /**
   * @return True if this timestamp occurs after @em that timestamp; false otherwise.
   */
  bool operator>(const Timestamp &that) const { return value_ > that.value_; }

  /**
   * @return True if this timestamp occurs after or is equal to @em that timestamp; false otherwise.
   */
  bool operator>=(const Timestamp &that) const { return value_ >= that.value_; }

  /** @return The native representation of the timestamp. */
  Timestamp::NativeType ToNative() const;

  /**
   * Construct a Timestamp with the specified native representation.
   * @param val The native representation of a timestamp.
   * @return The constructed timestamp.
   */
  static Timestamp FromNative(Timestamp::NativeType val);

  /**
   * @return A string representation of timestamp in the form "YYYY-MM-DD HH:MM:SS.ZZZ"
   */
  std::string ToString() const;

  /**
   * Convert a C-style string of the form "YYYY-MM-DD HH::MM::SS" into a timestamp. Will attempt to
   * convert the first timestamp-like object it sees, skipping any leading whitespace.
   * @param str The string to convert.
   * @param len The length of the string.
   * @return The constructed Timestamp. May be invalid.
   */
  static Timestamp FromString(const char *str, std::size_t len);

  /**
   * Convert a string of the form "YYYY-MM-DD HH::MM::SS" into a timestamp. Will attempt to convert
   * the first timestamp-like object it sees, skipping any leading whitespace.
   * @param str The string to convert.
   * @return The constructed Timestamp. May be invalid.
   */
  static Timestamp FromString(std::string_view str) { return FromString(str.data(), str.size()); }

  /**
   * Instantiate a timestamp with the specified number of microseconds in Julian time.
   * @param usec The number of microseconds in Julian time.
   * @return The constructed timestamp.
   */
  static Timestamp FromMicroseconds(uint64_t usec) { return Timestamp(usec); }

  /**
   * Given time components parse the timezone and construct an adjusted TPL timestamp. If any
   * component is invalid, the bool result value will be false.
   * @param c The starting character, which is a '+' or '-'
   * @param year The year.
   * @param month The month.
   * @param day The day.
   * @param hour The hour.
   * @param min The minute.
   * @param sec The second.
   * @param milli The millisecond.
   * @param micro The microsecond.
   * @param ptr Start of timestamp string.
   * @param limit End of timestamp string.
   * @return The constructed timestamp if valid.
   */
  static Timestamp AdjustTimeZone(char c, int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min,
                                  int32_t sec, int32_t milli, int32_t micro, const char *ptr, const char *limit);

  /**
   * Given year, month, day, hour, minute, second components construct a TPL timestamp. If any
   * component is invalid, the bool result value will be false.
   * @param year The year.
   * @param month The month.
   * @param day The day.
   * @param hour The hour.
   * @param min The minute.
   * @param sec The second.
   * @return The constructed timestamp if valid.
   */
  static Timestamp FromYMDHMS(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min, int32_t sec);

  /**
   * Given year, month, day, hour, minute, second, ms, and us components construct a TPL timestamp. If any
   * component is invalid, the bool result value will be false.
   * @param year The year.
   * @param month The month.
   * @param day The day.
   * @param hour The hour.
   * @param min The minute.
   * @param sec The second.
   * @param milli The millisecond.
   * @param micro The microsecond.
   * @return The constructed timestamp if valid.
   */
  static Timestamp FromYMDHMSMU(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min, int32_t sec,
                                int32_t milli, int32_t micro);

 private:
  friend class Date;
  friend struct TimestampVal;

  explicit Timestamp(NativeType value) : value_(value) {}

 private:
  // Timestamp value -- the native type denotes the microseconds with respect to Julian time
  NativeType value_;
};

//===----------------------------------------------------------------------===//
//
// Fixed point decimals
//
//===----------------------------------------------------------------------===//

/**
 * A generic fixed point decimal value. This only serves as a storage container for decimals of various sizes.
 * Operations on decimals require a precision and scale.
 *
 * @tparam T The underlying native data type sufficiently large to store decimals of a pre-determined scale.
 */
template <typename T>
class EXPORT Decimal {
 public:
  /**
   * Convert an input string into a decimal representation.
   * @param input       The input string to convert.
   *                    If the input string has more digits than the specified precision, the value is rounded up.
   * @param precision   Number of digits after the decimal point.
   *                    The precision must be <= 38.
   *
   * TODO(WAN): This is basically a constructor. Why isn't it a constructor?
   */
  void RoundUpAndSet(std::string input, uint32_t precision);

  /** Underlying native data type. */
  using NativeType = T;

  /**
   * Create a decimal value using the given raw underlying encoded value.
   * @param value The value to set this decimal to.
   */
  explicit Decimal(const T &value) : value_(value) {}

  /**
   * Empty constructor.
   * TODO(WAN): Why do we have an empty constructor? We've been reasonably good about keeping constructors private.
   */
  Decimal() = default;

  /**
   * @return The raw underlying encoded decimal value.
   */
  operator T() const { return value_; }  // NOLINT

  /**
   * Compute the hash value of this decimal instance.
   * @param seed The value to seed the hash with.
   * @return The hash value for this decimal instance.
   */
  hash_t Hash(const hash_t seed) const {
    // TODO(WAN): Find out where this function came from and properly attribute them. Also not convinced about this.
    uint128_t x = value_;
    const uint64_t k_mul = 0x9ddfea08eb382d69ULL;
    uint128_t low_mask = 0xFFFFFFFFFFFFFFFF;
    uint64_t a = ((x & low_mask) ^ (x >> 64)) * k_mul;
    a ^= (a >> 47);
    uint64_t b = ((x >> 64) ^ a) * k_mul;
    b ^= (b >> 47);
    b *= k_mul;
    return b;
  }

  /**
   * @return The hash value of this decimal instance.
   */
  hash_t Hash() const { return Hash(0); }

  /**
   * Add the encoded decimal value @em that to this decimal value.
   * @param that The value to add.
   * @return This decimal value.
   */
  const Decimal<T> &operator+=(const T &that) {
    value_ += that;
    return *this;
  }

  /**
   * Subtract the encoded decimal value @em that from this decimal value.
   * @param that The value to subtract.
   * @return This decimal value.
   */
  const Decimal<T> &operator-=(const T &that) {
    value_ -= that;
    return *this;
  }

  /**
   * Multiply the encoded decimal value @em that with this decimal value.
   * @param that The value to multiply by.
   * @return This decimal value.
   */
  const Decimal<T> &operator*=(const T &that) {
    value_ *= that;
    return *this;
  }

  /**
   * Divide this decimal value by the encoded decimal value @em that.
   * @param that The value to divide by.
   * @return This decimal value.
   */
  const Decimal<T> &operator/=(const T &that) {
    value_ /= that;
    return *this;
  }

  /**
   * Modulo divide this decimal value by the encoded decimal value @em that.
   * @param that The value to modulus by.
   * @return This decimal value.
   */
  const Decimal<T> &operator%=(const T &that) {
    value_ %= that;
    return *this;
  }

  /**
   * Get the underlying value of the decimal
   * @return This decimal value.
   *
   * TODO(WAN): Abstraction leak. Why?
   */
  T GetValue() const { return value_; }

  /**
   * Set the underlying value of the decimal
   * @param value The value to be set.
   *
   * TODO(WAN): Abstraction leak. Why?
   */
  void SetValue(T value) { value_ = value; }

  /** This function divides with a given decimal
   * Note we multiply with the 10^precision of the denominator and then
   * divide by the denominator
   * If we do not overflow, we use a 128 number division
   * If we already know the magic number of the 128 bit number then we use
   * magic number division
   * If we overflow, we need to use a multiword 256 bit division
   * If we already know the magic number of the 256 bit number then we use
   * magic number division
   * We retain the result in numerator precision, this is due to the simple
   * fact that if we were to retain it in denominator precision we need to
   * multiply with 10 ^ (2* denominator - numerator) which will require a
   * 256 x 256 bit multiplication with an overflow check in 512 bits
   * @param input the decimal to be divide with
   * @param denominator_precision Number of digits after decimal point.*/

  void SignedDivideWithDecimal(Decimal<T> input, uint32_t denominator_precision);

  /** This function divides with a given unsigned 128 bit number with
   * another 128 bit input. The given number here is stored inside the
   * decimal class and assumes that it is non negative.
   * If constant is a power of 2, we right shift
   * If we have the magic number, we would use it to do the division
   * Otherwise we do a normal division
   * @param constant divisor*/
  void UnsignedDivideConstant128Bit(uint128_t constant);

  /** This function divides a 256 bit unsigned number with
   * another 128 bit input. We use the magic number division to perform this.
   * This function assumes we have the magic number already present
   * @param dividend the dividend in an array of 128 bit numbers each
   * having 64 bits
   * @param constant divisor
   * @return result of division*/
  uint128_t UnsignedMagicDivideConstantNumerator256Bit(uint128_t dividend[4], uint128_t constant);

  /** Signed version of UnsignedDivideConstant128Bit
   * @param input divisor*/
  void SignedDivideWithConstant(int64_t input);

  /**
   * Multiply the current decimal with an unsigned decimal.
   * The precision of the result depends on the precision provided.
   *
   * @param unsigned_input  The input decimal to multiply against. Must be unsigned!
   * @param precision       The number of digits after the decimal point.
   *                        To obtain higher precision result, pass in the lower precision of the operands.
   *                        To obtain lower precision result, pass in the higher precision of the operands.
   */
  void MultiplyAndSet(const Decimal<T> &unsigned_input, uint32_t precision);

  /** Signed version of MultiplyAndSet
   * @param input the decimal to be multiplied with
   * @param lower_precision Number of digits after decimal point
   * of the decimal with lower precision. Note the result will be stored in
   * the higher precision value.*/
  void SignedMultiplyWithDecimal(Decimal<T> input, unsigned lower_precision);

  /** Signed version of MultiplyAndSet with a constant
   * @param input the constant to be multiplied with. */
  void SignedMultiplyWithConstant(int64_t input);

  /** This function converts an input decimal string to the
   * underlying decimal representation. We try to retain the
   * maximum number of digits possible after the decimal point
   * and set the precision accordingly.
   * @param input input string
   * @return precision for the underlying decimal*/
  int SetMaxmPrecision(std::string input);

 private:
  // The encoded decimal value
  T value_;

  /* This function performs the magic number division by a power of
   * 10 for 128 bit unsigned integers. It assumes that the underlying
   * decimal number is positive. This routine is used after multiplication
   * of decimals to get the correct result.
   * @param power - i in 10^i.
   * */
  void UnsignedDivideConstant128BitPowerOfTen(uint32_t power);
};

using Decimal32 = Decimal<int32_t>;
using Decimal64 = Decimal<int64_t>;
using Decimal128 = Decimal<int128_t>;

//===----------------------------------------------------------------------===//
//
// Variable-length values
//
//===----------------------------------------------------------------------===//

/**
 * A container for varlens.
 */
class EXPORT VarlenHeap {
 public:
  /**
   * Allocate memory from the heap whose contents will be filled in by the user BEFORE creating a varlen.
   * @param len The length of the varlen to allocate.
   * @return The character byte array.
   */
  char *PreAllocate(std::size_t len) { return heap_.Allocate(len); }

  /**
   * Allocate a varlen from this heap whose contents are the same as the input string.
   * @param str The string to copy into the heap.
   * @param len The length of the input string.
   * @return A varlen.
   */
  storage::VarlenEntry AddVarlen(const char *str, std::size_t len) {
    auto *content = heap_.AddString(std::string_view(str, len));
    return storage::VarlenEntry::Create(reinterpret_cast<byte *>(content), len, false);
  }

  /**
   * Allocate and return a varlen from this heap whose contents as the same as the input string.
   * @param string The string to copy into the heap.
   * @return A varlen.
   */
  storage::VarlenEntry AddVarlen(const std::string &string) { return AddVarlen(string.c_str(), string.length()); }

  /**
   * Add a copy of the given varlen into this heap.
   * @param other The varlen to copy into this heap.
   * @return A new varlen entry.
   */
  storage::VarlenEntry AddVarlen(const storage::VarlenEntry &other) {
    return AddVarlen(reinterpret_cast<const char *>(other.Content()), other.Size());
  }

  /**
   * Destroy all heap-allocated varlens.
   */
  void Destroy() { heap_.Destroy(); }

 private:
  // Internal heap of strings
  util::StringHeap heap_;
};

/**
 * Simple structure representing a blob.
 */
class EXPORT Blob {
 public:
  /**
   * Crete an empty blob reference.
   */
  Blob() = default;

  /**
   * Create a reference to an existing blob.
   * @param data The blob data.
   * @param size The size of the blob.
   */
  Blob(byte *data, std::size_t size) noexcept : data_(data), size_(size) {}

  /**
   * @return A const-view of the raw blob data.
   */
  const byte *GetData() const noexcept { return data_; }

  /**
   * @return The size of the blob in bytes.
   */
  std::size_t GetSize() const noexcept { return size_; }

  /**
   * Compare two strings. Returns:
   * < 0 if left < right
   *  0  if left == right
   * > 0 if left > right
   *
   * @param left The first blob.
   * @param right The second blob.
   * @return The appropriate signed value indicating comparison order.
   */
  static int32_t Compare(const Blob &left, const Blob &right) {
    const std::size_t min_len = std::min(left.GetSize(), right.GetSize());
    const int32_t result = min_len == 0 ? 0 : std::memcmp(left.GetData(), right.GetData(), left.GetSize());
    if (result != 0) {
      return result;
    }
    return left.GetSize() - right.GetSize();
  }

  /**
   * @return True if this blob is byte-for-byte equivalent to @em that blob; false otherwise.
   */
  bool operator==(const Blob &that) const noexcept {
    return size_ == that.size_ && std::memcmp(data_, that.data_, size_) == 0;
  }

  /**
   * @return True if this blob is byte-for-byte not-equal-to @em that blob; false otherwise.
   */
  bool operator!=(const Blob &that) const noexcept { return !(*this == that); }

  /**
   * @return True if this blob is byte-for-byte less-than @em that blob; false otherwise.
   */
  bool operator<(const Blob &that) const noexcept { return Compare(*this, that) < 0; }

  /**
   * @return True if this blob is byte-for-byte less-than-or-equal-to @em that blob; false
   *         otherwise.
   */
  bool operator<=(const Blob &that) const noexcept { return Compare(*this, that) <= 0; }

  /**
   * @return True if this blob is byte-for-byte greater than @em that blob; false otherwise.
   */
  bool operator>(const Blob &that) const noexcept { return Compare(*this, that) > 0; }

  /**
   * @return True if this blob is byte-for-byte greater-than-or-equal-to @em that blob; false
   *         otherwise.
   */
  bool operator>=(const Blob &that) const noexcept { return Compare(*this, that) >= 0; }

 private:
  // Root
  byte *data_{nullptr};

  // Length
  std::size_t size_{0};
};

/** Converts the provided date into a timestamp. */
inline Timestamp Date::ConvertToTimestamp() const { return Timestamp(value_ * K_MICRO_SECONDS_PER_DAY); }
/** Converts the provided timestamp into a date. */
inline Date Timestamp::ConvertToDate() const { return Date(value_ / K_MICRO_SECONDS_PER_DAY); }

// TODO(WAN): These Nlz's are not runtime type functions. Dump them somewhere else.

/* Calculate the Number of leading zeros for a 128 bit unsigned int*/
int Nlz128(uint128_t x);
/* Calculate the Number of leading zeros for a 128 bit unsigned int faster
 * This function assumes u would not be 0*/
inline int Nlz128Fast(uint128_t u) {
  uint64_t hi = u >> 64;
  uint64_t lo = u;
  int retval[2] = {__builtin_clzll(hi), __builtin_clzll(lo) + 64};
  int idx = static_cast<int>(hi == 0);
  return retval[idx];
}

}  // namespace noisepage::execution::sql
