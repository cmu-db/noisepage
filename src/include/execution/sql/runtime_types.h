#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <string>

#include "common/strong_typedef.h"
#include "execution/util/execution_common.h"
#include "execution/util/hash.h"

namespace terrier::execution::sql {

class Timestamp;

/** Number of milliseconds in a day. */
static constexpr uint64_t K_MS_PER_DAY = 24 * 60 * 60 * 1000;
/** Number of microseconds in a day. */
static constexpr uint64_t K_US_PER_DAY = static_cast<uint64_t>(24) * 60 * 60 * 1000 * 1000;

/** A SQL date. */
class EXPORT Date {
 public:
  /** The internal representation of a SQL date. */
  using NativeType = uint32_t;

  /** Empty constructor. */
  Date() = default;

  /** @return True if this is a valid date instance; false otherwise. */
  bool IsValid() const noexcept;

  /** @return A string representation of this date in the form "YYYY-MM-MM". */
  std::string ToString() const;

  /** @return The year of this date. */
  int32_t ExtractYear() const noexcept;

  /** @return The month of this date. */
  uint32_t ExtractMonth() const noexcept;

  /** @return The day of this date. */
  uint32_t ExtractDay() const noexcept;

  /**
   * Convert this date object into its year, month, and day parts.
   * @param[out] year The year corresponding to this date.
   * @param[out] month The month corresponding to this date.
   * @param[out] day The day corresponding to this date.
   */
  void ExtractComponents(int32_t *year, uint32_t *month, uint32_t *day);

  /**
   * Convert this date instance into a timestamp instance.
   * @return The timestamp instance representing this date.
   */
  Timestamp ConvertToTimestamp() const noexcept;

  /**
   * Compute the hash value of this date instance.
   * @param seed The value to seed the hash with.
   * @return The hash value for this date instance.
   */
  hash_t Hash(const hash_t seed) const { return util::Hasher::Hash(value_, seed); }

  /**
   * @return The hash value of this date instance.
   */
  hash_t Hash() const { return Hash(0); }

  /**
   * @return True if this date equals @em that date; false otherwise.
   */
  bool operator==(const Date &that) const noexcept { return value_ == that.value_; }

  /**
   * @return True if this date is not equal to @em that date; false otherwise.
   */
  bool operator!=(const Date &that) const noexcept { return value_ != that.value_; }

  /**
   * @return True if this data occurs before @em that date; false otherwise.
   */
  bool operator<(const Date &that) const noexcept { return value_ < that.value_; }

  /**
   * @return True if this data occurs before or is the same as @em that date; false otherwise.
   */
  bool operator<=(const Date &that) const noexcept { return value_ <= that.value_; }

  /**
   * @return True if this date occurs after @em that date; false otherwise.
   */
  bool operator>(const Date &that) const noexcept { return value_ > that.value_; }

  /**
   * @return True if this date occurs after or is equal to @em that date; false otherwise.
   */
  bool operator>=(const Date &that) const noexcept { return value_ >= that.value_; }

  /** @return The native representation of the date. */
  Date::NativeType ToNative() const noexcept;

  /**
   * Construct a Date with the specified native representation.
   * @param val The native representation of a date.
   * @return The constructed Date.
   */
  static Date FromNative(Date::NativeType val);

  /**
   * Convert a string of the form "YYYY-MM-DD" into a date instance.
   * @param str The string to convert.
   * @return The constructed Date. May be invalid.
   */
  static Date FromString(const std::string &str);

  /**
   * Create a Date instance from a specified year, month, and day.
   * @param year The year of the date.
   * @param month The month of the date.
   * @param day The day of the date.
   * @return The constructed date. May be invalid.
   */
  static Date FromYMD(int32_t year, uint32_t month, uint32_t day);

  /**
   * Is the date corresponding to the given year, month, and day a valid date?
   * @param year The year of the date.
   * @param month The month of the date.
   * @param day The day of the date.
   * @return True if valid date.
   */
  static bool IsValidDate(int32_t year, uint32_t month, uint32_t day);

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
   * Compute the hash value of this timestamp instance.
   * @param seed The value to seed the hash with.
   * @return The hash value for this timestamp instance.
   */
  hash_t Hash(const hash_t seed) const { return util::Hasher::Hash(value_, seed); }

  /**
   * @return The hash value of this timestamp instance.
   */
  hash_t Hash() const { return Hash(0); }

  /**
   * @return A string representation of timestamp in the form "YYYY-MM-DD HH:MM:SS.ZZZ"
   */
  std::string ToString() const;

  /**
   * @return True if this timestamp equals @em that timestamp; false otherwise.
   */
  bool operator==(const Timestamp &that) const noexcept { return value_ == that.value_; }

  /**
   * @return True if this timestamp is not equal to @em that timestamp; false otherwise.
   */
  bool operator!=(const Timestamp &that) const noexcept { return value_ != that.value_; }

  /**
   * @return True if this data occurs before @em that timestamp; false otherwise.
   */
  bool operator<(const Timestamp &that) const noexcept { return value_ < that.value_; }

  /**
   * @return True if this data occurs before or is the same as @em that timestamp; false otherwise.
   */
  bool operator<=(const Timestamp &that) const noexcept { return value_ <= that.value_; }

  /**
   * @return True if this timestamp occurs after @em that timestamp; false otherwise.
   */
  bool operator>(const Timestamp &that) const noexcept { return value_ > that.value_; }

  /**
   * @return True if this timestamp occurs after or is equal to @em that timestamp; false otherwise.
   */
  bool operator>=(const Timestamp &that) const noexcept { return value_ >= that.value_; }

  /** @return The native representation of the timestamp. */
  Timestamp::NativeType ToNative() const noexcept;

  /**
   * Construct a Timestamp with the specified native representation.
   * @param val The native representation of a timestamp.
   * @return The constructed timestamp.
   */
  static Timestamp FromNative(Timestamp::NativeType val);

  /**
   * Convert a C-style string of the form "YYYY-MM-DD HH::MM::SS" into a timestamp. Will attempt to
   * convert the first timestamp-like object it sees, skipping any leading whitespace.
   * @param str The string to convert.
   * @param len The length of the string.
   * @return The constructed Timestamp. May be invalid.
   */
  static Timestamp FromString(const char *str, std::size_t len);

  /**
   * Instantiate a timestamp with the specified number of microseconds in Julian time.
   * @param usec The number of microseconds in Julian time.
   * @return The constructed timestamp.
   */
  static Timestamp FromMicroseconds(uint64_t usec) { return Timestamp(usec); }

  /** Instantiate a timestamp from calendar time. */
  static Timestamp FromHMSu(int32_t year, uint32_t month, uint32_t day, uint8_t hour, uint8_t minute, uint8_t sec,
                            uint64_t usec);

  /**
   * Convert a string of the form "YYYY-MM-DD HH::MM::SS" into a timestamp. Will attempt to convert
   * the first timestamp-like object it sees, skipping any leading whitespace.
   * @param str The string to convert.
   * @return The constructed Timestamp. May be invalid.
   */
  static Timestamp FromString(const std::string &str) { return FromString(str.c_str(), str.size()); }

 private:
  friend class Date;
  friend struct TimestampVal;

  explicit Timestamp(NativeType value) : value_(value) {}

 private:
  // Timestamp value
  NativeType value_;
};

/** Converts the provided date into a timestamp. */
inline Timestamp Date::ConvertToTimestamp() const noexcept { return Timestamp(value_ * K_US_PER_DAY); }

}  // namespace terrier::execution::sql
