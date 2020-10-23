#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iosfwd>
#include <string>
#include <type_traits>
#include <utility>

#include "common/json_header.h"
#include "common/macros.h"

namespace noisepage::common {
/*
 * A strong typedef is like a typedef, except the compiler will enforce explicit
 * conversion for you.
 *
 * Usually, typedefs (or equivalent 'using' statement) are transparent to the
 * compiler. If you declare A and B to both be int, they are interchangeable.
 * This is not exactly ideal because then it becomes easy for you to do something
 * like this:
 *
 * // some definition
 * A foo(A a, B b);
 *
 * // invocation
 * (a = 42, b = 10)
 * foo(10, 42); // oops
 *
 * ... and the compiler will happily compile and run that code with no warning.
 *
 * With a strong typedef, you are required to explicitly convert these types,
 * turning our example into:
 *
 * A a(42);
 * B b(10);
 * foo(a, b);
 *
 * Now foo(b, a) would be a type mismatch.
 *
 * To extract the primitive integral type that backs the strong typedef out
 * of an instance, use the StrongTypeAlias::UnderlyingValue() member. For example:
 *
 * using A = StrongTypeAlias<struct SomeTag, int>
 *
 * A a(42)
 * assert(a.UnderlyingValue() == 42)
 *
 * This mechanism works with all integral types (as defined by std::is_integral).
 *
 * In order to use this macro, you need to use STRONG_TYPEDEF_HEADER in the .h file, then
 * include common/strong_typedef_body.h in the corresponding .cpp file and use
 * STRONG_TYPEDEF_BODY with the same arguements. Finally, you need to add an explicit instantation
 * of the template in common/strong_typedef.cpp.
 *
 */
#define STRONG_TYPEDEF_HEADER(name, underlying_type)                                            \
  namespace tags {                                                                              \
  struct name##_typedef_tag {};                                                                 \
  }                                                                                             \
  using name = ::noisepage::common::StrongTypeAlias<tags::name##_typedef_tag, underlying_type>; \
  namespace tags {                                                                              \
  void to_json(nlohmann::json &j, const name &c);   /* NOLINT */                                \
  void from_json(const nlohmann::json &j, name &c); /* NOLINT */                                \
  }

/**
 * A StrongTypeAlias is the underlying implementation of STRONG_TYPEDEF.
 *
 * Unless you know what you are doing, you shouldn't touch this class. Just use
 * the MACRO defined above
 * @tparam Tag a dummy class type to annotate the underlying type
 * @tparam IntType the underlying type
 */
template <class Tag, typename IntType>
class StrongTypeAlias {
  static_assert(std::is_integral<IntType>::value, "Only int types are defined for strong typedefs");

 public:
  StrongTypeAlias() = default;

  /**
   * Constructs a new StrongTypeAlias.
   * @param val const reference to the underlying type.
   */
  constexpr explicit StrongTypeAlias(const IntType &val) : val_(val) {}
  /**
   * Move constructs a new StrongTypeAlias.
   * @param val const reference to the underlying type.
   */
  constexpr explicit StrongTypeAlias(IntType &&val) : val_(std::move(val)) {}

  /**
   * @return the underlying value
   */
  constexpr IntType &UnderlyingValue() { return val_; }

  /**
   * @return the underlying value
   */
  constexpr const IntType &UnderlyingValue() const { return val_; }

  // TODO(Kyle): perhaps remove ability to static_cast to underlying value altogether.
  /**
   *
   * @return the underlying value
   */
  explicit operator IntType() const { return val_; }

  /**
   * Checks if this is equal to the other StrongTypeAlias.
   * @param rhs the other StrongTypeAlias to be compared.
   * @return true if the StrongTypeAliases are equal, false otherwise.
   */
  bool operator==(const StrongTypeAlias &rhs) const { return val_ == rhs.val_; }

  /**
   * Checks if this is not equal to the other StrongTypeAlias.
   * @param rhs the other StrongTypeAlias to be compared.
   * @return true if the StrongTypeAliases are not equal, false otherwise.
   */
  bool operator!=(const StrongTypeAlias &rhs) const { return val_ != rhs.val_; }

  /**
   * prefix-increment.
   * @return the value of the variable after the modification.
   */
  StrongTypeAlias &operator++() {
    ++val_;
    return *this;
  }

  /**
   * postfix-increment.
   * @return the value of the variable before the modification.
   */
  StrongTypeAlias operator++(int) { return StrongTypeAlias(val_++); }

  /**
   * addition.
   * @param operand another int type
   * @return sum of the underlying value and given operand
   */
  StrongTypeAlias operator+(const IntType &operand) const {
    return StrongTypeAlias(static_cast<IntType>(val_ + operand));
  }

  /**
   * addition and assignment
   * @param rhs another int type
   * @return self-reference after the rhs is added to the underlying value
   */
  StrongTypeAlias &operator+=(const IntType &rhs) {
    val_ += rhs;
    return *this;
  }

  /**
   * prefix-decrement.
   * @return the value of the variable after the modification.
   */
  StrongTypeAlias &operator--() {
    --val_;
    return *this;
  }

  /**
   * postfix-decrement.
   * @return the value of the variable before the modification.
   */
  StrongTypeAlias operator--(int) { return StrongTypeAlias(val_--); }

  /**
   * subtraction
   * @param operand another int type
   * @return difference between the underlying value and given operand
   */
  StrongTypeAlias operator-(const IntType &operand) const { return StrongTypeAlias(val_ - operand); }

  /**
   * subtraction and assignment
   * @param rhs another int type
   * @return self-reference after the rhs is subtracted from the underlying value
   */
  StrongTypeAlias &operator-=(const IntType &rhs) {
    val_ -= rhs;
    return *this;
  }

  /**
   * @param other the other type alias to compare to
   * @return whether underlying value of this < other
   */
  bool operator<(const StrongTypeAlias &other) const { return val_ < other.val_; }

  /**
   * @param other the other type alias to compare to
   * @return whether underlying value of this < other
   */
  bool operator<=(const StrongTypeAlias &other) const { return val_ <= other.val_; }

  /**
   * @param other the other type alias to compare to
   * @return whether underlying value of this < other
   */
  bool operator>(const StrongTypeAlias &other) const { return val_ > other.val_; }

  /**
   * @param other the other type alias to compare to
   * @return whether underlying value of this < other
   */
  bool operator>=(const StrongTypeAlias &other) const { return val_ >= other.val_; }

  /**
   * Outputs the StrongTypeAlias to the output stream.
   * @param os output stream to be written to.
   * @param alias StrongTypeAlias to be output.
   * @return modified output stream.
   */
  friend std::ostream &operator<<(std::ostream &os, const StrongTypeAlias &alias) { return os << alias.val_; }

  /**
   * @return underlying value serialized to json
   */
  nlohmann::json ToJson() const;

  /**
   * @param j json to deserialize
   */
  void FromJson(const nlohmann::json &j);

 private:
  IntType val_;
};
}  // namespace noisepage::common

/* Define all typedefs here */
namespace noisepage {
using byte = std::byte;
using int128_t = __int128;
using uint128_t = unsigned __int128;
using hash_t = uint64_t;
}  // namespace noisepage

namespace std {
// TODO(Tianyu): Expand this specialization if needed.
/**
 * Specialization of StrongTypeAlias for std::atomic<uint32_t>.
 * @tparam Tag a dummy class type to annotate the underlying uint32_t
 */
template <class Tag, class IntType>
struct atomic<noisepage::common::StrongTypeAlias<Tag, IntType>> {
  static_assert(std::is_integral<IntType>::value, "Only int types are defined for strong typedefs");

  /**
   * Type alias shorthand.
   */
  using t = noisepage::common::StrongTypeAlias<Tag, IntType>;
  /**
   * Constructs new atomic variable.
   * @param val value to initialize with.
   */
  explicit atomic(IntType val = 0) : underlying_{val} {}
  /**
   * Constructs new atomic variable.
   * @param val value to initialize with.
   */
  explicit atomic(t val) : underlying_{val.UnderlyingValue()} {}

  DISALLOW_COPY_AND_MOVE(atomic)

  /**
   * Checks if the atomic object is lock-free.
   * @return true if the atomic operations on the objects of this type are lock-free, false otherwise.
   */
  bool is_lock_free() const noexcept {  // NOLINT match underlying API
    return underlying_.is_lock_free();
  }

  /**
   * Atomically replaces the current value with desired. Memory is affected according to the value of order.
   * @param desired	the value to store into the atomic variable.
   * @param order memory order constraints to enforce.
   */
  void store(t desired, memory_order order = memory_order_seq_cst) volatile noexcept {  // NOLINT match underlying API
    underlying_.store(desired.UnderlyingValue(), order);
  }

  /**
   * Atomically loads and returns the current value of the atomic variable.
   * Memory is affected according to the value of order.
   * @param order memory order constraints to enforce.
   * @return The current value of the atomic variable.
   */
  t load(memory_order order = memory_order_seq_cst) const volatile noexcept {  // NOLINT match underlying API
    return t(underlying_.load(order));
  }

  /**
   * Atomically replaces the underlying value with desired. The operation is read-modify-write operation.
   * Memory is affected according to the value of order.
   * @param desired	value to assign.
   * @param order memory order constraints to enforce.
   * @return The value of the atomic variable before the call.
   */
  t exchange(t desired, memory_order order = memory_order_seq_cst) volatile noexcept {  // NOLINT match underlying API
    return t(underlying_.exchange(!desired, order));
  }

  /**
   * Atomically compares the [object representation (until C++20) / value representation (since C++20)] of *this
   * with that of expected, and if those are bitwise-equal, replaces the former with desired
   * (performs read-modify-write operation). Otherwise, loads the actual value stored in *this
   * into expected (performs load operation).
   * @param expected reference to the value expected to be found in the atomic object.
   * @param desired the value to store in the atomic object if it is as expected.
   * @param order the memory synchronization ordering for both operations.
   * @return true if the underlying atomic value was successfully changed, false otherwise.
   */
  // NOLINTNEXTLINE
  bool compare_exchange_weak(t &expected, t desired, memory_order order = memory_order_seq_cst) volatile noexcept {
    return underlying_.compare_exchange_weak(expected.UnderlyingValue(), desired.UnderlyingValue(), order);
  }

  /**
   * Atomically compares the [object representation (until C++20) / value representation (since C++20)] of *this
   * with that of expected, and if those are bitwise-equal, replaces the former with desired
   * (performs read-modify-write operation). Otherwise, loads the actual value stored in *this
   * into expected (performs load operation).
   * @param expected reference to the value expected to be found in the atomic object.
   * @param desired the value to store in the atomic object if it is as expected.
   * @param order the memory synchronization ordering for both operations.
   * @return true if the underlying atomic value was successfully changed, false otherwise.
   */
  // NOLINTNEXTLINE
  bool compare_exchange_strong(t &expected, t desired, memory_order order = memory_order_seq_cst) volatile noexcept {
    return underlying_.compare_exchange_strong(expected.UnderlyingValue(), desired.UnderlyingValue(), order);
  }

  /**
   * Atomic pre-increment.
   * @return the value of the atomic variable after the modification.
   */
  t operator++() volatile noexcept {
    IntType result = ++underlying_;
    return t(result);
  }

  /**
   * Atomic post-increment.
   * @return the value of the atomic variable before the modification.
   */
  t operator++(int) volatile noexcept {
    const IntType result = underlying_++;
    return t(result);
  }

 private:
  atomic<IntType> underlying_;
};

/**
 * Implements std::hash for StrongTypeAlias.
 * @tparam Tag a dummy class type to annotate the underlying type.
 * @tparam T the underlying type.
 */
template <class Tag, typename T>
struct hash<noisepage::common::StrongTypeAlias<Tag, T>> {
  /**
   * Returns the hash of the underlying type's contents.
   * @param alias the aliased type to be hashed.
   * @return the hash of the aliased type.
   */
  size_t operator()(const noisepage::common::StrongTypeAlias<Tag, T> &alias) const {
    return hash<T>()(alias.UnderlyingValue());
  }
};

/**
 * Implements std::less for StrongTypeAlias.
 * @tparam Tag a dummy class type to annotate the underlying type.
 * @tparam T the underlying type.
 */
template <class Tag, class T>
struct less<noisepage::common::StrongTypeAlias<Tag, T>> {
  /**
   * @param x one value
   * @param y other value
   * @return x < y (underlying value)
   */
  bool operator()(const noisepage::common::StrongTypeAlias<Tag, T> &x,
                  const noisepage::common::StrongTypeAlias<Tag, T> &y) const {
    return std::less<T>()(x.UnderlyingValue(), y.UnderlyingValue());
  }
};
}  // namespace std
