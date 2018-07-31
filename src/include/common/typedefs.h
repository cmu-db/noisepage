#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iosfwd>
#include <string>
#include <type_traits>
#include <utility>

#include "common/macros.h"

namespace terrier {
/*
 * A strong typedef is like a typedef, except the compiler will enforce explicit
 * conversion for you.
 *
 * Usually, typedefs (or equivalent 'using' statement) are transparent to the
 * compiler. If you declare A and B to both be int, they are interchangeable.
 * This is not exactly ideal because then it becomes easy for you to do something
 * like this:
 *
 * // some defintion
 * void foo(A a, B b);
 *
 * // invocation
 * (a = 42, b = 10)
 * foo(10, 42); // oops
 *
 * ... and the compiler will happily compile and run that code with no warning.
 *
 * With strong typedef, you are required to explicitly convert these types, turning
 * our example into:
 *
 * A a(42);
 * B b(10);
 * foo(a, b);
 *
 * Now foo(b, a) would be a type mismatch.
 *
 * To get 42 out of a, simply do (!a) to get back an int.
 *
 * To call the constructor function-style, use VALUE_OF macro by giving
 * it the name of your strong typedef and the value you want. THE
 * VALUE MUST HAVE EXPLICIT TYPE OF THE UNDERLYING TYPE.
 *
 * e.g. STRONG_TYPEDEF(foo, uint32_t)
 * ...
 * return VALUE_OF(foo, 42u);
 */
#define STRONG_TYPEDEF(name, underlying_type) \
  struct name##_typedef_tag {};               \
  using name = StrongTypeAlias<name##_typedef_tag, underlying_type>;

#define VALUE_OF(name, val) ValueOf<name##_typedef_tag>(val)

/**
 * A StrongTypeAlias is the underlying implementation of STRONG_TYPEDEF.
 *
 * Unless you know what you are doing, you shouldn't touch this class. Just use
 * the MACRO defined above
 * @tparam Tag a dummy class type to annotate the underlying type
 * @tparam T the underlying type
 */
template <class Tag, typename T>
class StrongTypeAlias {
 public:
  StrongTypeAlias() : val_() {}
  explicit StrongTypeAlias(const T &val) : val_(val) {}
  explicit StrongTypeAlias(T &&val) : val_(std::move(val)) {}

  const T &operator!() const { return val_; }

  bool operator==(const StrongTypeAlias &rhs) const { return val_ == rhs.val_; }

  bool operator!=(const StrongTypeAlias &rhs) const { return val_ != rhs.val_; }

  friend std::ostream &operator<<(std::ostream &os, const StrongTypeAlias &alias) { return os << alias.val_; }

 private:
  T val_;
};

template <class Tag, typename T>
StrongTypeAlias<Tag, T> ValueOf(T val) {
  return StrongTypeAlias<Tag, T>(val);
}

// TODO(Tianyu): Follow this example to extend the StrongTypeAlias type to
// have the operators and other std utils you normally expect from certain types.
// template <class Tag>
// class StrongTypeAlias<Tag, uint32_t> {
//  // Write your operator here!
//};

/* Define all typedefs here! */
// TODO(Tianyu): Maybe?
using byte = std::byte;
STRONG_TYPEDEF(timestamp_t, uint64_t);
STRONG_TYPEDEF(layout_version_t, uint32_t);
}  // namespace terrier

namespace std {
// TODO(Tianyu): This might be what std::atomic will give you by default
// for 32-bit structs. But you will probably need to explicitly specialize
// if you want operators.

// TODO(Tianyu): Expand this specialization if need other things
// from std::atomic<uint32_t>
template <class Tag>
struct atomic<terrier::StrongTypeAlias<Tag, uint32_t>> {
  using t = terrier::StrongTypeAlias<Tag, uint32_t>;
  explicit atomic(uint32_t val = 0) : underlying_{val} {}
  explicit atomic(t val) : underlying_{!val} {}
  DISALLOW_COPY_AND_MOVE(atomic);

  bool is_lock_free() const noexcept { return underlying_.is_lock_free(); }

  void store(t desired, memory_order order = memory_order_seq_cst) volatile noexcept {
    underlying_.store(!desired, order);
  }

  t load(memory_order order = memory_order_seq_cst) const volatile noexcept { return t(underlying_.load(order)); }

  t exchange(t desired, memory_order order = memory_order_seq_cst) volatile noexcept {
    return t(underlying_.exchange(!desired, order));
  }

  bool compare_exchange_weak(t &expected, t desired, memory_order order = memory_order_seq_cst) volatile noexcept {
    return underlying_.compare_exchange_weak(!expected, !desired, order);
  }

  bool compare_exchange_strong(t &expected, t desired, memory_order order = memory_order_seq_cst) volatile noexcept {
    return underlying_.compare_exchange_strong(!expected, !desired, order);
  }

  t operator++() volatile noexcept {
    uint32_t result = ++underlying_;
    return t(result);
  }

  t operator++(int) volatile noexcept {
    const uint32_t result = underlying_++;
    return t(result);
  }

 private:
  atomic<uint32_t> underlying_;
};

template <class Tag, typename T>
struct hash<terrier::StrongTypeAlias<Tag, T>> {
  size_t operator()(const terrier::StrongTypeAlias<Tag, T> &alias) const { return hash<T>()(!alias); }
};
}  // namespace std
