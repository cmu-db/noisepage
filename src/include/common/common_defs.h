#pragma once
#include <cstdint>
#include <type_traits>
#include <iosfwd>
#include <string>

namespace terrier {
// TODO(Tianyu): Maybe?
using byte = std::byte;

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
 */
#define STRONG_TYPEDEF(name, underlying_type)      \
  struct name##_typedef_tag {};                    \
  using name = StrongTypeAlias<name##_typedef_tag, underlying_type>;

/**
 * A StrongTypeAlias is the underlying implementation of STRONG_TYPEDEF.
 *
 * Unless you know what you are doing, you shouldn't touch this class. Just use
 * the MACRO defined above
 * @tparam Tag a dummy class type to annotate the underlying type
 * @tparam T the underlying type
 */
template<class Tag, typename T>
class StrongTypeAlias {
 public:
  StrongTypeAlias() : val_() {}
  explicit StrongTypeAlias(const T &val) : val_(val) {}
  explicit StrongTypeAlias(T &&val)
      noexcept(std::is_nothrow_move_constructible<T>::value): val_(std::move(val)) {}

  StrongTypeAlias(const StrongTypeAlias &other) = default;
  StrongTypeAlias(StrongTypeAlias &&other)
      noexcept(std::is_nothrow_move_constructible<T>::value) = default;

  T &operator!() {
    return val_;
  }

  friend std::ostream &operator<<(std::ostream &os, const StrongTypeAlias &alias) {
    return os << alias.val_;
  }

 private:
  T val_;
};

namespace std {
template<class Tag, typename T>
template <> struct hash<StrongTypeAlias<Tag, T>> {
  size_t operator()(const StrongTypeAlias<Tag, T> &alias) {
    return hash<T>()(!alias);
  }
};
}

// TODO(Tianyu): Remove this if we don't end up using tbb
namespace tbb {
template <class Tag, typename T>
template <> struct tbb_hash<StrongTypeAlias<Tag, T>> {
  size_t operator()(const StrongTypeAlias<Tag, T> &alias) {
    return tbb_hash<T>(!alias);
  }
};
}

template<typename E>
class EnumHash {
 public:
  uint64_t operator()(const E &e) const {
    return std::hash<typename std::underlying_type<E>::type>()(
        static_cast<typename std::underlying_type<E>::type>(e));
  }
};

class Constants {
 public:
  // 1 Megabyte, in bytes
  const static uint32_t BLOCK_SIZE = 1048576;
};
}