#pragma once
#include "common/typedefs.h"
#include "gtest/gtest.h"

namespace terrier {

struct StorageTestUtil {
  StorageTestUtil() = delete;
#define TO_INT(p) reinterpret_cast<uintptr_t>(p)
  /**
   * Check if memory address represented by val in [lower, upper)
   * @tparam A type of ptr
   * @tparam B type of ptr
   * @tparam C type of ptr
   * @param val value to check
   * @param lower lower bound
   * @param upper upper bound
   */
  template <typename A, typename B, typename C>
  static void CheckInBounds(A *val, B *lower, C *upper) {
    EXPECT_GE(TO_INT(val), TO_INT(lower));
    EXPECT_LT(TO_INT(val), TO_INT(upper));
  };

  /**
   * Check if memory address represented by val not in [lower, upper)
   * @tparam A type of ptr
   * @tparam B type of ptr
   * @tparam C type of ptr
   * @param val value to check
   * @param lower lower bound
   * @param upper upper bound
   */
  template <typename A, typename B, typename C>
  static void CheckNotInBounds(A *val, B *lower, C *upper) {
    EXPECT_TRUE(TO_INT(val) < TO_INT(lower) || TO_INT(val) >= TO_INT(upper));
  };
#undef TO_INT
  /**
   * @tparam A type of ptr
   * @param ptr ptr to start from
   * @param bytes bytes to advance
   * @return  pointer that is the specified amount of bytes ahead of the given
   */
  template <typename A>
  static A *IncrementByBytes(A *ptr, uint64_t bytes) {
    return reinterpret_cast<A *>(reinterpret_cast<byte *>(ptr) + bytes);
  }
};
}  // namespace terrier
