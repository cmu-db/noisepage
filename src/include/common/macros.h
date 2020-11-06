#pragma once

#include <cassert>
#include <stdexcept>

//===--------------------------------------------------------------------===//
// attributes
//===--------------------------------------------------------------------===//

#define UNUSED_ATTRIBUTE __attribute__((unused))
#define RESTRICT __restrict__
#define NEVER_INLINE __attribute__((noinline))
#define PACKED __attribute__((packed))
// NOLINTNEXTLINE
#define FALLTHROUGH [[fallthrough]]
#define NORETURN __attribute((noreturn))

#ifdef NDEBUG
#define ALWAYS_INLINE __attribute__((always_inline))
#else
#ifndef ALWAYS_INLINE
#define ALWAYS_INLINE
#endif
#endif

#ifdef __clang__
#define NO_CLONE
#else
#define NO_CLONE __attribute__((noclone))
#endif

//===--------------------------------------------------------------------===//
// ALWAYS_ASSERT
//===--------------------------------------------------------------------===//

#ifdef NDEBUG
#define NOISEPAGE_ASSERT(expr, message) ((void)0)
#else
/*
 * On assert failure, most existing implementations of C++ will print out the condition.
 * By ANDing the truthy not-null message and our initial expression together, we get
 * asserts-with-messages without needing to bring in iostream or logging.
 */
#define NOISEPAGE_ASSERT(expr, message) assert((expr) && (message))
#endif /* NDEBUG */

//===--------------------------------------------------------------------===//
// Compiler version checks
//===--------------------------------------------------------------------===//

#if __GNUC__ > 6 || (__GNUC__ == 6 && __GNUC_MINOR__ >= 0)
#define GCC_AT_LEAST_6 1
#else
#define GCC_AT_LEAST_6 0
#endif

#if __GNUC__ > 5 || (__GNUC__ == 5 && __GNUC_MINOR__ >= 1)
#define GCC_AT_LEAST_51 1
#else
#define GCC_AT_LEAST_51 0
#endif

// g++-5.0 does not support overflow builtins
#if GCC_AT_LEAST_51
#define GCC_OVERFLOW_BUILTINS_DEFINED 1
#else
#define GCC_OVERFLOW_BUILTINS_DEFINED 0
#endif

//===--------------------------------------------------------------------===//
// Port to OSX
//===---------------------------
#ifdef __APPLE__
#define off64_t off_t
#define MAP_ANONYMOUS MAP_ANON
#endif

//===--------------------------------------------------------------------===//
// utils
//===--------------------------------------------------------------------===//

#define ARRAY_NELEMS(a) (sizeof(a) / sizeof((a)[0]))

//===----------------------------------------------------------------------===//
// Handy macros to hide move/copy class constructors
//===----------------------------------------------------------------------===//

// Macros to disable copying and moving
#ifndef DISALLOW_COPY
#define DISALLOW_COPY(cname)     \
  /* Delete copy constructor. */ \
  cname(const cname &) = delete; \
  /* Delete copy assignment. */  \
  cname &operator=(const cname &) = delete;

#define DISALLOW_MOVE(cname)     \
  /* Delete move constructor. */ \
  cname(cname &&) = delete;      \
  /* Delete move assignment. */  \
  cname &operator=(cname &&) = delete;

/**
 * Disable copy and move.
 */
#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname);               \
  DISALLOW_MOVE(cname);

/** Disallow instantiation of the class. This should be used for classes that only have static functions. */
#define DISALLOW_INSTANTIATION(cname) \
  /* Prevent instantiation. */        \
  cname() = delete;

#endif

/**
 * Used to mark a class as only obtainable from reinterpreting a chunk of memory initialized as byte array or a buffer.
 *
 * Such classes typically have variable-length objects, that the c++ compiler cannot initialize or lay out correctly as
 * local variables. Thus we will have to keep track of the memory space and take care to only refer to these objects
 * with pointers.
 *
 * Typically classes marked with these will expose static factory methods that calculate the size of an object in memory
 * given some parameters and an Initialize method to construct a valid object from pointer to a chunk of memory
 */
#define MEM_REINTERPRETATION_ONLY(cname) \
  cname() = delete;                      \
  DISALLOW_COPY_AND_MOVE(cname)          \
  ~cname() = delete;

//===----------------------------------------------------------------------===//
// LLVM version checking macros
//===----------------------------------------------------------------------===//

#define LLVM_VERSION_GE(major, minor) \
  (LLVM_VERSION_MAJOR > (major) || (LLVM_VERSION_MAJOR == (major) && LLVM_VERSION_MINOR >= (minor)))

#define LLVM_VERSION_EQ(major, minor) (LLVM_VERSION_MAJOR == (major) && LLVM_VERSION_MINOR == (minor))

//===----------------------------------------------------------------------===//
// switch statements
//===----------------------------------------------------------------------===//
#if defined __clang__
#define NOISEPAGE_FALLTHROUGH [[clang::fallthrough]]  // NOLINT
#elif defined __GNUC__ && __GNUC__ >= 7
#define NOISEPAGE_FALLTHROUGH [[fallthrough]]  // NOLINT
#else
#define NOISEPAGE_FALLTHROUGH
#endif

//===----------------------------------------------------------------------===//
// Google Test ONLY
//===----------------------------------------------------------------------===//
#ifdef NDEBUG
#define GTEST_DEBUG_ONLY(TestName) DISABLED_##TestName
#else
#define GTEST_DEBUG_ONLY(TestName) TestName
#endif

#define FRIEND_TEST(test_case_name, test_name) friend class test_case_name##_##test_name##_Test

// We use a dependency injection style where nullptr means the feature is disabled for many components
// This macro exists purely to improve readability of code.
#define DISABLED nullptr
// Use this macro to add polymorphism to a class, when the sole purpose of it is to enable mocks and fakes
// during testing. This makes it clear to the reader that no other form of polymorphism is expected. This
// also means it is possible to get performance back through use of compiler macro magic
// TODO(Tianyu): The easiest thing to do is to write this wrapped in a if on some macro flag (NO_FAKE),
// and then we can turn this off and on from cmake
#define FAKED_IN_TEST virtual
