#pragma once

#include <llvm/Support/ErrorHandling.h>

#include <cstddef>
#include <cstdint>
#define EXPORT __attribute__((visibility("default")))

/** Selection vector. */
using sel_t = uint16_t;
using int128_t = __int128;
using uint128_t = unsigned __int128;

//===--------------------------------------------------------------------===//
// branch predictor hints
//===--------------------------------------------------------------------===//

#define LIKELY(x) LLVM_LIKELY(x)
#define UNLIKELY(x) LLVM_UNLIKELY(x)

/**
 * Macros to apply functions on all types
 */
#define FOR_EACH_BOOL_TYPE(F, ...) F(bool, __VA_ARGS__)

#define FOR_EACH_SIGNED_INT_TYPE(F, ...) \
  F(int8_t, __VA_ARGS__)                 \
  F(int16_t, __VA_ARGS__)                \
  F(int32_t, __VA_ARGS__)                \
  F(int64_t, __VA_ARGS__)

#define FOR_EACH_UNSIGNED_INT_TYPE(F, ...) \
  F(uint8_t, __VA_ARGS__)                  \
  F(uint16_t, __VA_ARGS__)                 \
  F(uint32_t, __VA_ARGS__)                 \
  F(uint64_t, __VA_ARGS__)

#define FOR_EACH_FLOAT_TYPE(F, ...) \
  F(float, __VA_ARGS__)             \
  F(double, __VA_ARGS__)

#define BOOL_TYPES(F, ...) FOR_EACH_BOOL_TYPE(F, __VA_ARGS__)

#define INT_TYPES(F, ...)                  \
  FOR_EACH_SIGNED_INT_TYPE(F, __VA_ARGS__) \
  FOR_EACH_UNSIGNED_INT_TYPE(F, __VA_ARGS__)

#define FLOAT_TYPES(F, ...) FOR_EACH_FLOAT_TYPE(F, __VA_ARGS__)

#define ALL_NUMERIC_TYPES(F, ...) \
  INT_TYPES(F, __VA_ARGS__)       \
  FLOAT_TYPES(F, __VA_ARGS__)

#define ALL_TYPES(F, ...)    \
  BOOL_TYPES(F, __VA_ARGS__) \
  INT_TYPES(F, __VA_ARGS__)  \
  FLOAT_TYPES(F, __VA_ARGS__)

//===----------------------------------------------------------------------===//
// Indicate that a statement should not be reached
//===----------------------------------------------------------------------===//
#define UNREACHABLE(msg) llvm_unreachable(msg)

namespace terrier::execution {
/**
 * A compact structure used during parsing to capture and describe the position in the source as 1-based line and column
 * number
 */
struct SourcePosition {
  /**
   * Line number
   */
  uint64_t line_;
  /**
   * Column number
   */
  uint64_t column_;
};

/**
 * Use to classify locality of reference for memory accesses
 */
enum class Locality : uint8_t { None = 0, Low = 1, Medium = 2, High = 3 };
}  // namespace terrier::execution
