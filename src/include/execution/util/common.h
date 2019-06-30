#pragma once

#include <cstddef>
#include <cstdint>

#include <string>

/**
 * Common integral type shorthands
 */
using i8 = int8_t;
using i16 = int16_t;
using i32 = int32_t;
using i64 = int64_t;
using i128 = __int128;
using u8 = uint8_t;
using u16 = uint16_t;
using u32 = uint32_t;
using u64 = uint64_t;
using u128 = unsigned __int128;
using f32 = float;
using f64 = double;
using byte = std::byte;

using hash_t = u64;

#define FOR_EACH_SIGNED_INT_TYPE(F, ...) \
  F(i8, __VA_ARGS__)                     \
  F(i16, __VA_ARGS__)                    \
  F(i32, __VA_ARGS__)                    \
  F(i64, __VA_ARGS__)

#define FOR_EACH_UNSIGNED_INT_TYPE(F, ...) \
  F(u8, __VA_ARGS__)                       \
  F(u16, __VA_ARGS__)                      \
  F(u32, __VA_ARGS__)                      \
  F(u64, __VA_ARGS__)

#define FOR_EACH_FLOAT_TYPE(F, ...) \
  F(f32, __VA_ARGS__)               \
  F(f64, __VA_ARGS__)

#define INT_TYPES(F, ...)                  \
  FOR_EACH_SIGNED_INT_TYPE(F, __VA_ARGS__) \
  FOR_EACH_UNSIGNED_INT_TYPE(F, __VA_ARGS__)

#define ALL_TYPES(F, ...)   \
  INT_TYPES(F, __VA_ARGS__) \
  FOR_EACH_FLOAT_TYPE(F, __VA_ARGS__)

namespace tpl {

/**
 * A compact structure used during parsing to capture and describe the position in the source as 1-based line and column
 * number
 */
struct SourcePosition {
  /**
   * Line number
   */
  u64 line;
  /**
   * Column number
   */
  u64 column;
};

/**
 * Use to classify locality of reference for memory accesses
 */
enum class Locality : u8 { None = 0, Low = 1, Medium = 2, High = 3 };

/**
 * The number of bits per byte
 */
static constexpr const u32 kBitsPerByte = 8;

/**
 * The default vector size to use when performing vectorized iteration
 */
static constexpr const u32 kDefaultVectorSize = 2048;

/**
 * The default prefetch distance to use
 */
static constexpr const u32 kPrefetchDistance = 16;

/**
 * The key for the parser to use in attribute maps to signal
 * whether or not a ForIn loop contains an OID iterator
 */
static constexpr const char *OID_KEY = "IS_OID";

// Common memory sizes
/**
 * KB
 */
static constexpr const u32 KB = 1024;

/**
 * MB
 */
static constexpr const u32 MB = KB * KB;

/**
 * GB
 */
static constexpr const u32 GB = KB * KB * KB;

}  // namespace tpl
