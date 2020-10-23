#pragma once

#include <immintrin.h>

#include "common/macros.h"
#include "execution/util/execution_common.h"
#include "execution/util/simd/types.h"

#ifndef SIMD_TOP_LEVEL
#error "Don't include 'execution/util/simd/avx2.h' directly; instead, include 'execution/util/simd.h'"
#endif

namespace noisepage::execution::util::simd {

#define USE_GATHER 0

/**
 * A 256-bit SIMD register vector. This is a purely internal class that holds common functions for other user-visible
 * vector classes.
 */
class Vec256b {
 public:
  Vec256b() = default;
  /**
   * Create a vector whose contents are the 256-bit register reg.
   * @param reg initial contents of the vector
   */
  explicit Vec256b(const __m256i &reg) : reg_(reg) {}

  /**
   * Type-cast operator so that Vec*'s can be used directly with intrinsics.
   */
  ALWAYS_INLINE operator __m256i() const { return reg_; }  // NOLINT

  /**
   * Store the contents of this vector into the provided unaligned pointer.
   */
  void Store(void *ptr) const { _mm256_storeu_si256(reinterpret_cast<__m256i *>(ptr), Reg()); }

  /**
   * Store the contents of this vector into the provided aligned pointer.
   */
  void StoreAligned(void *ptr) const { _mm256_store_si256(reinterpret_cast<__m256i *>(ptr), Reg()); }

  /**
   * @return true if all the corresponding masked bits in this vector are set
   */
  bool AllBitsAtPositionsSet(const Vec256b &mask) const { return _mm256_testc_si256(Reg(), mask) == 1; }

 protected:
  /**
   * @return the underlying register
   */
  const __m256i &Reg() const { return reg_; }

 protected:
  /**
   * Underlying SIMD register.
   */
  __m256i reg_;
};

// ---------------------------------------------------------
// Vec4 Definition
// ---------------------------------------------------------

/**
 * A 256-bit SIMD register interpreted as four 64-bit integer values.
 */
class Vec4 : public Vec256b {
  friend class Vec4Mask;

 public:
  Vec4() = default;
  /**
   * Create a vector with 4 copies of val.
   * @param val initial value for entire vector
   */
  explicit Vec4(int64_t val) { reg_ = _mm256_set1_epi64x(val); }
  /**
   * Create a vector whose contents are the 256-bit register reg.
   * @param reg initial contents of the vector
   */
  explicit Vec4(const __m256i &reg) : Vec256b(reg) {}
  /**
   * Create a vector containing the 4 integers in order MSB [val4, val3, val2, val1] LSB.
   */
  Vec4(int64_t val1, int64_t val2, int64_t val3, int64_t val4) { reg_ = _mm256_setr_epi64x(val1, val2, val3, val4); }

  /**
   * @return number of elements that can be stored in this vector
   */
  static constexpr uint32_t Size() { return 4; }

  /**
   * Load and sign-extend four 32-bit values from the input array
   */
  Vec4 &Load(const int32_t *ptr);

  /**
   * Load and sign-extend four 32-bit values from the input array
   */
  Vec4 &Load(const uint32_t *ptr) { return Load(reinterpret_cast<const int32_t *>(ptr)); }

  /**
   * Load four 64-bit values from the input array
   */
  Vec4 &Load(const int64_t *ptr);

  /**
   * Load four 64-bit values from the input array
   */
  Vec4 &Load(const uint64_t *ptr) { return Load(reinterpret_cast<const int64_t *>(ptr)); }

#ifdef __APPLE__
  // NOLINTNEXTLINE (runtime/int)
  static_assert(sizeof(long) == sizeof(int64_t), "On MacOS, long isn't 64-bits!");

  /**
   * Load four 64-bit values from the input array
   */
  Vec4 &Load(const long *ptr) { return Load(reinterpret_cast<const int64_t *>(ptr)); }  // NOLINT (runtime/int)

  /**
   * Load four 64-bit values from the input array
   */
  Vec4 &Load(const unsigned long *ptr) { return Load(reinterpret_cast<const int64_t *>(ptr)); }  // NOLINT (runtime/int)
#endif

  /**
   * Gather non-contiguous elements from the input array ptr stored at index positions from pos.
   */
  template <typename T>
  Vec4 &Gather(const T *ptr, const Vec4 &pos);

  /**
   * Truncates the four 64-bit elements into 32-bit elements and stores them
   * into the output array.
   */
  void Store(int32_t *arr) const;

  /**
   * Truncates the four 64-bit elements into 32-bit elements and stores them
   * into the output array.
   */
  void Store(uint32_t *arr) const { Store(reinterpret_cast<int32_t *>(arr)); }

  /**
   * Stores the four 64-bit elements in this vector into the output array.
   */
  void Store(int64_t *arr) const;

  /**
   * Stores the four 64-bit elements in this vector into the output array.
   */
  void Store(uint64_t *arr) const { Store(reinterpret_cast<int64_t *>(arr)); }

#ifdef __APPLE__
  // NOLINTNEXTLINE (runtime/int)
  static_assert(sizeof(long) == sizeof(int64_t), "On MacOS, long isn't 64-bits!");

  /**
   * Stores the four 64-bit elements in this vector into the output array.
   */
  void Store(long *arr) const { Store(reinterpret_cast<int64_t *>(arr)); }  // NOLINT (runtime/int)

  /**
   * Stores the four 64-bit elements in this vector into the output array.
   */
  void Store(unsigned long *arr) const { Store(reinterpret_cast<int64_t *>(arr)); }  // NOLINT (runtime/int)
#endif

  /**
   * Extract the integer at the given index from this vector.
   */
  int64_t Extract(uint32_t index) const {
    alignas(32) int64_t x[Size()];
    Store(x);
    return x[index & 3];
  }

  /**
   * Extract the integer at the given index from this vector.
   */
  int64_t operator[](uint32_t index) const { return Extract(index); }
};

// ---------------------------------------------------------
// Vec4 Implementation
// ---------------------------------------------------------

/**
 * Loads 128 bits from ptr as four 32-bit integers, each 32-bit integer is then sign extended to be 64-bit.
 */
ALWAYS_INLINE inline Vec4 &Vec4::Load(const int32_t *ptr) {
  auto tmp = _mm_loadu_si128(reinterpret_cast<const __m128i *>(ptr));
  reg_ = _mm256_cvtepi32_epi64(tmp);
  return *this;
}

/**
 * Loads 256 bits from ptr as four 64-bit integers.
 */
ALWAYS_INLINE inline Vec4 &Vec4::Load(const int64_t *ptr) {
  // Load aligned and unaligned have almost no performance difference on AVX2
  // machines. To alleviate some pain from clients having to know this info
  // we always use an unaligned load.
  reg_ = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(ptr));
  return *this;
}

/**
 * Gather non-contiguous elements from the input array ptr stored at index positions from pos.
 */
template <typename T>
ALWAYS_INLINE inline Vec4 &Vec4::Gather(const T *ptr, const Vec4 &pos) {
  using signed_t = std::make_signed_t<T>;
  return Gather<signed_t>(reinterpret_cast<const signed_t *>(ptr), pos);
}

/**
 * Gathers four 64-bit integers into our vector, sign extending as necessary.
 */
template <>
ALWAYS_INLINE inline Vec4 &Vec4::Gather<int64_t>(const int64_t *ptr, const Vec4 &pos) {
#if USE_GATHER == 1
  reg_ = _mm256_int64_tgather_epi64(ptr, pos, 8);
#else
  alignas(32) int64_t x[Size()];
  pos.Store(x);
  reg_ = _mm256_setr_epi64x(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]]);
#endif
  return *this;
}

/**
 * Truncate the four 64-bit values in this vector and store them into the first four elements of the provided array.
 */
ALWAYS_INLINE inline void Vec4::Store(int32_t *arr) const {
  auto truncated = _mm256_cvtepi64_epi32(Reg());
  _mm_store_si128(reinterpret_cast<__m128i *>(arr), truncated);
}

ALWAYS_INLINE inline void Vec4::Store(int64_t *arr) const { Vec256b::Store(reinterpret_cast<void *>(arr)); }

// ------------------gre---------------------------------------
// Vec8 Definition
// ---------------------------------------------------------

/**
 * A 256-bit SIMD register interpreted as eight 32-bit integer values.
 */
class Vec8 : public Vec256b {
 public:
  Vec8() = default;
  /**
   * Create a vector with 8 copies of val.
   * @param val initial value for entire vector
   */
  explicit Vec8(int32_t val) { reg_ = _mm256_set1_epi32(val); }
  /**
   * Create a vector whose contents are the 256-bit register reg.
   * @param reg initial contents of the vector
   */
  explicit Vec8(const __m256i &reg) : Vec256b(reg) {}
  /**
   * Create a vector containing the 8 integers in order MSB [val8, val7, ..., val1] LSB.
   */
  Vec8(int32_t val1, int32_t val2, int32_t val3, int32_t val4, int32_t val5, int32_t val6, int32_t val7, int32_t val8) {
    reg_ = _mm256_setr_epi32(val1, val2, val3, val4, val5, val6, val7, val8);
  }

  /**
   * Load and sign-extend 8 8-bit values stored contiguously from the input
   * pointer array.
   */
  Vec8 &Load(const int8_t *ptr);

  /**
   * Load and sign-extend 8 8-bit values stored contiguously from the input
   * pointer array.
   */
  Vec8 &Load(const uint8_t *ptr) { return Load(reinterpret_cast<const int8_t *>(ptr)); }

  /**
   * Load and sign-extend 8 16-bit values stored contiguously from the input
   * pointer array.
   */
  Vec8 &Load(const int16_t *ptr);

  /**
   * Load and sign-extend 8 16-bit values stored contiguously from the input
   * pointer array.
   */
  Vec8 &Load(const uint16_t *ptr) { return Load(reinterpret_cast<const int16_t *>(ptr)); }

  /**
   * Load 8 32-bit values stored contiguously from the input pointer array.
   */
  Vec8 &Load(const int32_t *ptr);

  /**
   * Load 8 32-bit values stored contiguously from the input pointer array.
   */
  Vec8 &Load(const uint32_t *ptr) { return Load(reinterpret_cast<const int32_t *>(ptr)); }

  /**
   * Gather non-contiguous elements from the input array ptr stored at index positions from pos.
   * @tparam T data type of the underlying array
   * @param ptr input array
   * @param pos list of positions in the input array to gather
   */
  template <typename T>
  Vec8 &Gather(const T *ptr, const Vec8 &pos);

  /**
   * Stores our eight 32-bit values into arr.
   */
  void Store(int32_t *arr) const;

  /**
   * Stores our eight 32-bit values into arr.
   */
  void Store(uint32_t *arr) const { Store(reinterpret_cast<int32_t *>(arr)); }

  /**
   * @return number of elements that can be stored in this vector
   */
  static constexpr uint32_t Size() { return 8; }

  /**
   * @return the element at the given index
   */
  ALWAYS_INLINE int32_t Extract(uint32_t index) const {
    NOISEPAGE_ASSERT(index < 8, "Out-of-bounds mask element access");
    alignas(32) int32_t x[Size()];
    Store(x);
    return x[index & 7];
  }

  /**
   * @return the element at the given index
   */
  ALWAYS_INLINE int32_t operator[](uint32_t index) const { return Extract(index); }
};

// ---------------------------------------------------------
// Vec8 Implementation
// ---------------------------------------------------------

// Vec8's can be loaded from any array whose element types are smaller than or
// equal to 32-bits. Eight elements are always read from the input array, but
// are up-casted to 32-bits when appropriate.

/**
 * Loads 128 bits from ptr as eight 8-bit integers, each 8-bit integer is then sign extended to be 32-bit.
 * Not a typo: it loads 128 bits.
 */
ALWAYS_INLINE inline Vec8 &Vec8::Load(const int8_t *ptr) {
  auto tmp = _mm_loadu_si128(reinterpret_cast<const __m128i *>(ptr));
  reg_ = _mm256_cvtepi8_epi32(tmp);
  return *this;
}

/**
 * Loads 128 bits from ptr as eight 16-bit integers, each 16-bit integer is then sign extended to be 32-bit.
 */
ALWAYS_INLINE inline Vec8 &Vec8::Load(const int16_t *ptr) {
  auto tmp = _mm_loadu_si128(reinterpret_cast<const __m128i *>(ptr));
  reg_ = _mm256_cvtepi16_epi32(tmp);
  return *this;
}

/**
 * Loads 256 bits from ptr as eight 32-bit integers.
 */
ALWAYS_INLINE inline Vec8 &Vec8::Load(const int32_t *ptr) {
  reg_ = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(ptr));
  return *this;
}

// Like loads, Vec8's can be gathered from any array whose element types are
// smaller than or equal to 32-bits. The input position vector must have eight
// indexes from the input array; eight array elements are always loaded and the
// elements are up-casted when appropriate.

/**
 * Gather non-contiguous elements from the input array ptr stored at index positions from pos.
 * @tparam T data type of the underlying array
 * @param ptr input array
 * @param pos list of positions in the input array to gather
 */
template <typename T>
ALWAYS_INLINE inline Vec8 &Vec8::Gather(const T *ptr, const Vec8 &pos) {
  using signed_t = std::make_signed_t<T>;
  return Gather<signed_t>(reinterpret_cast<const signed_t *>(ptr), pos);
}

/**
 * Gathers sixteen 8-bit integers into our vector, sign extending as necessary.
 */
template <>
ALWAYS_INLINE inline Vec8 &Vec8::Gather<int8_t>(const int8_t *ptr, const Vec8 &pos) {
#if USE_GATHER == 1
  reg_ = _mm256_int32_tgather_epi32(ptr, pos, 1);
  reg_ = _mm256_srai_epi32(reg_, 24);
#else
  alignas(32) int32_t x[Size()];
  pos.Store(x);
  reg_ = _mm256_setr_epi32(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]], ptr[x[4]], ptr[x[5]], ptr[x[6]], ptr[x[7]]);
#endif
  return *this;
}

/**
 * Gathers sixteen 8-bit integers into our vector, sign extending as necessary.
 */
template <>
ALWAYS_INLINE inline Vec8 &Vec8::Gather<int16_t>(const int16_t *ptr, const Vec8 &pos) {
#if USE_GATHER == 1
  reg_ = _mm256_int32_tgather_epi32(ptr, pos, 2);
  reg_ = _mm256_srai_epi32(reg_, 16);
#else
  alignas(32) int32_t x[Size()];
  pos.Store(x);
  reg_ = _mm256_setr_epi32(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]], ptr[x[4]], ptr[x[5]], ptr[x[6]], ptr[x[7]]);
#endif
  return *this;
}

/**
 * Gathers four 8-bit integers into our vector, sign extending as necessary.
 */
template <>
ALWAYS_INLINE inline Vec8 &Vec8::Gather<int32_t>(const int32_t *ptr, const Vec8 &pos) {
#if USE_GATHER == 1
  reg_ = _mm256_int32_tgather_epi32(ptr, pos, 4);
#else
  alignas(32) int32_t x[Size()];
  pos.Store(x);
  reg_ = _mm256_setr_epi32(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]], ptr[x[4]], ptr[x[5]], ptr[x[6]], ptr[x[7]]);
#endif
  return *this;
}

ALWAYS_INLINE inline void Vec8::Store(int32_t *arr) const { Vec256b::Store(reinterpret_cast<void *>(arr)); }

// --------------------------------------------------------
// Vec8Mask Definition
// --------------------------------------------------------

/**
 * An 8-bit mask stored logically as 1-bit in each of the eight 32-bit lanes in a 256-bit register.
 */
class Vec8Mask : public Vec8 {
 public:
  Vec8Mask() = default;
  /**
   * Instantiates a wrapped 256-bit register mask, each of its eight 32-bit lanes is a logical 1-bit.
   * @param reg 256-bit register mask to be wrapped
   */
  explicit Vec8Mask(const __m256i &reg) : Vec8(reg) {}

  /**
   * Extract the value of the bit at index idx in this mask.
   */
  bool Extract(uint32_t idx) const { return Vec8::Extract(idx) != 0; }

  /**
   * Extract the value of the bit at index idx in this mask.
   */
  bool operator[](uint32_t idx) const { return Extract(idx); }
  /**
   * Updates positions to contiguously contain mask's set positions, with a fixed offset added to every element.
   * @param[out] positions will contain all the set positions in the mask with offset added
   * @param offset fixed amount that will be added to every position
   * @return number of set bits in the mask
   */
  uint32_t ToPositions(uint32_t *positions, uint32_t offset) const;

  /**
   * Updates positions to contiguously contain the masked out positions in pos.
   * @param[out] positions will contain all the set positions in the mask with offset added
   * @param pos positions to be masked against
   * @return number of set bits in the mask
   */
  uint32_t ToPositions(uint32_t *positions, const Vec8 &pos) const;
};

// ---------------------------------------------------------
// Vec8Mask Implementation
// ---------------------------------------------------------

ALWAYS_INLINE inline uint32_t Vec8Mask::ToPositions(uint32_t *positions, uint32_t offset) const {
  int32_t mask = _mm256_movemask_ps(_mm256_castsi256_ps(Reg()));
  NOISEPAGE_ASSERT(mask < 256, "8-bit mask must be less than 256");
  __m128i match_pos_scaled = _mm_loadl_epi64(reinterpret_cast<const __m128i *>(&K8_BIT_MATCH_LUT[mask]));
  __m256i match_pos = _mm256_cvtepi8_epi32(match_pos_scaled);
  __m256i pos_vec = _mm256_add_epi32(_mm256_set1_epi32(offset), match_pos);
  _mm256_storeu_si256(reinterpret_cast<__m256i *>(positions), pos_vec);
  return __builtin_popcount(mask);
}

ALWAYS_INLINE inline uint32_t Vec8Mask::ToPositions(uint32_t *positions, const execution::util::simd::Vec8 &pos) const {
  int32_t mask = _mm256_movemask_ps(_mm256_castsi256_ps(Reg()));
  NOISEPAGE_ASSERT(mask < 256, "8-bit mask must be less than 256");
  __m128i perm_comp = _mm_loadl_epi64(reinterpret_cast<const __m128i *>(&K8_BIT_MATCH_LUT[mask]));
  __m256i perm = _mm256_cvtepi8_epi32(perm_comp);
  __m256i perm_pos = _mm256_permutevar8x32_epi32(pos, perm);
  __m256i perm_mask = _mm256_permutevar8x32_epi32(Reg(), perm);
  _mm256_maskstore_epi32(reinterpret_cast<int32_t *>(positions), perm_mask, perm_pos);
  return __builtin_popcount(mask);
}

/**
 * Vec4Mask Definition
 */
class Vec4Mask : public Vec4 {
 public:
  Vec4Mask() = default;
  /**
   * Instantiates a wrapped 4-bit mask.
   * @param mask 4-bit mask to be wrapped
   */
  explicit Vec4Mask(const __m256i &mask) : Vec4(mask) {}

  /**
   * @return true if mask is set at the given index, false otherwise
   */
  int32_t Extract(uint32_t index) const { return static_cast<int32_t>(Vec4::Extract(index) != 0); }

  /**
   * @return true if mask is set at the given index, false otherwise
   */
  int32_t operator[](uint32_t index) const { return Extract(index); }

  /**
   * Updates positions to contiguously contain mask's set positions, with a fixed offset added to every element.
   * @param[out] positions will contain all the set positions in the mask with offset added
   * @param offset fixed amount that will be added to every position
   * @return number of set bits in the mask
   */
  ALWAYS_INLINE inline uint32_t ToPositions(uint32_t *positions, uint32_t offset) const {
    int32_t mask = _mm256_movemask_pd(_mm256_castsi256_pd(Reg()));
    NOISEPAGE_ASSERT(mask < 16, "4-bit mask must be less than 16");
    __m128i match_pos_scaled =
        _mm_loadl_epi64(reinterpret_cast<__m128i *>(const_cast<uint64_t *>(&K4_BIT_MATCH_LUT[mask])));
    __m128i match_pos = _mm_cvtepi16_epi32(match_pos_scaled);
    __m128i pos_vec = _mm_add_epi32(_mm_set1_epi32(offset), match_pos);
    _mm_storeu_si128(reinterpret_cast<__m128i *>(positions), pos_vec);
    return __builtin_popcount(mask);
  }

  /**
   * Updates positions to contiguously contain the masked out positions in pos.
   * @param[out] positions will contain all the set positions in the mask with offset added
   * @param pos positions to be masked against
   * @return number of set bits in the mask
   */
  ALWAYS_INLINE inline uint32_t ToPositions(uint32_t *positions, const Vec4 &pos) const {
    int32_t mask = _mm256_movemask_pd(_mm256_castsi256_pd(Reg()));
    NOISEPAGE_ASSERT(mask < 16, "4-bit mask must be less than 16");

    // TODO(pmenon): Fix this slowness!
    {
      alignas(32) int64_t m_arr[Size()];
      alignas(32) int64_t p_arr[Size()];
      Store(m_arr);
      pos.Store(p_arr);
      for (uint32_t idx = 0, i = 0; i < 4; i++) {
        positions[idx] = static_cast<uint32_t>(p_arr[i]);
        idx += static_cast<uint32_t>(m_arr[i] != 0);
      }
    }

    return __builtin_popcount(mask);
  }
};

// ---------------------------------------------------------
// Vec256b - Generic Bitwise Operations
// ---------------------------------------------------------

// Bit-wise NOT
ALWAYS_INLINE inline Vec256b operator~(const Vec256b &v) { return Vec256b(_mm256_xor_si256(v, _mm256_set1_epi32(-1))); }

ALWAYS_INLINE inline Vec256b operator&(const Vec256b &a, const Vec256b &b) { return Vec256b(_mm256_and_si256(a, b)); }

ALWAYS_INLINE inline Vec256b operator|(const Vec256b &a, const Vec256b &b) { return Vec256b(_mm256_or_si256(a, b)); }

ALWAYS_INLINE inline Vec256b operator^(const Vec256b &a, const Vec256b &b) { return Vec256b(_mm256_xor_si256(a, b)); }

// ---------------------------------------------------------
// Vec8Mask
// ---------------------------------------------------------

ALWAYS_INLINE inline Vec8Mask operator&(const Vec8Mask &a, const Vec8Mask &b) {
  return Vec8Mask(Vec256b(a) & Vec256b(b));
}

// ---------------------------------------------------------
// Vec4Mask
// ---------------------------------------------------------

ALWAYS_INLINE inline Vec4Mask operator&(const Vec4Mask &a, const Vec4Mask &b) {
  return Vec4Mask(Vec256b(a) & Vec256b(b));
}

// ---------------------------------------------------------
// Vec4 - Comparison Operations
// ---------------------------------------------------------

ALWAYS_INLINE inline Vec4Mask operator>(const Vec4 &a, const Vec4 &b) { return Vec4Mask(_mm256_cmpgt_epi64(a, b)); }

ALWAYS_INLINE inline Vec4Mask operator==(const Vec4 &a, const Vec4 &b) { return Vec4Mask(_mm256_cmpeq_epi64(a, b)); }

ALWAYS_INLINE inline Vec4Mask operator>=(const Vec4 &a, const Vec4 &b) { return Vec4Mask(~(b > a)); }

ALWAYS_INLINE inline Vec4Mask operator<(const Vec4 &a, const Vec4 &b) { return b > a; }

ALWAYS_INLINE inline Vec4Mask operator<=(const Vec4 &a, const Vec4 &b) { return b >= a; }

ALWAYS_INLINE inline Vec4Mask operator!=(const Vec4 &a, const Vec4 &b) { return Vec4Mask(~Vec256b(a == b)); }

// ---------------------------------------------------------
// Vec4 - Arithmetic Operations
// ---------------------------------------------------------
// The following operators have to use non const references. So turn off linting.
ALWAYS_INLINE inline Vec4 operator+(const Vec4 &a, const Vec4 &b) { return Vec4(_mm256_add_epi64(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec4 &operator+=(Vec4 &a, const Vec4 &b) {
  a = a + b;
  return a;
}

ALWAYS_INLINE inline Vec4 operator-(const Vec4 &a, const Vec4 &b) { return Vec4(_mm256_sub_epi64(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec4 &operator-=(Vec4 &a, const Vec4 &b) {
  a = a - b;
  return a;
}

ALWAYS_INLINE inline Vec4 operator&(const Vec4 &a, const Vec4 &b) { return Vec4(Vec256b(a) & Vec256b(b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec4 &operator&=(Vec4 &a, const Vec4 &b) {
  a = a & b;
  return a;
}

ALWAYS_INLINE inline Vec4 operator|(const Vec4 &a, const Vec4 &b) { return Vec4(Vec256b(a) | Vec256b(b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec4 &operator|=(Vec4 &a, const Vec4 &b) {
  a = a | b;
  return a;
}

ALWAYS_INLINE inline Vec4 operator^(const Vec4 &a, const Vec4 &b) { return Vec4(Vec256b(a) ^ Vec256b(b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec4 &operator^=(Vec4 &a, const Vec4 &b) {
  a = a ^ b;
  return a;
}

ALWAYS_INLINE inline Vec4 operator>>(const Vec4 &a, const uint32_t shift) { return Vec4(_mm256_srli_epi64(a, shift)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec4 &operator>>=(Vec4 &a, const uint32_t shift) {
  a = a >> shift;
  return a;
}

ALWAYS_INLINE inline Vec4 operator<<(const Vec4 &a, const uint32_t shift) { return Vec4(_mm256_slli_epi64(a, shift)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec4 &operator<<=(Vec4 &a, const uint32_t shift) {
  a = a << shift;
  return a;
}

ALWAYS_INLINE inline Vec4 operator>>(const Vec4 &a, const Vec4 &b) { return Vec4(_mm256_srlv_epi64(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec4 &operator>>=(Vec4 &a, const Vec4 &b) {
  a = a >> b;
  return a;
}

ALWAYS_INLINE inline Vec4 operator<<(const Vec4 &a, const Vec4 &b) { return Vec4(_mm256_sllv_epi64(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec4 &operator<<=(Vec4 &a, const Vec4 &b) {
  a = a << b;
  return a;
}

// ---------------------------------------------------------
// Vec8 - Comparison Operations
// ---------------------------------------------------------

ALWAYS_INLINE inline Vec8Mask operator>(const Vec8 &a, const Vec8 &b) { return Vec8Mask(_mm256_cmpgt_epi32(a, b)); }

ALWAYS_INLINE inline Vec8Mask operator==(const Vec8 &a, const Vec8 &b) { return Vec8Mask(_mm256_cmpeq_epi32(a, b)); }

ALWAYS_INLINE inline Vec8Mask operator>=(const Vec8 &a, const Vec8 &b) {
  __m256i max_a_b = _mm256_max_epu32(a, b);
  return Vec8Mask(_mm256_cmpeq_epi32(a, max_a_b));
}

ALWAYS_INLINE inline Vec8Mask operator<(const Vec8 &a, const Vec8 &b) { return b > a; }

ALWAYS_INLINE inline Vec8Mask operator<=(const Vec8 &a, const Vec8 &b) { return b >= a; }

ALWAYS_INLINE inline Vec8Mask operator!=(const Vec8 &a, const Vec8 &b) { return Vec8Mask(~Vec256b(a == b)); }

// ---------------------------------------------------------
// Vec8 - Arithmetic Operations
// ---------------------------------------------------------

ALWAYS_INLINE inline Vec8 operator+(const Vec8 &a, const Vec8 &b) { return Vec8(_mm256_add_epi32(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator+=(Vec8 &a, const Vec8 &b) {
  a = a + b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator-(const Vec8 &a, const Vec8 &b) { return Vec8(_mm256_sub_epi32(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator-=(Vec8 &a, const Vec8 &b) {
  a = a - b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator*(const Vec8 &a, const Vec8 &b) { return Vec8(_mm256_mullo_epi32(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator*=(Vec8 &a, const Vec8 &b) {
  a = a * b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator&(const Vec8 &a, const Vec8 &b) { return Vec8(Vec256b(a) & Vec256b(b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator&=(Vec8 &a, const Vec8 &b) {
  a = a & b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator|(const Vec8 &a, const Vec8 &b) { return Vec8(Vec256b(a) | Vec256b(b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator|=(Vec8 &a, const Vec8 &b) {
  a = a | b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator^(const Vec8 &a, const Vec8 &b) { return Vec8(Vec256b(a) ^ Vec256b(b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator^=(Vec8 &a, const Vec8 &b) {
  a = a ^ b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator>>(const Vec8 &a, const uint32_t shift) { return Vec8(_mm256_srli_epi32(a, shift)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator>>=(Vec8 &a, const uint32_t shift) {
  a = a >> shift;
  return a;
}

ALWAYS_INLINE inline Vec8 operator<<(const Vec8 &a, const uint32_t shift) { return Vec8(_mm256_slli_epi32(a, shift)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator<<=(Vec8 &a, const uint32_t shift) {
  a = a << shift;
  return a;
}

ALWAYS_INLINE inline Vec8 operator>>(const Vec8 &a, const Vec8 &b) { return Vec8(_mm256_srlv_epi32(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator>>=(Vec8 &a, const Vec8 &b) {
  a = a >> b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator<<(const Vec8 &a, const Vec8 &b) { return Vec8(_mm256_sllv_epi32(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator<<=(Vec8 &a, const Vec8 &b) {
  a = a << b;
  return a;
}

// ---------------------------------------------------------
// Filter
// ---------------------------------------------------------

/**
 * Generic Filter
 */
template <typename T, typename Enable = void>
struct FilterVecSizer;

/**
 * int8_t Filter
 */
template <>
struct FilterVecSizer<int8_t> {
  /**
   * Eight 32-bit integer values.
   */
  using Vec = Vec8;
  /**
   * Mask for eight 32-bit integer values.
   */
  using VecMask = Vec8Mask;
};

/**
 * int16_t Filter
 */
template <>
struct FilterVecSizer<int16_t> {
  /**
   * Eight 32-bit integer values.
   */
  using Vec = Vec8;
  /**
   * Mask for eight 32-bit integer values.
   */
  using VecMask = Vec8Mask;
};

/**
 * int32_t Filter
 */
template <>
struct FilterVecSizer<int32_t> {
  /**
   * Eight 32-bit integer values.
   */
  using Vec = Vec8;
  /**
   * Mask for eight 32-bit integer values.
   */
  using VecMask = Vec8Mask;
};

/**
 * int64_t Filter
 */
template <>
struct FilterVecSizer<int64_t> {
  /**
   * Four 64-bit integer values.
   */
  using Vec = Vec4;
  /**
   * Mask for four 64-bit integer values.
   */
  using VecMask = Vec4Mask;
};

#ifdef __APPLE__  // need this explicit instantiation
/**
 * intptr_t Filter
 */
template <>
struct FilterVecSizer<intptr_t> {
  /**
   * Four 64-bit integer values.
   */
  using Vec = Vec4;
  /**
   * Mask for four 64-bit integer values.
   */
  using VecMask = Vec4Mask;
};
#endif

/**
 * Arbitrary Filter
 */
template <typename T>
struct FilterVecSizer<T, std::enable_if_t<std::is_unsigned_v<T>>> : public FilterVecSizer<std::make_signed_t<T>> {};

template <typename T, template <typename> typename Compare>
static inline uint32_t FilterVectorByVal(const T *RESTRICT in, uint32_t in_count, T val, uint32_t *RESTRICT out,
                                         const uint32_t *RESTRICT sel, uint32_t *RESTRICT in_pos) {
  using Vec = typename FilterVecSizer<T>::Vec;
  using VecMask = typename FilterVecSizer<T>::VecMask;

  const Compare cmp{};

  const Vec xval(val);

  uint32_t out_pos = 0;

  if (sel == nullptr) {
    Vec in_vec;
    for (*in_pos = 0; *in_pos + Vec::Size() < in_count; *in_pos += Vec::Size()) {
      in_vec.Load(in + *in_pos);
      VecMask mask = cmp(in_vec, xval);
      out_pos += mask.ToPositions(out + out_pos, *in_pos);
    }
  } else {
    Vec in_vec, sel_vec;
    for (*in_pos = 0; *in_pos + Vec::Size() < in_count; *in_pos += Vec::Size()) {
      sel_vec.Load(sel + *in_pos);
      in_vec.Gather(in, sel_vec);
      VecMask mask = cmp(in_vec, xval);
      out_pos += mask.ToPositions(out + out_pos, sel_vec);
    }
  }

  return out_pos;
}

template <typename T, template <typename> typename Compare>
static inline uint32_t FilterVectorByVector(const T *RESTRICT in_1, const T *RESTRICT in_2,
                                            const uint32_t in_count,  // NOLINT
                                            uint32_t *RESTRICT out, const uint32_t *RESTRICT sel,
                                            uint32_t *RESTRICT in_pos) {  // NOLINT
  using Vec = typename FilterVecSizer<T>::Vec;
  using VecMask = typename FilterVecSizer<T>::VecMask;

  const Compare cmp{};

  uint32_t out_pos = 0;

  if (sel == nullptr) {
    Vec in_1_vec, in_2_vec;
    for (*in_pos = 0; *in_pos + Vec::Size() < in_count; *in_pos += Vec::Size()) {
      in_1_vec.Load(in_1 + *in_pos);
      in_2_vec.Load(in_2 + *in_pos);
      VecMask mask = cmp(in_1_vec, in_2_vec);
      out_pos += mask.ToPositions(out + out_pos, *in_pos);
    }
  } else {
    Vec in_1_vec, in_2_vec, sel_vec;
    for (*in_pos = 0; *in_pos + Vec::Size() < in_count; *in_pos += Vec::Size()) {
      sel_vec.Load(sel + *in_pos);
      in_1_vec.Gather(in_1, sel_vec);
      in_2_vec.Gather(in_2, sel_vec);
      VecMask mask = cmp(in_1_vec, in_2_vec);
      out_pos += mask.ToPositions(out + out_pos, sel_vec);
    }
  }

  return out_pos;
}

}  // namespace noisepage::execution::util::simd
