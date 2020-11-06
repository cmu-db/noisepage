#pragma once

#include <immintrin.h>

#include "common/macros.h"
#include "execution/util/execution_common.h"
#include "execution/util/simd/types.h"

namespace noisepage::execution::util::simd {

#define USE_GATHER 1

// ---------------------------------------------------------
// Vec256b Definition
// ---------------------------------------------------------

/**
 * A 512-bit SIMD register vector. This is a purely internal class that holds common functions for other user-visible
 * vector classes
 */
class Vec512b {
 public:
  Vec512b() = default;
  /**
   * Instantiates a 512-bit SIMD register vector with the same contents as reg.
   * @param reg 512-bit SIMD register
   */
  explicit Vec512b(const __m512i &reg) : reg_(reg) {}

  /**
   * Type-cast operator so that Vec*'s can be used directly with intrinsics.
   */
  ALWAYS_INLINE operator __m512i() const { return reg_; }  // NOLINT

  /**
   * Store the contents of this vector into the provided unaligned pointer.
   */
  ALWAYS_INLINE void StoreUnaligned(void *ptr) const { _mm512_storeu_si512(ptr, Reg()); }

  /**
   * Store the contents of this vector into the provided aligned pointer.
   */
  ALWAYS_INLINE void StoreAligned(void *ptr) const { _mm512_store_si512(ptr, Reg()); }

 protected:
  /**
   * @return the underlying register
   */
  const __m512i &Reg() const { return reg_; }

 protected:
  /**
   * Underlying SIMD register.
   */
  __m512i reg_;
};

// ---------------------------------------------------------
// Vec8 Definition
// ---------------------------------------------------------

/**
 * A 512-bit SIMD register interpreted as eight 64-bit integer values.
 */
class Vec8 : public Vec512b {
 public:
  Vec8() = default;
  /**
   * Create a vector with 8 copies of val.
   * @param val initial value for entire vector
   */
  explicit Vec8(int64_t val) { reg_ = _mm512_set1_epi64(val); }
  /**
   * Create a vector whose contents are the 512-bit register reg.
   * @param reg initial contents of the vector
   */
  explicit Vec8(const __m512i &reg) : Vec512b(reg) {}
  /**
   * Create a vector containing the 8 integers in order MSB [val8, val7, ..., val1] LSB.
   */
  Vec8(int64_t val1, int64_t val2, int64_t val3, int64_t val4, int64_t val5, int64_t val6, int64_t val7, int64_t val8) {
    reg_ = _mm512_setr_epi64(val1, val2, val3, val4, val5, val6, val7, val8);
  }

  /**
   * @return number of elements that can be stored in this vector
   */
  static constexpr uint32_t Size() { return 8; }

  /**
   * Load and sign-extend eight 32-bit values stored contiguously from the
   * input pointer array.
   */
  Vec8 &Load(const int32_t *ptr);

  /**
   * Load and sign-extend eight 32-bit values stored contiguously from the
   * input pointer array.
   */
  Vec8 &Load(const uint32_t *ptr) { return Load(reinterpret_cast<const int32_t *>(ptr)); }

  /**
   * Load and sign-extend eight 64-bit values stored contiguously from the
   * input pointer array.
   */
  Vec8 &Load(const int64_t *ptr);

  /**
   * Load and sign-extend eight 64-bit values stored contiguously from the
   * input pointer array.
   */
  Vec8 &Load(const uint64_t *ptr) { return Load(reinterpret_cast<const int64_t *>(ptr)); }

#ifdef __APPLE__
  // NOLINTNEXTLINE (runtime/int)
  static_assert(sizeof(long) == sizeof(int64_t), "On MacOS, long isn't 64-bits!");

  /**
   * Load and sign-extend eight 64-bit values stored contiguously from the
   * input pointer array.
   */
  Vec8 &Load(const long *ptr) { return Load(reinterpret_cast<const int64_t *>(ptr)); }  // NOLINT (runtime/int)

  /**
   * Load and sign-extend eight 64-bit values stored contiguously from the
   * input pointer array.
   */
  Vec8 &Load(const unsigned long *ptr) { return Load(reinterpret_cast<const int64_t *>(ptr)); }  // NOLINT(runtime/int)
#endif

  /**
   * Gathers 8 elements into our vector using the given positions.
   * @tparam T integral type of size (8, 16, 32, 64) bits
   * @param ptr data to be reshuffled
   * @param pos positions to shuffle with, e.g. ptr[pos[0]] is at LSB
   * @return our updated vector
   */
  template <typename T>
  Vec8 &Gather(const T *ptr, const Vec8 &pos);

  /**
   * Truncate eight 64-bit integers to eight 8-bit integers and store the
   * result into the output array.
   */
  void Store(int8_t *arr) const;

  /**
   * Truncate eight 64-bit integers to eight 8-bit integers and store the
   * result into the output array.
   */
  void Store(uint8_t *arr) const { Store(reinterpret_cast<int8_t *>(arr)); }

  /**
   * Truncate eight 64-bit integers to eight 16-bit integers and store the
   * result into the output array.
   */
  void Store(int16_t *arr) const;

  /**
   * Truncate eight 64-bit integers to eight 16-bit integers and store the
   * result into the output array.
   */
  void Store(uint16_t *arr) const { Store(reinterpret_cast<int16_t *>(arr)); }

  /**
   * Truncate eight 64-bit integers to eight 32-bit integers and store the
   * result into the output array.
   */
  void Store(int32_t *arr) const;

  /**
   * Truncate eight 64-bit integers to eight 32-bit integers and store the
   * result into the output array.
   */
  void Store(uint32_t *arr) const { Store(reinterpret_cast<int32_t *>(arr)); }

  /**
   * Store the eight 64-bit integers in this vector into the output array.
   */
  void Store(int64_t *arr) const;

  /**
   * Store the eight 64-bit integers in this vector into the output array.
   */
  void Store(uint64_t *arr) const { Store(reinterpret_cast<int64_t *>(arr)); }

#ifdef __APPLE__
  // NOLINTNEXTLINE (runtime/int)
  static_assert(sizeof(long) == sizeof(int64_t), "On MacOS, long isn't 64-bits!");

  /**
   * Store the eight 64-bit integers in this vector into the output array.
   */
  void Store(long *arr) const { Store(reinterpret_cast<int64_t *>(arr)); }  // NOLINT (runtime/int)

  /**
   * Store the eight 64-bit integers in this vector into the output array.
   */
  void Store(unsigned long *arr) const { Store(reinterpret_cast<int64_t *>(arr)); }  // NOLINT (runtime/int)
#endif

  /**
   * @return true if all the corresponding masked bits in this vector are set
   */
  bool AllBitsAtPositionsSet(const Vec8 &mask) const;

  /**
   * @return the element at the given index
   */
  int64_t Extract(uint32_t index) const {
    NOISEPAGE_ASSERT(index < 8, "Out-of-bounds mask element access");
    alignas(64) int64_t x[Size()];
    Store(x);
    return x[index & 7];
  }

  /**
   * @return the element at the given index
   */
  int64_t operator[](uint32_t index) const { return Extract(index); }
};

// ---------------------------------------------------------
// Vec8 Implementation
// ---------------------------------------------------------

/**
 * Loads 256 bits from ptr as eight 32-bit integers, each 32-bit integer is then sign extended to be 64-bit.
 */
ALWAYS_INLINE inline Vec8 &Vec8::Load(const int32_t *ptr) {
  auto tmp = _mm256_load_si256(reinterpret_cast<const __m256i *>(ptr));
  reg_ = _mm512_cvtepi32_epi64(tmp);
  return *this;
}

/**
 * Loads 512 bits from ptr as eight 64-bit integers.
 */
ALWAYS_INLINE inline Vec8 &Vec8::Load(const int64_t *ptr) {
  reg_ = _mm512_loadu_si512((const __m512i *)ptr);
  return *this;
}

template <typename T>
ALWAYS_INLINE inline Vec8 &Vec8::Gather(const T *ptr, const Vec8 &pos) {
#if USE_GATHER
  reg_ = _mm512_i64gather_epi64(pos, ptr, 8);
#else
  alignas(64) int64_t x[Size()];
  pos.Store(x);
  reg_ = _mm512_setr_epi64(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]], ptr[x[4]], ptr[x[5]], ptr[x[6]], ptr[x[7]]);
#endif
  return *this;
}

ALWAYS_INLINE inline void Vec8::Store(int8_t *arr) const {
  const __mmask8 all(~(uint8_t)0);
  _mm512_mask_cvtepi64_storeu_epi8(reinterpret_cast<void *>(arr), all, Reg());
}

ALWAYS_INLINE inline void Vec8::Store(int16_t *arr) const {
  const __mmask8 all(~(uint8_t)0);
  _mm512_mask_cvtepi64_storeu_epi16(reinterpret_cast<void *>(arr), all, Reg());
}

ALWAYS_INLINE inline void Vec8::Store(int32_t *arr) const {
  const __mmask8 all(~(uint8_t)0);
  _mm512_mask_cvtepi64_storeu_epi32(reinterpret_cast<void *>(arr), all, Reg());
}

ALWAYS_INLINE inline void Vec8::Store(int64_t *arr) const { Vec512b::StoreUnaligned(reinterpret_cast<void *>(arr)); }

ALWAYS_INLINE inline bool Vec8::AllBitsAtPositionsSet(const Vec8 &mask) const {
  return _mm512_testn_epi64_mask(Reg(), mask) == 0;
}

/**
 * Vector with sixteen 32-bit values.
 */
class Vec16 : public Vec512b {
 public:
  Vec16() = default;
  /**
   * Create a vector with 16 copies of val.
   * @param val initial value for entire vector
   */
  explicit Vec16(int32_t val) { reg_ = _mm512_set1_epi32(val); }
  /**
   * Create a vector whose contents are the 512-bit register reg.
   * @param reg initial contents of the vector
   */
  explicit Vec16(const __m512i &reg) : Vec512b(reg) {}
  /**
   * Create a vector containing the 16 integers in order MSB [val16, val15, ..., val1] LSB.
   */
  Vec16(int32_t val1, int32_t val2, int32_t val3, int32_t val4, int32_t val5, int32_t val6, int32_t val7, int32_t val8,
        int32_t val9, int32_t val10, int32_t val11, int32_t val12, int32_t val13, int32_t val14, int32_t val15,
        int32_t val16) {
    reg_ = _mm512_setr_epi32(val1, val2, val3, val4, val5, val6, val7, val8, val9, val10, val11, val12, val13, val14,
                             val15, val16);
  }

  /**
   * @return number of elements that can be stored in this vector
   */
  ALWAYS_INLINE static constexpr int Size() { return 16; }

  /**
   * Load and sign-extend 16 8-bit values stored contiguously from the input
   * pointer array.
   */
  Vec16 &Load(const int8_t *ptr);

  /**
   * Load and sign-extend 16 8-bit values stored contiguously from the input
   * pointer array.
   */
  Vec16 &Load(const uint8_t *ptr) { return Load(reinterpret_cast<const int8_t *>(ptr)); }

  /**
   * Load and sign-extend 16 16-bit values stored contiguously from the input
   * pointer array.
   */
  Vec16 &Load(const int16_t *ptr);

  /**
   * Load and sign-extend 16 16-bit values stored contiguously from the input
   * pointer array.
   */
  Vec16 &Load(const uint16_t *ptr) { return Load(reinterpret_cast<const int16_t *>(ptr)); }

  /**
   * Load 16 32-bit values stored contiguously from the input pointer array.
   */
  Vec16 &Load(const int32_t *ptr);

  /**
   * Load 16 32-bit values stored contiguously from the input pointer array.
   */
  Vec16 &Load(const uint32_t *ptr) { return Load(reinterpret_cast<const int32_t *>(ptr)); }

  /**
   * Gathers 16 elements into our vector using the given positions.
   * @tparam T integral type of size (8, 16, 32) bits
   * @param ptr data to be reshuffled
   * @param pos positions to shuffle with, e.g. ptr[pos[0]] is at LSB
   * @return our updated vector
   */
  template <typename T>
  Vec16 &Gather(const T *ptr, const Vec16 &pos);

  /**
   * Truncate the 16 32-bit integers in this vector to 16 8-bit integers and
   * store the result in the output array.
   */
  void Store(int8_t *arr) const;

  /**
   * Truncate the 16 32-bit integers in this vector to 16 8-bit integers and
   * store the result in the output array.
   */
  void Store(uint8_t *arr) const { Store(reinterpret_cast<int8_t *>(arr)); }

  /**
   * Truncate the 16 32-bit integers in this vector to 16 16-bit integers and
   * store the result in the output array.
   */
  void Store(int16_t *arr) const;

  /**
   * Truncate the 16 32-bit integers in this vector to 16 16-bit integers and
   * store the result in the output array.
   */
  void Store(uint16_t *arr) const { Store(reinterpret_cast<int16_t *>(arr)); }

  /**
   * Store the 16 32-bit integers in the output array.
   */
  void Store(int32_t *arr) const;

  /**
   * Store the 16 32-bit integers in the output array.
   */
  void Store(uint32_t *arr) const { Store(reinterpret_cast<int32_t *>(arr)); }

  /**
   * Stores our sixteen 32-bit elements into arr, truncating if necessary.
   */
  template <typename T>
  typename std::enable_if_t<std::conjunction_v<std::is_integral<T>, std::is_unsigned<T>>> Store(T *arr) const {
    using SignedType = std::make_signed_t<T>;
    Store(reinterpret_cast<SignedType *>(arr));
  }

  /**
   * Test if all masked bits in this vector are set.
   * @param mask sixteen 32-bit masks
   * @return true if all corresponding bits in the vector are set
   */
  bool AllBitsAtPositionsSet(const Vec16 &mask) const;

  /**
   * @return the element at the given index
   */
  int32_t Extract(uint32_t index) const {
    alignas(64) int32_t x[Size()];
    Store(x);
    return x[index & 15];
  }

  /**
   * @return the element at the given index
   */
  int32_t operator[](uint32_t index) const { return Extract(index); }
};

// ---------------------------------------------------------
// Vec16 Implementation
// ---------------------------------------------------------

ALWAYS_INLINE inline Vec16 &Vec16::Load(const int8_t *ptr) {
  auto tmp = _mm_loadu_si128(reinterpret_cast<const __m128i *>(ptr));
  reg_ = _mm512_cvtepi8_epi32(tmp);
  return *this;
}

ALWAYS_INLINE inline Vec16 &Vec16::Load(const int16_t *ptr) {
  auto tmp = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(ptr));
  reg_ = _mm512_cvtepi16_epi32(tmp);
  return *this;
}

ALWAYS_INLINE inline Vec16 &Vec16::Load(const int32_t *ptr) {
  reg_ = _mm512_loadu_si512(ptr);
  return *this;
}

/**
 * Gathers 16 elements into our vector using the given positions.
 * @tparam T integral type of size (8, 16, 32) bits
 * @param ptr data to be reshuffled
 * @param pos positions to shuffle with, e.g. ptr[pos[0]] is at LSB
 * @return our updated vector
 */
template <typename T>
ALWAYS_INLINE inline Vec16 &Vec16::Gather(const T *ptr, const Vec16 &pos) {
  using signed_t = std::make_signed_t<T>;
  return Gather<signed_t>(reinterpret_cast<const signed_t *>(ptr), pos);
}

/**
 * Gathers sixteen 8-bit integers into our vector, sign extending as necessary.
 */
template <>
ALWAYS_INLINE inline Vec16 &Vec16::Gather<int8_t>(const int8_t *ptr, const Vec16 &pos) {
  alignas(64) int32_t x[Size()];
  pos.Store(x);
  reg_ =
      _mm512_setr_epi32(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]], ptr[x[4]], ptr[x[5]], ptr[x[6]], ptr[x[7]],
                        ptr[x[8]], ptr[x[9]], ptr[x[10]], ptr[x[11]], ptr[x[12]], ptr[x[13]], ptr[x[14]], ptr[x[15]]);
  return *this;
}

/**
 * Gathers sixteen 16-bit integers into our vector, sign extending as necessary.
 */
template <>
ALWAYS_INLINE inline Vec16 &Vec16::Gather<int16_t>(const int16_t *ptr, const Vec16 &pos) {
  alignas(64) int32_t x[Size()];
  pos.Store(x);
  reg_ =
      _mm512_setr_epi32(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]], ptr[x[4]], ptr[x[5]], ptr[x[6]], ptr[x[7]],
                        ptr[x[8]], ptr[x[9]], ptr[x[10]], ptr[x[11]], ptr[x[12]], ptr[x[13]], ptr[x[14]], ptr[x[15]]);
  return *this;
}

/**
 * Gathers sixteen 32-bit integers into our vector, sign extending as necessary.
 */
template <>
ALWAYS_INLINE inline Vec16 &Vec16::Gather<int32_t>(const int32_t *ptr, const Vec16 &pos) {
  reg_ = _mm512_i32gather_epi32(pos, ptr, 4);
  return *this;
}

ALWAYS_INLINE inline void Vec16::Store(int8_t *arr) const {
  const __mmask16 all(~(uint16_t)0);
  _mm512_mask_cvtepi32_storeu_epi8(reinterpret_cast<void *>(arr), all, Reg());
}

ALWAYS_INLINE inline void Vec16::Store(int16_t *arr) const {
  const __mmask16 all(~(uint16_t)0);
  _mm512_mask_cvtepi32_storeu_epi16(reinterpret_cast<void *>(arr), all, Reg());
}

ALWAYS_INLINE inline void Vec16::Store(int32_t *arr) const { Vec512b::StoreUnaligned(reinterpret_cast<void *>(arr)); }

ALWAYS_INLINE inline bool Vec16::AllBitsAtPositionsSet(const Vec16 &mask) const {
  return _mm512_testn_epi32_mask(Reg(), mask) == 0;
}

// ---------------------------------------------------------
// Vec8Mask Definition
// ---------------------------------------------------------

class Vec8Mask {
 public:
  Vec8Mask() = default;
  /**
   * Instantiates a wrapped 8-bit mask.
   * @param mask 8-bit mask to be wrapped
   */
  explicit Vec8Mask(const __mmask8 &mask) : mask_(mask) {}

  /**
   * Updates positions to contiguously contain mask's set positions, with a fixed offset added to every element.
   * @param[out] positions will contain all the set positions in the mask with offset added
   * @param offset fixed amount that will be added to every position
   * @return number of set bits in the mask
   */
  ALWAYS_INLINE uint32_t ToPositions(uint32_t *positions, uint32_t offset) const {
    __m512i sequence = _mm512_setr_epi64(0, 1, 2, 3, 4, 5, 6, 7);
    __m512i pos_vec = _mm512_add_epi64(sequence, _mm512_set1_epi64(offset));
    __m256i pos_vec_comp = _mm512_cvtepi64_epi32(pos_vec);
    _mm256_mask_compressstoreu_epi32(positions, mask_, pos_vec_comp);
    return __builtin_popcountll(mask_);
  }

  /**
   * @return mask size
   */
  static constexpr int Size() { return 8; }

  /**
   * Updates positions to contiguously contain the masked out positions in pos.
   * @param[out] positions will contain all the set positions in the mask with offset added
   * @param pos positions to be masked against
   * @return number of set bits in the mask
   */
  ALWAYS_INLINE uint32_t ToPositions(uint32_t *positions, const Vec8 &pos) const {
    __m256i pos_comp = _mm512_cvtepi64_epi32(pos);
    _mm256_mask_compressstoreu_epi32(positions, mask_, pos_comp);
    return __builtin_popcountll(mask_);
  }

  /**
   * @return true if mask is set at the given index, false otherwise
   */
  ALWAYS_INLINE bool Extract(uint32_t index) const { return (static_cast<uint32_t>(mask_) >> index) & 1; }

  /**
   * @return true if mask is set at the given index, false otherwise
   */
  ALWAYS_INLINE bool operator[](uint32_t index) const { return Extract(index); }

  /**
   * Type-cast operator so that Vec*'s can be used directly with intrinsics.
   */
  ALWAYS_INLINE operator __mmask8() const { return mask_; }

 private:
  __mmask8 mask_;
};

/**
 * Vec16Mask Definition
 */
class Vec16Mask {
 public:
  Vec16Mask() = default;
  /**
   * Instantiates a wrapped 16-bit mask.
   * @param mask 16-bit mask to be wrapped
   */
  explicit Vec16Mask(const __mmask16 &mask) : mask_(mask) {}

  /**
   * @return mask size
   */
  static constexpr int Size() { return 16; }

  /**
   * Updates positions to contiguously contain mask's set positions, with a fixed offset added to every element.
   * @param[out] positions will contain all the set positions in the mask with offset added
   * @param offset fixed amount that will be added to every position
   * @return number of set bits in the mask
   */
  ALWAYS_INLINE uint32_t ToPositions(uint32_t *positions, uint32_t offset) const {
    __m512i sequence = _mm512_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    __m512i pos_vec = _mm512_add_epi32(sequence, _mm512_set1_epi32(offset));
    _mm512_mask_compressstoreu_epi32(positions, mask_, pos_vec);
    return __builtin_popcountll(mask_);
  }

  /**
   * Updates positions to contiguously contain the masked out positions in pos.
   * @param[out] positions will contain all the set positions in the mask with offset added
   * @param pos positions to be masked against
   * @return number of set bits in the mask
   */
  ALWAYS_INLINE uint32_t ToPositions(uint32_t *positions, const Vec16 &pos) const {
    _mm512_mask_compressstoreu_epi32(positions, mask_, pos);
    return __builtin_popcountll(mask_);
  }

  /**
   * @return true if mask is set at the given index, false otherwise
   */
  bool Extract(uint32_t index) const { return static_cast<bool>((static_cast<uint32_t>(mask_) >> index) & 1); }

  /**
   * @return true if mask is set at the given index, false otherwise
   */
  bool operator[](uint32_t index) const { return Extract(index); }

  /**
   * Type-cast operator so that Vec*'s can be used directly with intrinsics.
   */
  operator __mmask16() const { return mask_; }

 private:
  __mmask16 mask_;
};

// ---------------------------------------------------------
// Vec512b Bitwise Operations
// ---------------------------------------------------------

ALWAYS_INLINE inline Vec512b operator&(const Vec512b &a, const Vec512b &b) { return Vec512b(_mm512_and_epi64(a, b)); }

ALWAYS_INLINE inline Vec512b operator|(const Vec512b &a, const Vec512b &b) { return Vec512b(_mm512_or_si512(a, b)); }

ALWAYS_INLINE inline Vec512b operator^(const Vec512b &a, const Vec512b &b) { return Vec512b(_mm512_xor_si512(a, b)); }

// ---------------------------------------------------------
// Vec8 Comparison Operations
// ---------------------------------------------------------

ALWAYS_INLINE inline Vec8Mask operator>(const Vec8 &a, const Vec8 &b) {
  return Vec8Mask(_mm512_cmpgt_epi64_mask(a, b));
}

ALWAYS_INLINE inline Vec8Mask operator==(const Vec8 &a, const Vec8 &b) {
  return Vec8Mask(_mm512_cmpeq_epi64_mask(a, b));
}

ALWAYS_INLINE inline Vec8Mask operator<(const Vec8 &a, const Vec8 &b) {
  return Vec8Mask(_mm512_cmplt_epi64_mask(a, b));
}

ALWAYS_INLINE inline Vec8Mask operator<=(const Vec8 &a, const Vec8 &b) {
  return Vec8Mask(_mm512_cmple_epi64_mask(a, b));
}

ALWAYS_INLINE inline Vec8Mask operator>=(const Vec8 &a, const Vec8 &b) {
  return Vec8Mask(_mm512_cmpge_epi64_mask(a, b));
}

ALWAYS_INLINE inline Vec8Mask operator!=(const Vec8 &a, const Vec8 &b) {
  return Vec8Mask(_mm512_cmpneq_epi64_mask(a, b));
}

// ---------------------------------------------------------
// Vec8 Arithmetic Operations
// ---------------------------------------------------------
// The following operators have to use non const references. So turn off linting.
ALWAYS_INLINE inline Vec8 operator+(const Vec8 &a, const Vec8 &b) { return Vec8(_mm512_add_epi64(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator+=(Vec8 &a, const Vec8 &b) {
  a = a + b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator-(const Vec8 &a, const Vec8 &b) { return Vec8(_mm512_sub_epi64(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator-=(Vec8 &a, const Vec8 &b) {
  a = a - b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator*(const Vec8 &a, const Vec8 &b) { return Vec8(_mm512_mullo_epi64(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator*=(Vec8 &a, const Vec8 &b) {
  a = a * b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator&(const Vec8 &a, const Vec8 &b) { return Vec8(_mm512_and_epi64(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator&=(Vec8 &a, const Vec8 &b) {
  a = a & b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator|(const Vec8 &a, const Vec8 &b) { return Vec8(_mm512_or_epi64(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator|=(Vec8 &a, const Vec8 &b) {
  a = a | b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator^(const Vec8 &a, const Vec8 &b) { return Vec8(_mm512_xor_epi64(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator^=(Vec8 &a, const Vec8 &b) {
  a = a ^ b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator>>(const Vec8 &a, const uint32_t shift) { return Vec8(_mm512_srli_epi64(a, shift)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator>>=(Vec8 &a, const uint32_t shift) {
  a = a >> shift;
  return a;
}

ALWAYS_INLINE inline Vec8 operator<<(const Vec8 &a, const uint32_t shift) { return Vec8(_mm512_slli_epi64(a, shift)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator<<=(Vec8 &a, const uint32_t shift) {
  a = a << shift;
  return a;
}

ALWAYS_INLINE inline Vec8 operator>>(const Vec8 &a, const Vec8 &b) { return Vec8(_mm512_srlv_epi64(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator>>=(Vec8 &a, const Vec8 &b) {
  a = a >> b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator<<(const Vec8 &a, const Vec8 &b) { return Vec8(_mm512_sllv_epi64(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator<<=(Vec8 &a, const Vec8 &b) {
  a = a << b;
  return a;
}

// ---------------------------------------------------------
// Vec16 Comparison Operations
// ---------------------------------------------------------

ALWAYS_INLINE inline Vec16Mask operator>(const Vec16 &a, const Vec16 &b) {
  return Vec16Mask(_mm512_cmpgt_epi32_mask(a, b));
}

ALWAYS_INLINE inline Vec16Mask operator==(const Vec16 &a, const Vec16 &b) {
  return Vec16Mask(_mm512_cmpeq_epi32_mask(a, b));
}

ALWAYS_INLINE inline Vec16Mask operator<(const Vec16 &a, const Vec16 &b) {
  return Vec16Mask(_mm512_cmplt_epi32_mask(a, b));
}

ALWAYS_INLINE inline Vec16Mask operator<=(const Vec16 &a, const Vec16 &b) {
  return Vec16Mask(_mm512_cmple_epi32_mask(a, b));
}

ALWAYS_INLINE inline Vec16Mask operator>=(const Vec16 &a, const Vec16 &b) {
  return Vec16Mask(_mm512_cmpge_epi32_mask(a, b));
}

ALWAYS_INLINE inline Vec16Mask operator!=(const Vec16 &a, const Vec16 &b) {
  return Vec16Mask(_mm512_cmpneq_epi32_mask(a, b));
}

// ---------------------------------------------------------
// Vec16 Arithmetic Operations
// ---------------------------------------------------------

ALWAYS_INLINE inline Vec16 operator+(const Vec16 &a, const Vec16 &b) { return Vec16(_mm512_add_epi32(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec16 &operator+=(Vec16 &a, const Vec16 &b) {
  a = a + b;
  return a;
}

ALWAYS_INLINE inline Vec16 operator-(const Vec16 &a, const Vec16 &b) { return Vec16(_mm512_sub_epi32(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec16 &operator-=(Vec16 &a, const Vec16 &b) {
  a = a - b;
  return a;
}

ALWAYS_INLINE inline Vec16 operator&(const Vec16 &a, const Vec16 &b) { return Vec16(_mm512_and_epi32(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec16 &operator&=(Vec16 &a, const Vec16 &b) {
  a = a & b;
  return a;
}

ALWAYS_INLINE inline Vec16 operator|(const Vec16 &a, const Vec16 &b) { return Vec16(_mm512_or_epi32(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec16 &operator|=(Vec16 &a, const Vec16 &b) {
  a = a | b;
  return a;
}

ALWAYS_INLINE inline Vec16 operator^(const Vec16 &a, const Vec16 &b) { return Vec16(_mm512_xor_epi32(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec16 &operator^=(Vec16 &a, const Vec16 &b) {
  a = a ^ b;
  return a;
}

ALWAYS_INLINE inline Vec16 operator>>(const Vec16 &a, const uint32_t shift) {
  return Vec16(_mm512_srli_epi32(a, shift));
}

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec16 &operator>>=(Vec16 &a, const uint32_t shift) {
  a = a >> shift;
  return a;
}

ALWAYS_INLINE inline Vec16 operator<<(const Vec16 &a, const uint32_t shift) {
  return Vec16(_mm512_slli_epi32(a, shift));
}

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec16 &operator<<=(Vec16 &a, const uint32_t shift) {
  a = a << shift;
  return a;
}

ALWAYS_INLINE inline Vec16 operator>>(const Vec16 &a, const Vec16 &b) { return Vec16(_mm512_srlv_epi32(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec16 &operator>>=(Vec16 &a, const Vec16 &b) {
  a = a >> b;
  return a;
}

ALWAYS_INLINE inline Vec16 operator<<(const Vec16 &a, const Vec16 &b) { return Vec16(_mm512_sllv_epi32(a, b)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec16 &operator<<=(Vec16 &a, const Vec16 &b) {
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
   * Sixteen 32-bit integer values.
   */
  using Vec = Vec16;
  /**
   * Mask for sixteen 32-bit integer values.
   */
  using VecMask = Vec16Mask;
};

/**
 * int16_t Filter
 */
template <>
struct FilterVecSizer<int16_t> {
  /**
   * Sixteen 32-bit integer values.
   */
  using Vec = Vec16;
  /**
   * Mask for sixteen 32-bit integer values.
   */
  using VecMask = Vec16Mask;
};

/**
 * int32_t Filter
 */
template <>
struct FilterVecSizer<int32_t> {
  /**
   * Sixteen 32-bit integer values.
   */
  using Vec = Vec16;
  /**
   * Mask for sixteen 32-bit integer values.
   */
  using VecMask = Vec16Mask;
};

/**
 * int64_t Filter
 */
template <>
struct FilterVecSizer<int64_t> {
  /**
   * Eight 64-bit integer values.
   */
  using Vec = Vec8;
  /**
   * Mask for eight 64-bit integer values.
   */
  using VecMask = Vec8Mask;
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
  using Vec = Vec8;
  /**
   * Mask for four 64-bit integer values.
   */
  using VecMask = Vec8Mask;
};
#endif

/**
 * Arbitrary Filter
 */
template <typename T>
struct FilterVecSizer<T, std::enable_if_t<std::is_unsigned_v<T>>> : public FilterVecSizer<std::make_signed_t<T>> {};

template <typename T, template <typename> typename Compare>
static inline uint32_t FilterVectorByVal(const T *RESTRICT in, uint32_t in_count, T val, uint32_t *RESTRICT out,
                                         const uint32_t *RESTRICT sel, uint32_t &RESTRICT in_pos) {
  using Vec = typename FilterVecSizer<T>::Vec;
  using VecMask = typename FilterVecSizer<T>::VecMask;

  const Compare cmp{};

  const Vec xval(val);

  uint32_t out_pos = 0;

  if (sel == nullptr) {
    Vec in_vec;
    for (in_pos = 0; in_pos + Vec::Size() < in_count; in_pos += Vec::Size()) {
      in_vec.Load(in + in_pos);
      VecMask mask = cmp(in_vec, xval);
      out_pos += mask.ToPositions(out + out_pos, in_pos);
    }
  } else {
    Vec in_vec, sel_vec;
    for (in_pos = 0; in_pos + Vec::Size() < in_count; in_pos += Vec::Size()) {
      sel_vec.Load(sel + in_pos);
      in_vec.Gather(in, sel_vec);
      VecMask mask = cmp(in_vec, xval);
      out_pos += mask.ToPositions(out + out_pos, sel_vec);
    }
  }

  return out_pos;
}

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
static inline uint32_t FilterVectorByVector(const T *RESTRICT in_1, const T *RESTRICT in_2, const uint32_t in_count,
                                            uint32_t *RESTRICT out, const uint32_t *RESTRICT sel,
                                            uint32_t *RESTRICT in_pos) {
  using Vec = typename FilterVecSizer<T>::Vec;
  using VecMask = typename FilterVecSizer<T>::VecMask;

  const Compare cmp;

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
