#pragma once

#include <immintrin.h>

#include "execution/util/common.h"
#include "execution/util/macros.h"
#include "execution/util/simd/types.h"

namespace terrier::execution::util::simd {

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
  ALWAYS_INLINE void StoreUnaligned(void *ptr) const { _mm512_storeu_si512(ptr, reg()); }

  /**
   * Store the contents of this vector into the provided aligned pointer.
   */
  ALWAYS_INLINE void StoreAligned(void *ptr) const { _mm512_store_si512(ptr, reg()); }

 protected:
  /**
   * @return the underlying register
   */
  const __m512i &reg() const { return reg_; }

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
  explicit Vec8(i64 val) { reg_ = _mm512_set1_epi64(val); }
  /**
   * Create a vector whose contents are the 512-bit register reg.
   * @param reg initial contents of the vector
   */
  explicit Vec8(const __m512i &reg) : Vec512b(reg) {}
  /**
   * Create a vector containing the 8 integers in order MSB [val8, val7, ..., val1] LSB.
   */
  Vec8(i64 val1, i64 val2, i64 val3, i64 val4, i64 val5, i64 val6, i64 val7, i64 val8) {
    reg_ = _mm512_setr_epi64(val1, val2, val3, val4, val5, val6, val7, val8);
  }

  /**
   * @return number of elements that can be stored in this vector
   */
  static constexpr u32 Size() { return 8; }

  /**
   * Loads 8 elements into the array, sign-extending each element to be 64-bit if necessary.
   * @tparam T integral type of size (32, 64) bits
   * @param ptr pointer to bits to be loaded
   * @return our updated vector
   */
  template <typename T>
  Vec8 &Load(const T *ptr);

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
   * Truncates our eight 64-bit elements into 8-bit elements and stores them into arr.
   */
  void Store(i8 *arr) const;
  /**
   * Truncates our eight 64-bit elements into 16-bit elements and stores them into arr.
   */
  void Store(i16 *arr) const;
  /**
   * Truncates our eight 64-bit elements into 32-bit elements and stores them into arr.
   */
  void Store(i32 *arr) const;
  /**
   * Stores our eight 64-bit elements into arr.
   */
  void Store(i64 *arr) const;

  /**
   * Stores our eight 64-bit elements into arr, truncating if necessary.
   */
  template <typename T>
  typename std::enable_if_t<std::conjunction_v<std::is_integral<T>, std::is_unsigned<T>>> Store(T *arr) const {
    using SignedType = std::make_signed_t<T>;
    Store(reinterpret_cast<SignedType *>(arr));
  }

  /**
   * @return true if all the corresponding masked bits in this vector are set
   */
  bool AllBitsAtPositionsSet(const Vec8 &mask) const;

  /**
   * @return the element at the given index
   */
  i64 Extract(u32 index) const {
    TPL_ASSERT(index < 8, "Out-of-bounds mask element access");
    alignas(64) i64 x[Size()];
    Store(x);
    return x[index & 7];
  }

  /**
   * @return the element at the given index
   */
  i64 operator[](u32 index) const { return Extract(index); }
};

// ---------------------------------------------------------
// Vec8 Implementation
// ---------------------------------------------------------

/**
 * Loads 8 elements into the array, sign-extending each element to be 64-bit if necessary.
 * @tparam T integral type of size (32, 64) bits
 * @param ptr pointer to bits to be loaded
 * @return our updated vector
 */
template <typename T>
ALWAYS_INLINE inline Vec8 &Vec8::Load(const T *ptr) {
  using signed_t = std::make_signed_t<T>;
  return Load<signed_t>(reinterpret_cast<const signed_t *>(ptr));
}

/**
 * Loads 256 bits from ptr as eight 32-bit integers, each 32-bit integer is then sign extended to be 64-bit.
 */
template <>
ALWAYS_INLINE inline Vec8 &Vec8::Load<i32>(const i32 *ptr) {
  auto tmp = _mm256_load_si256(reinterpret_cast<const __m256i *>(ptr));
  reg_ = _mm512_cvtepi32_epi64(tmp);
  return *this;
}

/**
 * Loads 512 bits from ptr as eight 64-bit integers.
 */
template <>
ALWAYS_INLINE inline Vec8 &Vec8::Load<i64>(const i64 *ptr) {
  reg_ = _mm512_loadu_si512((const __m512i *)ptr);
  return *this;
}

template <typename T>
ALWAYS_INLINE inline Vec8 &Vec8::Gather(const T *ptr, const Vec8 &pos) {
#if USE_GATHER
  reg_ = _mm512_i64gather_epi64(pos, ptr, 8);
#else
  alignas(64) i64 x[Size()];
  pos.Store(x);
  reg_ = _mm512_setr_epi64(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]], ptr[x[4]], ptr[x[5]], ptr[x[6]], ptr[x[7]]);
#endif
  return *this;
}

ALWAYS_INLINE inline void Vec8::Store(i8 *arr) const {
  const __mmask8 all(~(u8)0);
  _mm512_mask_cvtepi64_storeu_epi8(reinterpret_cast<void *>(arr), all, reg());
}

ALWAYS_INLINE inline void Vec8::Store(i16 *arr) const {
  const __mmask8 all(~(u8)0);
  _mm512_mask_cvtepi64_storeu_epi16(reinterpret_cast<void *>(arr), all, reg());
}

ALWAYS_INLINE inline void Vec8::Store(i32 *arr) const {
  const __mmask8 all(~(u8)0);
  _mm512_mask_cvtepi64_storeu_epi32(reinterpret_cast<void *>(arr), all, reg());
}

ALWAYS_INLINE inline void Vec8::Store(i64 *arr) const { Vec512b::StoreUnaligned(reinterpret_cast<void *>(arr)); }

ALWAYS_INLINE inline bool Vec8::AllBitsAtPositionsSet(const Vec8 &mask) const {
  return _mm512_testn_epi64_mask(reg(), mask) == 0;
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
  explicit Vec16(i32 val) { reg_ = _mm512_set1_epi32(val); }
  /**
   * Create a vector whose contents are the 512-bit register reg.
   * @param reg initial contents of the vector
   */
  explicit Vec16(const __m512i &reg) : Vec512b(reg) {}
  /**
   * Create a vector containing the 16 integers in order MSB [val16, val15, ..., val1] LSB.
   */
  Vec16(i32 val1, i32 val2, i32 val3, i32 val4, i32 val5, i32 val6, i32 val7, i32 val8, i32 val9, i32 val10, i32 val11,
        i32 val12, i32 val13, i32 val14, i32 val15, i32 val16) {
    reg_ = _mm512_setr_epi32(val1, val2, val3, val4, val5, val6, val7, val8, val9, val10, val11, val12, val13, val14,
                             val15, val16);
  }

  /**
   * @return number of elements that can be stored in this vector
   */
  ALWAYS_INLINE static constexpr int Size() { return 16; }

  /**
   * Loads 16 elements into the array, sign-extending each element to be 32-bit if necessary.
   * @tparam T integral type of size (8, 16, 32) bits
   * @param ptr pointer to bits to be loaded
   * @return our updated vector
   */
  template <typename T>
  Vec16 &Load(const T *ptr);

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
   * Truncates our sixteen 32-bit elements into 8-bit elements and stores them into arr.
   */
  void Store(i8 *arr) const;
  /**
   * Truncates our sixteen 32-bit elements into 16-bit elements and stores them into arr.
   */
  void Store(i16 *arr) const;
  /**
   * Stores our sixteen 32-bit elements into arr.
   */
  void Store(i32 *arr) const;

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
  i32 Extract(u32 index) const {
    alignas(64) i32 x[Size()];
    Store(x);
    return x[index & 15];
  }

  /**
   * @return the element at the given index
   */
  i32 operator[](u32 index) const { return Extract(index); }
};

// ---------------------------------------------------------
// Vec16 Implementation
// ---------------------------------------------------------

/**
 * Loads 16 elements into the array, sign-extending each element to be 32-bit if necessary.
 * @tparam T integral type of size (8, 16, 32) bits
 * @param ptr pointer to bits to be loaded
 * @return our updated vector
 */
template <typename T>
ALWAYS_INLINE inline Vec16 &Vec16::Load(const T *ptr) {
  using signed_t = std::make_signed_t<T>;
  return Load<signed_t>(reinterpret_cast<const signed_t *>(ptr));
}

/**
 * Loads 128 bits from ptr as sixteen 8-bit integers, each 8-bit integer is then sign extended to be 32-bit.
 */
template <>
ALWAYS_INLINE inline Vec16 &Vec16::Load<i8>(const i8 *ptr) {
  auto tmp = _mm_loadu_si128(reinterpret_cast<const __m128i *>(ptr));
  reg_ = _mm512_cvtepi8_epi32(tmp);
  return *this;
}

/**
 * Loads 256 bits from ptr as sixteen 16-bit integers, each 16-bit integer is then sign extended to be 32-bit.
 */
template <>
ALWAYS_INLINE inline Vec16 &Vec16::Load<i16>(const i16 *ptr) {
  auto tmp = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(ptr));
  reg_ = _mm512_cvtepi16_epi32(tmp);
  return *this;
}

/**
 * Loads 512 bits from ptr.
 */
template <>
ALWAYS_INLINE inline Vec16 &Vec16::Load<i32>(const i32 *ptr) {
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
ALWAYS_INLINE inline Vec16 &Vec16::Gather<i8>(const i8 *ptr, const Vec16 &pos) {
  alignas(64) i32 x[Size()];
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
ALWAYS_INLINE inline Vec16 &Vec16::Gather<i16>(const i16 *ptr, const Vec16 &pos) {
  alignas(64) i32 x[Size()];
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
ALWAYS_INLINE inline Vec16 &Vec16::Gather<i32>(const i32 *ptr, const Vec16 &pos) {
  reg_ = _mm512_i32gather_epi32(pos, ptr, 4);
  return *this;
}

ALWAYS_INLINE inline void Vec16::Store(i8 *arr) const {
  const __mmask16 all(~(u16)0);
  _mm512_mask_cvtepi32_storeu_epi8(reinterpret_cast<void *>(arr), all, reg());
}

ALWAYS_INLINE inline void Vec16::Store(i16 *arr) const {
  const __mmask16 all(~(u16)0);
  _mm512_mask_cvtepi32_storeu_epi16(reinterpret_cast<void *>(arr), all, reg());
}

ALWAYS_INLINE inline void Vec16::Store(i32 *arr) const { Vec512b::StoreUnaligned(reinterpret_cast<void *>(arr)); }

ALWAYS_INLINE inline bool Vec16::AllBitsAtPositionsSet(const Vec16 &mask) const {
  return _mm512_testn_epi32_mask(reg(), mask) == 0;
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
  ALWAYS_INLINE u32 ToPositions(u32 *positions, u32 offset) const {
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
  ALWAYS_INLINE u32 ToPositions(u32 *positions, const Vec8 &pos) const {
    __m256i pos_comp = _mm512_cvtepi64_epi32(pos);
    _mm256_mask_compressstoreu_epi32(positions, mask_, pos_comp);
    return __builtin_popcountll(mask_);
  }

  /**
   * @return true if mask is set at the given index, false otherwise
   */
  ALWAYS_INLINE bool Extract(u32 index) const { return (static_cast<u32>(mask_) >> index) & 1; }

  /**
   * @return true if mask is set at the given index, false otherwise
   */
  ALWAYS_INLINE bool operator[](u32 index) const { return Extract(index); }

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
  ALWAYS_INLINE u32 ToPositions(u32 *positions, u32 offset) const {
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
  ALWAYS_INLINE u32 ToPositions(u32 *positions, const Vec16 &pos) const {
    _mm512_mask_compressstoreu_epi32(positions, mask_, pos);
    return __builtin_popcountll(mask_);
  }

  /**
   * @return true if mask is set at the given index, false otherwise
   */
  bool Extract(u32 index) const { return static_cast<bool>((static_cast<u32>(mask_) >> index) & 1); }

  /**
   * @return true if mask is set at the given index, false otherwise
   */
  bool operator[](u32 index) const { return Extract(index); }

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

ALWAYS_INLINE inline Vec8 operator>>(const Vec8 &a, const u32 shift) { return Vec8(_mm512_srli_epi64(a, shift)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator>>=(Vec8 &a, const u32 shift) {
  a = a >> shift;
  return a;
}

ALWAYS_INLINE inline Vec8 operator<<(const Vec8 &a, const u32 shift) { return Vec8(_mm512_slli_epi64(a, shift)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec8 &operator<<=(Vec8 &a, const u32 shift) {
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

ALWAYS_INLINE inline Vec16 operator>>(const Vec16 &a, const u32 shift) { return Vec16(_mm512_srli_epi32(a, shift)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec16 &operator>>=(Vec16 &a, const u32 shift) {
  a = a >> shift;
  return a;
}

ALWAYS_INLINE inline Vec16 operator<<(const Vec16 &a, const u32 shift) { return Vec16(_mm512_slli_epi32(a, shift)); }

// NOLINTNEXTLINE
ALWAYS_INLINE inline Vec16 &operator<<=(Vec16 &a, const u32 shift) {
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
 * i8 Filter
 */
template <>
struct FilterVecSizer<i8> {
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
 * i16 Filter
 */
template <>
struct FilterVecSizer<i16> {
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
 * i32 Filter
 */
template <>
struct FilterVecSizer<i32> {
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
 * i64 Filter
 */
template <>
struct FilterVecSizer<i64> {
  /**
   * Eight 64-bit integer values.
   */
  using Vec = Vec8;
  /**
   * Mask for eight 64-bit integer values.
   */
  using VecMask = Vec8Mask;
};

/**
 * Arbitrary Filter
 */
template <typename T>
struct FilterVecSizer<T, std::enable_if_t<std::is_unsigned_v<T>>> : public FilterVecSizer<std::make_signed_t<T>> {};

template <typename T, template <typename> typename Compare>
static inline u32 FilterVectorByVal(const T *RESTRICT in, u32 in_count, T val, u32 *RESTRICT out,
                                    const u32 *RESTRICT sel, u32 &RESTRICT in_pos) {
  using Vec = typename FilterVecSizer<T>::Vec;
  using VecMask = typename FilterVecSizer<T>::VecMask;

  const Compare cmp{};

  const Vec xval(val);

  u32 out_pos = 0;

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
static inline u32 FilterVectorByVal(const T *RESTRICT in, u32 in_count, T val, u32 *RESTRICT out,
                                    const u32 *RESTRICT sel, u32 *RESTRICT in_pos) {
  using Vec = typename FilterVecSizer<T>::Vec;
  using VecMask = typename FilterVecSizer<T>::VecMask;

  const Compare cmp{};

  const Vec xval(val);

  u32 out_pos = 0;

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
static inline u32 FilterVectorByVector(const T *RESTRICT in_1, const T *RESTRICT in_2, const u32 in_count,
                                       u32 *RESTRICT out, const u32 *RESTRICT sel, u32 *RESTRICT in_pos) {
  using Vec = typename FilterVecSizer<T>::Vec;
  using VecMask = typename FilterVecSizer<T>::VecMask;

  const Compare cmp;

  u32 out_pos = 0;

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

}  // namespace terrier::execution::util::simd
