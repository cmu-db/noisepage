#pragma once

#include <immintrin.h>

#include "execution/util/common.h"
#include "execution/util/macros.h"
#include "execution/util/simd/types.h"

#ifndef SIMD_TOP_LEVEL
#error "Don't include <util/simd/avx2.h> directly; instead, include <util/simd.h>"
#endif

namespace tpl::util::simd {

#define USE_GATHER 0

/// A 256-bit SIMD register vector. This is a purely internal class that holds
/// common functions for other user-visible vector classes
class Vec256b {
 public:
  Vec256b() = default;
  explicit Vec256b(const __m256i &reg) : reg_(reg) {}

  // Type-cast operator so that Vec*'s can be used directly with intrinsics
  ALWAYS_INLINE operator __m256i() const { return reg_; }

  /// Store the contents of this vector into the provided unaligned pointer
  void Store(void *ptr) const { _mm256_storeu_si256(reinterpret_cast<__m256i *>(ptr), reg()); }

  /// Store the contents of this vector into the provided aligned pointer
  void StoreAligned(void *ptr) const { _mm256_store_si256(reinterpret_cast<__m256i *>(ptr), reg()); }

  /// Given a 256b mask, return true if all corresponding bits in this vector
  /// are set.
  bool AllBitsAtPositionsSet(const Vec256b &mask) const { return _mm256_testc_si256(reg(), mask) == 1; }

 protected:
  const __m256i &reg() const { return reg_; }

 protected:
  __m256i reg_;
};

// ---------------------------------------------------------
// Vec4 Definition
// ---------------------------------------------------------

/// A 256-bit SIMD register interpreted as 4 64-bit integer values
class Vec4 : public Vec256b {
  friend class Vec4Mask;

 public:
  Vec4() = default;
  explicit Vec4(i64 val) { reg_ = _mm256_set1_epi64x(val); }
  explicit Vec4(const __m256i &reg) : Vec256b(reg) {}
  Vec4(i64 val1, i64 val2, i64 val3, i64 val4) { reg_ = _mm256_setr_epi64x(val1, val2, val3, val4); }

  /// Return the number of elements that can be stored in this vector
  static constexpr u32 Size() { return 4; }

  /// Load four 64-bit values stored contiguously from the unaligned input
  /// pointer. The underlying data type of the array \a ptr can be either 32-bit
  /// or 64-bit integers. Up-casting is performed when appropriate.
  template <typename T>
  Vec4 &Load(const T *ptr);

  /// Gather non-contiguous elements from the input array \a ptr stored at
  /// index positions from \a pos
  template <typename T>
  Vec4 &Gather(const T *ptr, const Vec4 &pos);

  void Store(i32 *arr) const;
  void Store(i64 *arr) const;

  /// Store the contents of this vector contiguously into the input array \a ptr
  template <typename T>
  typename std::enable_if_t<std::conjunction_v<std::is_integral<T>, std::is_unsigned<T>>> Store(T *arr) const {
    using SignedType = std::make_signed_t<T>;
    Store(reinterpret_cast<SignedType *>(arr));
  }

  /// Extract the integer at the given index from this vector
  i64 Extract(u32 index) const {
    alignas(32) i64 x[Size()];
    Store(x);
    return x[index & 3];
  }

  /// Extract the integer at the given index from this vector
  i64 operator[](u32 index) const { return Extract(index); }
};

// ---------------------------------------------------------
// Vec4 Implementation
// ---------------------------------------------------------

template <typename T>
ALWAYS_INLINE inline Vec4 &Vec4::Load(const T *ptr) {
  using signed_t = std::make_signed_t<T>;
  return Load<signed_t>(reinterpret_cast<const signed_t *>(ptr));
}

template <>
ALWAYS_INLINE inline Vec4 &Vec4::Load<i32>(const i32 *ptr) {
  auto tmp = _mm_loadu_si128((const __m128i *)ptr);
  reg_ = _mm256_cvtepi32_epi64(tmp);
  return *this;
}

template <>
ALWAYS_INLINE inline Vec4 &Vec4::Load<i64>(const i64 *ptr) {
  // Load aligned and unaligned have almost no performance different on AVX2
  // machines. To alleviate some pain from clients having to know this info
  // we always use an unaligned load.
  reg_ = _mm256_loadu_si256((const __m256i *)ptr);
  return *this;
}

template <typename T>
ALWAYS_INLINE inline Vec4 &Vec4::Gather(const T *ptr, const Vec4 &pos) {
  using signed_t = std::make_signed_t<T>;
  return Gather<signed_t>(reinterpret_cast<const signed_t *>(ptr), pos);
}

template <>
ALWAYS_INLINE inline Vec4 &Vec4::Gather<i64>(const i64 *ptr, const Vec4 &pos) {
#if USE_GATHER == 1
  reg_ = _mm256_i64gather_epi64(ptr, pos, 8);
#else
  alignas(32) i64 x[Size()];
  pos.Store(x);
  reg_ = _mm256_setr_epi64x(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]]);
#endif
  return *this;
}

// Truncate the four 64-bit values in this vector and store them into the first
// four elements of the provided array
ALWAYS_INLINE inline void Vec4::Store(i32 *arr) const {
  auto truncated = _mm256_cvtepi64_epi32(reg());
  _mm_store_si128(reinterpret_cast<__m128i *>(arr), truncated);
}

ALWAYS_INLINE inline void Vec4::Store(i64 *arr) const { Vec256b::Store(reinterpret_cast<void *>(arr)); }

// ---------------------------------------------------------
// Vec8 Definition
// ---------------------------------------------------------

/// A 256-bit SIMD register interpreted as 8 32-bit integer values
class Vec8 : public Vec256b {
 public:
  // The below constructors are not explicit on purpose
  Vec8() = default;
  explicit Vec8(i32 val) { reg_ = _mm256_set1_epi32(val); }
  explicit Vec8(const __m256i &reg) : Vec256b(reg) {}
  Vec8(i32 val1, i32 val2, i32 val3, i32 val4, i32 val5, i32 val6, i32 val7, i32 val8) {
    reg_ = _mm256_setr_epi32(val1, val2, val3, val4, val5, val6, val7, val8);
  }

  /// Load 8 32-bit values stored contiguously from the input pointer \a ptr.
  /// The underlying data type of the array \a ptr can be either 8-bit, 16-bit,
  /// or 32-bit integers. Up-casting is performed when appropriate.
  template <typename T>
  Vec8 &Load(const T *ptr);

  /// Gather non-contiguous elements from the input array \a ptr stored at
  /// index positions from \a pos
  /// \tparam T The data type of the underlying array
  /// \param ptr The input array
  /// \param pos The list of positions in the input array to gather
  template <typename T>
  Vec8 &Gather(const T *ptr, const Vec8 &pos);

  void Store(i32 *arr) const;

  /// Store the contents of this vector contiguously into the input array \a ptr
  template <typename T>
  typename std::enable_if_t<std::conjunction_v<std::is_integral<T>, std::is_unsigned<T>>> Store(T *arr) const {
    using SignedType = std::make_signed_t<T>;
    Store(reinterpret_cast<SignedType *>(arr));
  }

  /// Return the number of elements that can be stored in this vector
  static constexpr u32 Size() { return 8; }

  ALWAYS_INLINE i32 Extract(u32 index) const {
    TPL_ASSERT(index < 8, "Out-of-bounds mask element access");
    alignas(32) i32 x[Size()];
    Store(x);
    return x[index & 7];
  }

  ALWAYS_INLINE i32 operator[](u32 index) const { return Extract(index); }
};

// ---------------------------------------------------------
// Vec8 Implementation
// ---------------------------------------------------------

// Vec8's can be loaded from any array whose element types are smaller than or
// equal to 32-bits. Eight elements are always read from the input array, but
// are up-casted to 32-bits when appropriate.

template <typename T>
ALWAYS_INLINE inline Vec8 &Vec8::Load(const T *ptr) {
  using signed_t = std::make_signed_t<T>;
  return Load<signed_t>(reinterpret_cast<const signed_t *>(ptr));
}

template <>
ALWAYS_INLINE inline Vec8 &Vec8::Load<i8>(const i8 *ptr) {
  auto tmp = _mm_loadu_si128((const __m128i *)ptr);
  reg_ = _mm256_cvtepi8_epi32(tmp);
  return *this;
}

template <>
ALWAYS_INLINE inline Vec8 &Vec8::Load<i16>(const i16 *ptr) {
  auto tmp = _mm_loadu_si128((const __m128i *)ptr);
  reg_ = _mm256_cvtepi16_epi32(tmp);
  return *this;
}

template <>
ALWAYS_INLINE inline Vec8 &Vec8::Load<i32>(const i32 *ptr) {
  reg_ = _mm256_loadu_si256((const __m256i *)ptr);
  return *this;
}

// Like loads, Vec8's can be gathered from any array whose element types are
// smaller than or equal to 32-bits. The input position vector must have eight
// indexes from the input array; eight array elements are always loaded and the
// elements are up-casted when appropriate.

template <typename T>
ALWAYS_INLINE inline Vec8 &Vec8::Gather(const T *ptr, const Vec8 &pos) {
  using signed_t = std::make_signed_t<T>;
  return Gather<signed_t>(reinterpret_cast<const signed_t *>(ptr), pos);
}

template <>
ALWAYS_INLINE inline Vec8 &Vec8::Gather<i8>(const i8 *ptr, const Vec8 &pos) {
#if USE_GATHER == 1
  reg_ = _mm256_i32gather_epi32(ptr, pos, 1);
  reg_ = _mm256_srai_epi32(reg_, 24);
#else
  alignas(32) i32 x[Size()];
  pos.Store(x);
  reg_ = _mm256_setr_epi32(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]], ptr[x[4]], ptr[x[5]], ptr[x[6]], ptr[x[7]]);
#endif
  return *this;
}

template <>
ALWAYS_INLINE inline Vec8 &Vec8::Gather<i16>(const i16 *ptr, const Vec8 &pos) {
#if USE_GATHER == 1
  reg_ = _mm256_i32gather_epi32(ptr, pos, 2);
  reg_ = _mm256_srai_epi32(reg_, 16);
#else
  alignas(32) i32 x[Size()];
  pos.Store(x);
  reg_ = _mm256_setr_epi32(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]], ptr[x[4]], ptr[x[5]], ptr[x[6]], ptr[x[7]]);
#endif
  return *this;
}

template <>
ALWAYS_INLINE inline Vec8 &Vec8::Gather<i32>(const i32 *ptr, const Vec8 &pos) {
#if USE_GATHER == 1
  reg_ = _mm256_i32gather_epi32(ptr, pos, 4);
#else
  alignas(32) i32 x[Size()];
  pos.Store(x);
  reg_ = _mm256_setr_epi32(ptr[x[0]], ptr[x[1]], ptr[x[2]], ptr[x[3]], ptr[x[4]], ptr[x[5]], ptr[x[6]], ptr[x[7]]);
#endif
  return *this;
}

ALWAYS_INLINE inline void Vec8::Store(i32 *arr) const { Vec256b::Store(reinterpret_cast<void *>(arr)); }

// --------------------------------------------------------
// Vec8Mask Definition
// --------------------------------------------------------

/// An 8-bit mask stored logically as 1-bit in each of the eight 32-bit lanes in
/// a 256-bit register
class Vec8Mask : public Vec8 {
 public:
  Vec8Mask() = default;
  explicit Vec8Mask(const __m256i &reg) : Vec8(reg) {}

  /// Extract the value of the bit at index \a index in this mask
  bool Extract(u32 idx) const { return Vec8::Extract(idx) != 0; }

  /// Extract the value of the bit at index \a index in this mask
  bool operator[](u32 idx) const { return Extract(idx); }

  u32 ToPositions(u32 *positions, u32 offset) const;

  u32 ToPositions(u32 *positions, const Vec8 &pos) const;
};

// ---------------------------------------------------------
// Vec8Mask Implementation
// ---------------------------------------------------------

ALWAYS_INLINE inline u32 Vec8Mask::ToPositions(u32 *positions, u32 offset) const {
  i32 mask = _mm256_movemask_ps(_mm256_castsi256_ps(reg()));
  TPL_ASSERT(mask < 256, "8-bit mask must be less than 256");
  __m128i match_pos_scaled = _mm_loadl_epi64(reinterpret_cast<const __m128i *>(&k8BitMatchLUT[mask]));
  __m256i match_pos = _mm256_cvtepi8_epi32(match_pos_scaled);
  __m256i pos_vec = _mm256_add_epi32(_mm256_set1_epi32(offset), match_pos);
  _mm256_storeu_si256(reinterpret_cast<__m256i *>(positions), pos_vec);
  return __builtin_popcount(mask);
}

ALWAYS_INLINE inline u32 Vec8Mask::ToPositions(u32 *positions, const tpl::util::simd::Vec8 &pos) const {
  i32 mask = _mm256_movemask_ps(_mm256_castsi256_ps(reg()));
  TPL_ASSERT(mask < 256, "8-bit mask must be less than 256");
  __m128i perm_comp = _mm_loadl_epi64(reinterpret_cast<const __m128i *>(&k8BitMatchLUT[mask]));
  __m256i perm = _mm256_cvtepi8_epi32(perm_comp);
  __m256i perm_pos = _mm256_permutevar8x32_epi32(pos, perm);
  __m256i perm_mask = _mm256_permutevar8x32_epi32(reg(), perm);
  _mm256_maskstore_epi32(reinterpret_cast<i32 *>(positions), perm_mask, perm_pos);
  return __builtin_popcount(mask);
}

// ---------------------------------------------------------
// Vec4Mask Definition
// ---------------------------------------------------------

class Vec4Mask : public Vec4 {
 public:
  Vec4Mask() = default;
  explicit Vec4Mask(const __m256i &reg) : Vec4(reg) {}

  i32 Extract(u32 index) const { return Vec4::Extract(index) != 0; }

  i32 operator[](u32 index) const { return Extract(index); }

  ALWAYS_INLINE inline u32 ToPositions(u32 *positions, u32 offset) const {
    i32 mask = _mm256_movemask_pd(_mm256_castsi256_pd(reg()));
    TPL_ASSERT(mask < 16, "4-bit mask must be less than 16");
    __m128i match_pos_scaled = _mm_loadl_epi64(reinterpret_cast<__m128i *>(const_cast<u64 *>(&k4BitMatchLUT[mask])));
    __m128i match_pos = _mm_cvtepi16_epi32(match_pos_scaled);
    __m128i pos_vec = _mm_add_epi32(_mm_set1_epi32(offset), match_pos);
    _mm_storeu_si128(reinterpret_cast<__m128i *>(positions), pos_vec);
    return __builtin_popcount(mask);
  }

  ALWAYS_INLINE inline u32 ToPositions(u32 *positions, const Vec4 &pos) const {
    i32 mask = _mm256_movemask_pd(_mm256_castsi256_pd(reg()));
    TPL_ASSERT(mask < 16, "4-bit mask must be less than 16");

    // TODO(pmenon): Fix this slowness!
    {
      alignas(32) i64 m_arr[Size()];
      alignas(32) i64 p_arr[Size()];
      Store(m_arr);
      pos.Store(p_arr);
      for (u32 idx = 0, i = 0; i < 4; i++) {
        positions[idx] = static_cast<u32>(p_arr[i]);
        idx += (m_arr[i] != 0);
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

ALWAYS_INLINE inline Vec4 operator+(const Vec4 &a, const Vec4 &b) { return Vec4(_mm256_add_epi64(a, b)); }

ALWAYS_INLINE inline Vec4 &operator+=(Vec4 &a, const Vec4 &b) {
  a = a + b;
  return a;
}

ALWAYS_INLINE inline Vec4 operator-(const Vec4 &a, const Vec4 &b) { return Vec4(_mm256_sub_epi64(a, b)); }

ALWAYS_INLINE inline Vec4 &operator-=(Vec4 &a, const Vec4 &b) {
  a = a - b;
  return a;
}

ALWAYS_INLINE inline Vec4 operator&(const Vec4 &a, const Vec4 &b) { return Vec4(Vec256b(a) & Vec256b(b)); }

ALWAYS_INLINE inline Vec4 &operator&=(Vec4 &a, const Vec4 &b) {
  a = a & b;
  return a;
}

ALWAYS_INLINE inline Vec4 operator|(const Vec4 &a, const Vec4 &b) { return Vec4(Vec256b(a) | Vec256b(b)); }

ALWAYS_INLINE inline Vec4 &operator|=(Vec4 &a, const Vec4 &b) {
  a = a | b;
  return a;
}

ALWAYS_INLINE inline Vec4 operator^(const Vec4 &a, const Vec4 &b) { return Vec4(Vec256b(a) ^ Vec256b(b)); }

ALWAYS_INLINE inline Vec4 &operator^=(Vec4 &a, const Vec4 &b) {
  a = a ^ b;
  return a;
}

ALWAYS_INLINE inline Vec4 operator>>(const Vec4 &a, const u32 shift) { return Vec4(_mm256_srli_epi64(a, shift)); }

ALWAYS_INLINE inline Vec4 &operator>>=(Vec4 &a, const u32 shift) {
  a = a >> shift;
  return a;
}

ALWAYS_INLINE inline Vec4 operator<<(const Vec4 &a, const u32 shift) { return Vec4(_mm256_slli_epi64(a, shift)); }

ALWAYS_INLINE inline Vec4 &operator<<=(Vec4 &a, const u32 shift) {
  a = a << shift;
  return a;
}

ALWAYS_INLINE inline Vec4 operator>>(const Vec4 &a, const Vec4 &b) { return Vec4(_mm256_srlv_epi64(a, b)); }

ALWAYS_INLINE inline Vec4 &operator>>=(Vec4 &a, const Vec4 &b) {
  a = a >> b;
  return a;
}

ALWAYS_INLINE inline Vec4 operator<<(const Vec4 &a, const Vec4 &b) { return Vec4(_mm256_sllv_epi64(a, b)); }

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

ALWAYS_INLINE inline Vec8 &operator+=(Vec8 &a, const Vec8 &b) {
  a = a + b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator-(const Vec8 &a, const Vec8 &b) { return Vec8(_mm256_sub_epi32(a, b)); }

ALWAYS_INLINE inline Vec8 &operator-=(Vec8 &a, const Vec8 &b) {
  a = a - b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator*(const Vec8 &a, const Vec8 &b) { return Vec8(_mm256_mullo_epi32(a, b)); }

ALWAYS_INLINE inline Vec8 &operator*=(Vec8 &a, const Vec8 &b) {
  a = a * b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator&(const Vec8 &a, const Vec8 &b) { return Vec8(Vec256b(a) & Vec256b(b)); }

ALWAYS_INLINE inline Vec8 &operator&=(Vec8 &a, const Vec8 &b) {
  a = a & b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator|(const Vec8 &a, const Vec8 &b) { return Vec8(Vec256b(a) | Vec256b(b)); }

ALWAYS_INLINE inline Vec8 &operator|=(Vec8 &a, const Vec8 &b) {
  a = a | b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator^(const Vec8 &a, const Vec8 &b) { return Vec8(Vec256b(a) ^ Vec256b(b)); }

ALWAYS_INLINE inline Vec8 &operator^=(Vec8 &a, const Vec8 &b) {
  a = a ^ b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator>>(const Vec8 &a, const u32 shift) { return Vec8(_mm256_srli_epi32(a, shift)); }

ALWAYS_INLINE inline Vec8 &operator>>=(Vec8 &a, const u32 shift) {
  a = a >> shift;
  return a;
}

ALWAYS_INLINE inline Vec8 operator<<(const Vec8 &a, const u32 shift) { return Vec8(_mm256_slli_epi32(a, shift)); }

ALWAYS_INLINE inline Vec8 &operator<<=(Vec8 &a, const u32 shift) {
  a = a << shift;
  return a;
}

ALWAYS_INLINE inline Vec8 operator>>(const Vec8 &a, const Vec8 &b) { return Vec8(_mm256_srlv_epi32(a, b)); }

ALWAYS_INLINE inline Vec8 &operator>>=(Vec8 &a, const Vec8 &b) {
  a = a >> b;
  return a;
}

ALWAYS_INLINE inline Vec8 operator<<(const Vec8 &a, const Vec8 &b) { return Vec8(_mm256_sllv_epi32(a, b)); }

ALWAYS_INLINE inline Vec8 &operator<<=(Vec8 &a, const Vec8 &b) {
  a = a << b;
  return a;
}

// ---------------------------------------------------------
// Filter
// ---------------------------------------------------------

template <typename T, typename Enable = void>
struct FilterVecSizer;

template <>
struct FilterVecSizer<i8> {
  using Vec = Vec8;
  using VecMask = Vec8Mask;
};

template <>
struct FilterVecSizer<i16> {
  using Vec = Vec8;
  using VecMask = Vec8Mask;
};

template <>
struct FilterVecSizer<i32> {
  using Vec = Vec8;
  using VecMask = Vec8Mask;
};

template <>
struct FilterVecSizer<i64> {
  using Vec = Vec4;
  using VecMask = Vec4Mask;
};

template <typename T>
struct FilterVecSizer<T, std::enable_if_t<std::is_unsigned_v<T>>> : public FilterVecSizer<std::make_signed_t<T>> {};

template <typename T, template <typename> typename Compare>
static inline u32 FilterVectorByVal(const T *RESTRICT in, u32 in_count, T val, u32 *RESTRICT out,
                                    const u32 *RESTRICT sel, u32 &RESTRICT in_pos) {
  using Vec = typename FilterVecSizer<T>::Vec;
  using VecMask = typename FilterVecSizer<T>::VecMask;

  const Compare cmp;

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

}  // namespace tpl::util::simd
