#pragma once

#include <cstdint>
#include <type_traits>

#include "execution/util/common.h"
#include "execution/util/macros.h"

namespace tpl::util {

namespace internal {

/**
 * Bitfield class for encoding/decoding values of type T into/from a storage
 * space of type S. The encoded version of type T occupies positions
 * [shift, shift + size] in the underlying storage value.
 *
 * Multiple bitfields can be applied onto the same underlying storage so long as
 * they occupy disjoint sets of bits. To determine the next available bit
 * position in a bitfield, use the BitFieldBase::kNextBit value.
 *
 * To use, create a subclass of the desired bitfield size (i.e., BitField32, not
 * the base class!), and specify the encoding type and bit-range where you want
 * to encode the values. Then, use the Encode/Decode/Update values to modify the
 * underlying storage.
 *
 * For example, assume we want to encode two u16 types into a single u32 raw
 * bitfield. We would do:
 *
 * class FieldOne : public BitField32<Type1, 0, 16> {};
 * class FieldTwo : public BitField32<Type2, FieldOne::kNextBit, 16> {};
 *
 * Given a raw u32 bitfield, reading type one and type two:
 *
 * Type1 t1 = FieldOne::Decode(u32_storage);
 * Type2 t2 = FieldTwo::Decode(u32_storage);
 *
 * @tparam S The type of the primitive storage type where the bitfield is stored
 * @tparam T The type we encode into the bitfield
 * @tparam shift The number of bits to shift
 * @tparam size The size of the bitfield
 */
template <typename S, typename T, u64 shift, u64 size>
class BitFieldBase {
 public:
  static constexpr const S kOne = static_cast<S>(1U);

  static constexpr const S kNextBit = shift + size;

  static constexpr const S kMask = ((kOne << size) - 1) << shift;

  ALWAYS_INLINE static constexpr S Encode(T val) {
    return static_cast<S>(val) << shift;
  }

  ALWAYS_INLINE static constexpr T Decode(S storage) {
    if constexpr (std::is_same_v<T, bool>) {
      return static_cast<T>(storage & kMask);
    }
    return static_cast<T>((storage & kMask) >> shift);
  }

  ALWAYS_INLINE static constexpr S Update(S curr_storage, T update) {
    return (curr_storage & ~kMask) | Encode(update);
  }

  static_assert((kNextBit - 1) / 8 < sizeof(S));
};

}  // namespace internal

template <typename T, unsigned position, unsigned size>
class BitField8 : public internal::BitFieldBase<u8, T, position, size> {};

template <typename T, unsigned position, unsigned size>
class BitField16 : public internal::BitFieldBase<u16, T, position, size> {};

template <typename T, unsigned position, unsigned size>
class BitField32 : public internal::BitFieldBase<u32, T, position, size> {};

template <typename T, unsigned position, unsigned size>
class BitField64 : public internal::BitFieldBase<u64, T, position, size> {};

}  // namespace tpl::util
