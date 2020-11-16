#pragma once

#include <cstdint>
#include <type_traits>

#include "common/macros.h"
#include "execution/util/execution_common.h"

namespace noisepage::execution::util {

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
 * For example, assume we want to encode two uint16_t types into a single uint32_t raw
 * bitfield. We would do:
 *
 * class FieldOne : public BitField32<Type1, 0, 16> {};
 * class FieldTwo : public BitField32<Type2, FieldOne::kNextBit, 16> {};
 *
 * Given a raw uint32_t bitfield, reading type one and type two:
 *
 * Type1 t1 = FieldOne::Decode(uint32_t_storage);
 * Type2 t2 = FieldTwo::Decode(uint32_t_storage);
 *
 * @tparam S The type of the primitive storage type where the bitfield is stored
 * @tparam T The type we encode into the bitfield
 * @tparam shift The number of bits to shift
 * @tparam size The size of the bitfield
 */
template <typename S, typename T, uint64_t shift, uint64_t size>
class BitFieldBase {
 public:
  /**
   * A bitfield with just the one bit set.
   */
  static constexpr const S K_ONE = static_cast<S>(1U);

  /**
   * The next available bit position in a bitfield, i.e. if you were creating another field,
   * this is the minimum amount of shift required so that the new field won't overlap with us.
   */
  static constexpr const S K_NEXT_BIT = shift + size;

  /**
   * A mask which is set for only the bits used by this field.
   */
  static constexpr const S K_MASK = ((K_ONE << size) - 1) << shift;

  /**
   * Represent the given value as a bitfield.
   * @param val the value to be encoded
   * @return the bitfield which encodes the value
   */
  ALWAYS_INLINE static constexpr S Encode(T val) { return static_cast<S>(val) << shift; }

  /**
   * Read the given bitfield as a value.
   * @param storage the bitfield to be read
   * @return the value encoded in the bitfield
   */
  ALWAYS_INLINE static constexpr T Decode(S storage) {
    // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
    if constexpr (std::is_same_v<T, bool>) {
      return static_cast<T>(storage & K_MASK);
    }
    return static_cast<T>((storage & K_MASK) >> shift);
  }

  /**
   * Create a new bitfield containing the updated value.
   * @param curr_storage the old bitfield
   * @param update the new value
   * @return the updated bitfield
   */
  ALWAYS_INLINE static constexpr S Update(S curr_storage, T update) {
    return (curr_storage & ~K_MASK) | Encode(update);
  }

  static_assert((K_NEXT_BIT - 1) / 8 < sizeof(S));
};

}  // namespace internal

template <typename T, unsigned position, unsigned size>
class BitField8 : public internal::BitFieldBase<uint8_t, T, position, size> {};

template <typename T, unsigned position, unsigned size>
class BitField16 : public internal::BitFieldBase<uint16_t, T, position, size> {};

template <typename T, unsigned position, unsigned size>
class BitField32 : public internal::BitFieldBase<uint32_t, T, position, size> {};

template <typename T, unsigned position, unsigned size>
class BitField64 : public internal::BitFieldBase<uint64_t, T, position, size> {};

}  // namespace noisepage::execution::util
