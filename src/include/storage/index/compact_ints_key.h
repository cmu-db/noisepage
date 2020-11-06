#pragma once

#include <cstring>
#include <functional>
#include <vector>

#include "portable_endian/portable_endian.h"
#include "storage/index/index_metadata.h"
#include "storage/projected_row.h"
#include "xxHash/xxh3.h"

namespace noisepage::storage::index {

// This is the maximum number of bytes to pack into a single CompactIntsKey template. This constraint is not
// arbitrary and cannot be increased beyond 32 bytes (256 bits) until AVX-512 is more widely available.
constexpr uint8_t COMPACTINTSKEY_MAX_SIZE = 32;

/**
 * CompactIntsKey - Compact representation of multifield integers
 *
 * This class is used for storing multiple integral fields into a compact
 * array representation. This class is largely used as a static object,
 * because special storage format is used to ensure a fast comparison
 * implementation.
 *
 * Integers are stored in a big-endian and sign-magnitude format. Big-endian
 * favors comparison since we could always start comparison using the first few
 * bytes. This gives the compiler opportunities to optimize comparison
 * using advanced techniques such as SIMD or loop unrolling.
 *
 * For details of how and why integers must be stored in a big-endian and
 * sign-magnitude format, please refer to adaptive radix tree's key format
 *
 * Note: CompactIntsKey size must always be aligned to 64 bit boundaries; There
 * are static assertion to enforce this rule
 *
 * @tparam KeySize number of 8 byte fields to use. Valid range is 1 through 4.
 */
template <uint8_t KeySize>
class CompactIntsKey {
 public:
  static_assert(KeySize > 0 && KeySize <= COMPACTINTSKEY_MAX_SIZE);  // size must be no greater than 256-bits
  static_assert(KeySize % sizeof(uintptr_t) == 0);                   // size must be multiple of 8 bytes

  /**
   * @return underlying byte array, exposed for hasher and comparators
   */
  const byte *KeyData() const { return key_data_; }

  /**
   * Set the CompactIntsKey's data based on a ProjectedRow and associated index metadata
   * @param from ProjectedRow to generate CompactIntsKey representation of
   * @param metadata index information, primarily attribute sizes and the precomputed offsets to translate PR layout to
   * CompactIntsKey
   * @param num_attrs Number of attributes
   */
  void SetFromProjectedRow(const storage::ProjectedRow &from, const IndexMetadata &metadata, size_t num_attrs) {
    const auto &attr_sizes = metadata.GetAttributeSizes();
    const auto &compact_ints_offsets = metadata.GetCompactIntsOffsets();

    NOISEPAGE_ASSERT(attr_sizes.size() == from.NumColumns(), "attr_sizes and ProjectedRow must be equal in size.");
    NOISEPAGE_ASSERT(attr_sizes.size() == compact_ints_offsets.size(),
                     "attr_sizes and attr_offsets must be equal in size.");
    NOISEPAGE_ASSERT(!attr_sizes.empty(), "attr_sizes has too few values.");
    NOISEPAGE_ASSERT(num_attrs > 0 && num_attrs <= from.NumColumns(), "num_attrs invariant failed");

    // NOLINTNEXTLINE (Matt): tidy thinks this has side-effects. I disagree.
    NOISEPAGE_ASSERT(std::invoke([&]() -> bool {
                       for (uint16_t i = 0; i < num_attrs; i++) {
                         if (from.IsNull(i)) return false;
                       }
                       return true;
                     }),
                     "There should not be any NULL attributes in this key.");

    // NOLINTNEXTLINE (Matt): tidy thinks this has side-effects. I disagree.
    NOISEPAGE_ASSERT(std::invoke([&]() -> bool {
                       for (const auto &i : metadata.GetSchema().GetColumns()) {
                         if (i.Nullable()) return false;
                       }
                       return true;
                     }),
                     "There should not be any NULL attributes in this schema.");

    // we hash and compare KeySize bytes in all of our operations. Since there might be over-provisioned bytes, we want
    // to make sure the entire key is memset to 0
    std::memset(key_data_, 0, KeySize);

    for (uint8_t i = 0; i < num_attrs; i++) {
      NOISEPAGE_ASSERT(compact_ints_offsets[i] + attr_sizes[i] <= KeySize, "out of bounds");
      CopyAttrFromProjection(from, from.ColumnIds()[i].UnderlyingValue(), attr_sizes[i], compact_ints_offsets[i]);
    }
  }

  /**
   * Returns whether this key is less than another key up to num_attrs for comparison.
   * @param rhs other key to compare against
   * @param metadata Index Key metadata
   * @param num_attrs attributes to compare against
   * @returns whether this is less than other
   */
  bool PartialLessThan(const CompactIntsKey<KeySize> &rhs, const IndexMetadata *metadata, size_t num_attrs) const {
    const auto &attr_sizes = metadata->GetAttributeSizes();
    const auto &compact_ints_offsets = metadata->GetCompactIntsOffsets();
    for (uint8_t i = 0; i < num_attrs; i++) {
      switch (attr_sizes[i]) {
        case sizeof(int8_t): {
          auto cur = GetInteger<int8_t>(compact_ints_offsets[i]);
          auto other = rhs.GetInteger<int8_t>(compact_ints_offsets[i]);
          if (cur < other) return true;
          if (cur > other) return false;
          break;
        }
        case sizeof(int16_t): {
          auto cur = GetInteger<int16_t>(compact_ints_offsets[i]);
          auto other = rhs.GetInteger<int16_t>(compact_ints_offsets[i]);
          if (cur < other) return true;
          if (cur > other) return false;
          break;
        }
        case sizeof(int32_t): {
          auto cur = GetInteger<int32_t>(compact_ints_offsets[i]);
          auto other = rhs.GetInteger<int32_t>(compact_ints_offsets[i]);
          if (cur < other) return true;
          if (cur > other) return false;
          break;
        }
        case sizeof(int64_t): {
          auto cur = GetInteger<int64_t>(compact_ints_offsets[i]);
          auto other = rhs.GetInteger<int64_t>(compact_ints_offsets[i]);
          if (cur < other) return true;
          if (cur > other) return false;
          break;
        }
        default:
          throw std::runtime_error("Invalid attribute size.");
      }
    }

    return true;
  }

 private:
  byte key_data_[KeySize];

  void CopyAttrFromProjection(const storage::ProjectedRow &from, const uint16_t projection_list_offset,
                              const uint8_t attr_size, const uint8_t compact_ints_offset) {
    const byte *const stored_attr = from.AccessWithNullCheck(projection_list_offset);
    NOISEPAGE_ASSERT(stored_attr != nullptr, "Cannot index a nullable attribute with CompactIntsKey.");
    switch (attr_size) {
      case sizeof(int8_t): {
        int8_t data = *reinterpret_cast<const int8_t *>(stored_attr);
        AddInteger<int8_t>(data, compact_ints_offset);
        break;
      }
      case sizeof(int16_t): {
        int16_t data = *reinterpret_cast<const int16_t *>(stored_attr);
        AddInteger<int16_t>(data, compact_ints_offset);
        break;
      }
      case sizeof(int32_t): {
        int32_t data = *reinterpret_cast<const int32_t *>(stored_attr);
        AddInteger<int32_t>(data, compact_ints_offset);
        break;
      }
      case sizeof(int64_t): {
        int64_t data = *reinterpret_cast<const int64_t *>(stored_attr);
        AddInteger<int64_t>(data, compact_ints_offset);
        break;
      }
      default:
        throw std::runtime_error("Invalid attribute size.");
    }
  }

  /*
   * TwoBytesToBigEndian() - Change 2 bytes to big endian
   *
   * This function could be achieved using XCHG instruction; so do not write
   * your own
   *
   * i.e. MOV AX, WORD PTR [data]
   *      XCHG AH, AL
   */
  static uint16_t TwoBytesToBigEndian(const uint16_t data) { return htobe16(data); }

  /*
   * FourBytesToBigEndian() - Change 4 bytes to big endian format
   *
   * This function uses BSWAP instruction in one atomic step; do not write
   * your own
   *
   * i.e. MOV EAX, WORD PTR [data]
   *      BSWAP EAX
   */
  static uint32_t FourBytesToBigEndian(const uint32_t data) { return htobe32(data); }

  /*
   * EightBytesToBigEndian() - Change 8 bytes to big endian format
   *
   * This function uses BSWAP instruction
   */
  static uint64_t EightBytesToBigEndian(const uint64_t data) { return htobe64(data); }

  /*
   * TwoBytesToHostEndian() - Converts back two byte integer to host byte order
   */
  static uint16_t TwoBytesToHostEndian(const uint16_t data) { return be16toh(data); }

  /*
   * FourBytesToHostEndian() - Converts back four byte integer to host byte
   * order
   */
  static uint32_t FourBytesToHostEndian(const uint32_t data) { return be32toh(data); }

  /*
   * EightBytesToHostEndian() - Converts back eight byte integer to host byte
   * order
   */
  static uint64_t EightBytesToHostEndian(const uint64_t data) { return be64toh(data); }

  /*
   * ToBigEndian() - Overloaded version for all kinds of integral data types
   */

  static uint8_t ToBigEndian(const uint8_t data) { return data; }

  static uint8_t ToBigEndian(const int8_t data) { return static_cast<uint8_t>(data); }

  static uint16_t ToBigEndian(const uint16_t data) { return TwoBytesToBigEndian(data); }

  static uint16_t ToBigEndian(const int16_t data) { return TwoBytesToBigEndian(static_cast<uint16_t>(data)); }

  static uint32_t ToBigEndian(const uint32_t data) { return FourBytesToBigEndian(data); }

  static uint32_t ToBigEndian(const int32_t data) { return FourBytesToBigEndian(static_cast<uint32_t>(data)); }

  static uint64_t ToBigEndian(const uint64_t data) { return EightBytesToBigEndian(data); }

  static uint64_t ToBigEndian(const int64_t data) { return EightBytesToBigEndian(static_cast<uint64_t>(data)); }

  /*
   * ToHostEndian() - Converts big endian data to host format
   */

  static uint8_t ToHostEndian(const uint8_t data) { return data; }

  static uint8_t ToHostEndian(const int8_t data) { return static_cast<uint8_t>(data); }

  static uint16_t ToHostEndian(const uint16_t data) { return TwoBytesToHostEndian(data); }

  static uint16_t ToHostEndian(const int16_t data) { return TwoBytesToHostEndian(static_cast<uint16_t>(data)); }

  static uint32_t ToHostEndian(const uint32_t data) { return FourBytesToHostEndian(data); }

  static uint32_t ToHostEndian(const int32_t data) { return FourBytesToHostEndian(static_cast<uint32_t>(data)); }

  static uint64_t ToHostEndian(const uint64_t data) { return EightBytesToHostEndian(data); }

  static uint64_t ToHostEndian(const int64_t data) { return EightBytesToHostEndian(static_cast<uint64_t>(data)); }

  /*
   * SignFlip() - Flips the highest bit of a given integral type
   *
   * This flip is logical, i.e. it happens on the logical highest bit of an
   * integer. The actual position on the address space is related to endianness
   * Therefore this should happen first.
   *
   * It does not matter whether IntType is signed or unsigned because we do
   * not use the sign bit
   */
  template <typename IntType>
  static IntType SignFlip(IntType data) {
    // This sets 1 on the MSB of the corresponding type
    // NOTE: Must cast 0x1 to the correct type first
    // otherwise, 0x1 is treated as the signed int type, and after leftshifting
    // if it is extended to larger type then sign extension will be used
    auto mask = static_cast<IntType>(static_cast<IntType>(0x1) << (sizeof(IntType) * 8UL - 1));
    return data ^ mask;
  }

  /**
   * Adds an integer into key_data_ in compact form
   * @tparam IntType must be of the following 4 types: int8_t; int16_t; int32_t; int64_t
   * @param data value to be inserted
   * @param offset byte offset within key_data_ to write to
   */
  template <typename IntType>
  void AddInteger(const IntType data, const size_t offset) {
    const auto sign_flipped = SignFlip<IntType>(data);
    AddUnsignedInteger(sign_flipped, offset);
  }

  /**
   * Adds an unsigned integer into key_data_ in compact form
   * @tparam IntType must be of the following 4 types: uint8_t; uint16_t; uint32_t; uint64_t
   * @param data value to be inserted
   * @param offset byte offset within key_data_ to write to
   */
  template <typename IntType>
  void AddUnsignedInteger(const IntType data, const size_t offset) {
    // This function always returns the unsigned type
    // so we must use automatic type inference
    const auto big_endian = ToBigEndian(data);

    NOISEPAGE_ASSERT(offset + sizeof(IntType) <= KeySize, "Out of bounds access on key_data_.");

    // This will almost always be optimized into single move
    std::memcpy(key_data_ + offset, &big_endian, sizeof(IntType));
  }

  /*
   * GetInteger() - Extracts an integer from the given offset
   *
   * This function has the same limitation as stated for AddInteger()
   */
  template <typename IntType>
  IntType GetInteger(size_t offset) const {
    const auto *ptr = reinterpret_cast<const IntType *>(key_data_ + offset);

    // This always returns an unsigned number
    auto host_endian = ToHostEndian(*ptr);

    return SignFlip<IntType>(static_cast<IntType>(host_endian));
  }

  /*
   * GetUnsignedInteger() - Extracts an unsigned integer from the given offset
   *
   * This function has the same limitation as stated for AddUnsignedInteger()
   */
  template <typename IntType>
  IntType GetUnsignedInteger(size_t offset) {
    const IntType *ptr = reinterpret_cast<IntType *>(key_data_ + offset);
    auto host_endian = ToHostEndian(*ptr);
    return static_cast<IntType>(host_endian);
  }
};

static_assert(sizeof(CompactIntsKey<8>) == 8, "size of the class should be 8 bytes");
static_assert(sizeof(CompactIntsKey<16>) == 16, "size of the class should be 16 bytes");
static_assert(sizeof(CompactIntsKey<24>) == 24, "size of the class should be 24 bytes");
static_assert(sizeof(CompactIntsKey<32>) == 32, "size of the class should be 32 bytes");

extern template class CompactIntsKey<8>;
extern template class CompactIntsKey<16>;
extern template class CompactIntsKey<24>;
extern template class CompactIntsKey<32>;

}  // namespace noisepage::storage::index

namespace std {

/**
 * Implements std::hash for CompactIntsKey. Allows the class to be used with containers that expect STL interface.
 * @tparam KeySize number of 8-byte fields to use. Valid range is 1 through 4.
 */
template <uint8_t KeySize>
struct hash<noisepage::storage::index::CompactIntsKey<KeySize>> {
  /**
   * @param key key to be hashed
   * @return hash of the key's underlying data
   */
  size_t operator()(const noisepage::storage::index::CompactIntsKey<KeySize> &key) const {
    const auto *const data = reinterpret_cast<const void *const>(key.KeyData());
    return static_cast<size_t>(XXH3_64bits(data, KeySize));
  }
};

/**
 * Implements std::equal_to for CompactIntsKey. Allows the class to be used with containers that expect STL interface.
 * @tparam KeySize number of 8-byte fields to use. Valid range is 1 through 4.
 */
template <uint8_t KeySize>
struct equal_to<noisepage::storage::index::CompactIntsKey<KeySize>> {
  /**
   * Due to the KeySize constraints this should be optimized to a single SIMD instruction.
   * @param lhs first key to be compared
   * @param rhs second key to be compared
   * @return true if first key is equal to the second key
   */
  bool operator()(const noisepage::storage::index::CompactIntsKey<KeySize> &lhs,
                  const noisepage::storage::index::CompactIntsKey<KeySize> &rhs) const {
    return std::memcmp(lhs.KeyData(), rhs.KeyData(), KeySize) == 0;
  }
};

/**
 * Implements std::less for CompactIntsKey. Allows the class to be used with containers that expect STL interface.
 * @tparam KeySize number of 8-byte fields to use. Valid range is 1 through 4.
 */
template <uint8_t KeySize>
struct less<noisepage::storage::index::CompactIntsKey<KeySize>> {
  /**
   * Due to the KeySize constraints, this should be optimized to a single SIMD instruction.
   * @param lhs first key to be compared
   * @param rhs second key to be compared
   * @return true if first key is less than the second key
   */
  bool operator()(const noisepage::storage::index::CompactIntsKey<KeySize> &lhs,
                  const noisepage::storage::index::CompactIntsKey<KeySize> &rhs) const {
    return std::memcmp(lhs.KeyData(), rhs.KeyData(), KeySize) < 0;
  }
};
}  // namespace std
