#pragma once

#include <boost/functional/hash.hpp>
#include <cstring>
#include <vector>

#include "common/portable_endian.h"
#include "storage/projected_row.h"
#include "storage/storage_defs.h"

namespace terrier::storage::index {

template <uint8_t KeySize>
class CompactIntsHasher;

// This is the maximum number of 8-byte slots that we will pack into a single
// CompactIntsKey template. You should not instantiate anything with more than this
#define INTSKEY_MAX_SLOTS 4

/*
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
 * Note: CompactIntsKey should always be aligned to 64 bit boundaries; There
 * are static assertion to enforce this rule
 */
template <uint8_t KeySize>
class CompactIntsKey {
 private:
  friend class CompactIntsHasher<KeySize>;

  // This is the actual byte size of the key
  static constexpr size_t key_size_byte = KeySize * sizeof(uint64_t);

  // This is the array we use for storing integers
  byte key_data[key_size_byte];

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
   * integer. The actual position on the address space is related to endianess
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

  /*
   * ZeroOut() - Sets all bits to zero
   */
  void ZeroOut() { std::memset(key_data, 0x00, key_size_byte); }

  /*
   * GetRawData() - Returns the raw data array
   */
  const byte *GetRawData() const { return key_data; }

  /*
   * GetInteger() - Extracts an integer from the given offset
   *
   * This function has the same limitation as stated for AddInteger()
   */
  template <typename IntType>
  IntType GetInteger(size_t offset) const {
    const auto *ptr = reinterpret_cast<const IntType *>(key_data + offset);

    // This always returns an unsigned number
    auto host_endian = ToHostEndian(*ptr);

    return SignFlip<IntType>(static_cast<IntType>(host_endian));
  }

  /*
   * GetUnsignedInteger() - Extracts an unsigned integer from the given offset
   *
   * The same constraint about IntType applies
   */
  template <typename IntType>
  IntType GetUnsignedInteger(size_t offset) {
    const IntType *ptr = reinterpret_cast<IntType *>(key_data + offset);
    auto host_endian = ToHostEndian(*ptr);
    return static_cast<IntType>(host_endian);
  }

  /*
   * SetFromColumn() - Sets the value of a column into a given offset of
   *                   this ints key
   *
   * This function returns a size_t which is the next starting offset.
   *
   * Note: Two column IDs are needed - one into the key schema which is used
   * to determine the type of the column; another into the tuple to
   * get data
   */

  void CopyAttrFromProjection(const storage::ProjectedRow &from, const uint16_t projection_list_offset,
                              const uint8_t attr_size, const uint8_t compact_ints_offset) {
    const byte *const stored_attr = from.AccessWithNullCheck(projection_list_offset);
    TERRIER_ASSERT(stored_attr != nullptr, "Cannot index a nullable attribute with CompactIntsKey.");
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
        // Invalid attr size
        throw std::runtime_error("Invalid key write value");
    }
  }

 public:
  /*
   * Constructor
   */
  CompactIntsKey() {
    TERRIER_ASSERT(KeySize > 0 && KeySize <= INTSKEY_MAX_SLOTS, "Invalid key size.");
    ZeroOut();
  }

  void SetFromProjectedRow(const storage::ProjectedRow &from, const std::vector<uint8_t> &attr_sizes,
                           const std::vector<uint8_t> &compact_ints_offsets) {
    TERRIER_ASSERT(attr_sizes.size() == from.NumColumns(), "attr_sizes and ProjectedRow must be equal in size.");
    TERRIER_ASSERT(attr_sizes.size() == compact_ints_offsets.size(),
                   "attr_sizes and attr_offsets must be equal in size.");
    TERRIER_ASSERT(!attr_sizes.empty(), "attr_sizes has too few values.");
    ZeroOut();

    for (uint8_t i = 0; i < from.NumColumns(); i++) {
      TERRIER_ASSERT(compact_ints_offsets[i] + attr_sizes[i] <= key_size_byte, "out of bounds");
      CopyAttrFromProjection(from, i, attr_sizes[i], compact_ints_offsets[i]);
    }
  }

  /*
   * Compare() - Compares two IntsType object of the same length
   *
   * This function has the same semantics as memcmp(). Negative result means
   * less than, positive result means greater than, and 0 means equal
   */
  static int Compare(const CompactIntsKey<KeySize> &a, const CompactIntsKey<KeySize> &b) {
    return std::memcmp(a.key_data, b.key_data, CompactIntsKey<KeySize>::key_size_byte);
  }

  /*
   * LessThan() - Returns true if first is less than the second
   */
  static bool LessThan(const CompactIntsKey<KeySize> &a, const CompactIntsKey<KeySize> &b) { return Compare(a, b) < 0; }

  /*
   * Equals() - Returns true if first is equivalent to the second
   */
  static bool Equals(const CompactIntsKey<KeySize> &a, const CompactIntsKey<KeySize> &b) { return Compare(a, b) == 0; }

  /*
   * AddInteger() - Adds a new integer into the compact form
   *
   * Note that IntType must be of the following 8 types:
   *   int8_t; int16_t; int32_t; int64_t
   * Otherwise the result is undefined
   */
  template <typename IntType>
  void AddInteger(IntType data, size_t offset) {
    auto sign_flipped = SignFlip<IntType>(data);

    // This function always returns the unsigned type
    // so we must use automatic type inference
    auto big_endian = ToBigEndian(sign_flipped);

    // This will almost always be optimized into single move
    std::memcpy(key_data + offset, &big_endian, sizeof(IntType));
  }

  /*
   * AddUnsignedInteger() - Adds an unsigned integer of a certain type
   *
   * Only the following unsigned type should be used:
   *   uint8_t; uint16_t; uint32_t; uint64_t
   */
  template <typename IntType>
  void AddUnsignedInteger(IntType data, size_t offset) {
    // This function always returns the unsigned type
    // so we must use automatic type inference
    auto big_endian = ToBigEndian(data);

    // This will almost always be optimized into single move
    std::memcpy(key_data + offset, &big_endian, sizeof(IntType));
  }
};

/*
 * class CompactIntsComparator - Compares two compact integer key
 */
template <uint8_t KeySize>
class CompactIntsComparator {
 public:
  CompactIntsComparator() { TERRIER_ASSERT(KeySize > 0 && KeySize <= INTSKEY_MAX_SLOTS, "Invalid key size."); }
  CompactIntsComparator(const CompactIntsComparator &) = default;

  /*
   * operator()() - Returns true if lhs < rhs
   */
  bool operator()(const CompactIntsKey<KeySize> &lhs, const CompactIntsKey<KeySize> &rhs) const {
    return CompactIntsKey<KeySize>::LessThan(lhs, rhs);
  }
};

/*
 * class CompactIntsEqualityChecker - Compares whether two integer keys are
 *                                    equivalent
 */
template <uint8_t KeySize>
class CompactIntsEqualityChecker {
 public:
  CompactIntsEqualityChecker() { TERRIER_ASSERT(KeySize > 0 && KeySize <= INTSKEY_MAX_SLOTS, "Invalid key size."); }
  CompactIntsEqualityChecker(const CompactIntsEqualityChecker &) = default;

  bool operator()(const CompactIntsKey<KeySize> &lhs, const CompactIntsKey<KeySize> &rhs) const {
    return CompactIntsKey<KeySize>::Equals(lhs, rhs);
  }
};

/*
 * class CompactIntsHasher - Hash function for integer key
 *
 * This function assumes the length of the integer key is always multiples
 * of 64 bits (8 byte word).
 */
template <uint8_t KeySize>
class CompactIntsHasher {
 public:
  // Emphasize here that we want a 8 byte aligned object
  static_assert(sizeof(CompactIntsKey<KeySize>) % sizeof(uint64_t) == 0,
                "Please align the size of compact integer key");

  // Make sure there is no other field
  static_assert(sizeof(CompactIntsKey<KeySize>) == CompactIntsKey<KeySize>::key_size_byte,
                "Extra fields detected in class CompactIntsKey");

  CompactIntsHasher() { TERRIER_ASSERT(KeySize > 0 && KeySize <= INTSKEY_MAX_SLOTS, "Invalid key size."); }
  CompactIntsHasher(const CompactIntsHasher &) = default;

  /*
   * operator()() - Hashes an object into size_t
   *
   * This function hashes integer key using 64 bit chunks. Chunks are
   * accumulated to the hash one by one. Since
   */
  size_t operator()(const CompactIntsKey<KeySize> &p) const {
    size_t seed = 0UL;
    const auto *const ptr = reinterpret_cast<const size_t *const>(p.GetRawData());

    // For every 8 byte word just combine it with the current seed
    for (size_t i = 0; i < KeySize; i++) {
      boost::hash_combine(seed, ptr[i]);
    }

    return seed;
  }
};

}  // namespace terrier::storage::index
