#pragma once

#include <cstring>
#include <functional>
#include <vector>

#include "farmhash/farmhash.h"
#include "storage/index/index_metadata.h"
#include "storage/projected_row.h"
#include "storage/storage_defs.h"

namespace terrier::storage::index {

// This is the maximum number of 8-byte slots to pack into a single CompactHashKey template. This constraint is not
// arbitrary and cannot be increased beyond 256 bits until AVX-512 is more widely available.
#define INTSKEY_MAX_SLOTS 4

/**
 * CompactHashKey - Compact representation of multifield integers
 *
 * This class is used for storing multiple integral fields into a compact
 * array representation. This class is largely used as a static object,
 * because special storage format is used to ensure a fast comparison
 * implementation.
 *
 * Note: CompactHashKey size must always be aligned to 64 bit boundaries; There
 * are static assertion to enforce this rule
 *
 * @tparam KeySize number of 8-byte fields to use. Valid range is 1 through 4.
 */
template <uint8_t KeySize>
class CompactHashKey {
 public:
  /**
   * key size in bytes, exposed for hasher and comparators
   */
  static constexpr size_t key_size_byte = KeySize * sizeof(uint64_t);
  static_assert(KeySize > 0 && KeySize <= INTSKEY_MAX_SLOTS);  // size must be no greater than 256-bits
  static_assert(key_size_byte % sizeof(uintptr_t) == 0);       // size must be multiple of 8 bytes

  /**
   * @return underlying byte array, exposed for hasher and comparators
   */
  const byte *KeyData() const { return key_data_; }

  /**
   * Set the CompactHashKey's data based on a ProjectedRow and associated index metadata
   * @param from ProjectedRow to generate CompactHashKey representation of
   * @param metadata index information, primarily attribute sizes and the precomputed offsets to translate PR layout to
   * CompactHashKey
   */
  void SetFromProjectedRow(const storage::ProjectedRow &from, const IndexMetadata &metadata) {
    const auto &attr_sizes = metadata.GetAttributeSizes();

    TERRIER_ASSERT(attr_sizes.size() == from.NumColumns(), "attr_sizes and ProjectedRow must be equal in size.");
    TERRIER_ASSERT(!attr_sizes.empty(), "attr_sizes has too few values.");

    std::memset(key_data_, 0, key_size_byte);

    uint8_t offset = 0;
    for (uint8_t i = 0; i < from.NumColumns(); i++) {
      const byte *const stored_attr = from.AccessWithNullCheck(static_cast<uint16_t>(from.ColumnIds()[i]));
      TERRIER_ASSERT(stored_attr != nullptr, "Attribute cannot be NULL in CompactHashKey.");
      const uint8_t size = attr_sizes[i];
      TERRIER_ASSERT(offset + size <= key_size_byte, "Out of bounds.");
      std::memcpy(key_data_ + offset, stored_attr, size);
      offset += size;
    }
  }

 private:
  byte key_data_[key_size_byte];
};

static_assert(sizeof(CompactHashKey<1>) == 8, "size of the class should be 8 bytes");
static_assert(sizeof(CompactHashKey<2>) == 16, "size of the class should be 16 bytes");
static_assert(sizeof(CompactHashKey<3>) == 24, "size of the class should be 24 bytes");
static_assert(sizeof(CompactHashKey<4>) == 32, "size of the class should be 32 bytes");

}  // namespace terrier::storage::index

namespace std {

/**
 * Implements std::hash for CompactHashKey. Allows the class to be used with STL containers and the BwTree index.
 * @tparam KeySize number of 8-byte fields to use. Valid range is 1 through 4.
 */
template <uint8_t KeySize>
struct hash<terrier::storage::index::CompactHashKey<KeySize>> {
  /**
   * @param key key to be hashed
   * @return hash of the key's underlying data
   */
  size_t operator()(const terrier::storage::index::CompactHashKey<KeySize> &key) const {
    const auto *const data = reinterpret_cast<const char *const>(key.KeyData());
    return static_cast<size_t>(util::Hash64(data, terrier::storage::index::CompactHashKey<KeySize>::key_size_byte));
  }
};

/**
 * Implements std::equal_to for CompactHashKey. Allows the class to be used with STL containers and the BwTree index.
 * @tparam KeySize number of 8-byte fields to use. Valid range is 1 through 4.
 */
template <uint8_t KeySize>
struct equal_to<terrier::storage::index::CompactHashKey<KeySize>> {
  /**
   * Due to the KeySize constraints this should be optimized to a single SIMD instruction.
   * @param lhs first key to be compared
   * @param rhs second key to be compared
   * @return true if first key is equal to the second key
   */
  bool operator()(const terrier::storage::index::CompactHashKey<KeySize> &lhs,
                  const terrier::storage::index::CompactHashKey<KeySize> &rhs) const {
    return std::memcmp(lhs.KeyData(), rhs.KeyData(), terrier::storage::index::CompactHashKey<KeySize>::key_size_byte) ==
           0;
  }
};
}  // namespace std
