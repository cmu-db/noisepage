#pragma once

#include <cstring>
#include <functional>
#include <vector>

#include "storage/index/index_metadata.h"
#include "storage/projected_row.h"
#include "storage/storage_defs.h"
#include "xxHash/xxh3.h"

namespace terrier::storage::index {

// This is the maximum number of bytes to pack into a single HashKey template. This constraint is arbitrary and can
// be increased if 4096-bytes is too small for future workloads.
#define HASHKEY_MAX_SIZE 4096

/**
 * HashKey - it's a thing
 *
 * This class is used for storing multiple integral fields into a compact
 * array representation
 *
 *
 * @tparam KeySize number of bytes for the key's internal buffer
 */
template <uint16_t KeySize>
class HashKey {
 public:
  /**
   * key size in bytes, exposed for hasher and comparators
   */
  static constexpr size_t key_size_byte = KeySize;
  static_assert(KeySize > 0 && KeySize <= HASHKEY_MAX_SIZE);  // size must be no greater than 256-bits
  static_assert(key_size_byte % sizeof(uintptr_t) == 0);      // size must be multiple of 8 bytes

  /**
   * @return underlying byte array, exposed for hasher and comparators
   */
  const byte *KeyData() const { return key_data_; }

  /**
   * Set the HashKey's data based on a ProjectedRow and associated index metadata
   * @param from ProjectedRow to generate HashKey representation of
   * @param metadata index information, primarily attribute sizes and the precomputed offsets to translate PR layout to
   * HashKey
   */
  void SetFromProjectedRow(const storage::ProjectedRow &from, const IndexMetadata &metadata) {
    std::memset(key_data_, 0, key_size_byte);

    const auto key_size = metadata.KeySize();

    // NOLINTNEXTLINE (Matt): tidy thinks this has side-effects. I disagree.
    TERRIER_ASSERT(std::invoke([&]() -> bool {
                     for (uint16_t i = 0; i < from.NumColumns(); i++) {
                       if (from.IsNull(i)) return false;
                     }
                     return true;
                   }),
                   "There should not be any NULL attributes in this key.");

    // This assumes no padding between attributes
    std::memcpy(key_data_, from.AccessWithNullCheck(0), key_size);
  }

 private:
  byte key_data_[key_size_byte];
};

}  // namespace terrier::storage::index

namespace std {

/**
 * Implements std::hash for HashKey. Allows the class to be used with STL containers and the BwTree index.
 * @tparam KeySize number of bytes for the key's internal buffer
 */
template <uint16_t KeySize>
struct hash<terrier::storage::index::HashKey<KeySize>> {
  /**
   * @param key key to be hashed
   * @return hash of the key's underlying data
   */
  size_t operator()(const terrier::storage::index::HashKey<KeySize> &key) const {
    // you're technically hashing more bytes than you need to, but hopefully key size isn't wildly over-provisioned
    return static_cast<size_t>(XXH3_64bits(reinterpret_cast<const void *>(key.KeyData()),
                                           terrier::storage::index::HashKey<KeySize>::key_size_byte));
  }
};

/**
 * Implements std::equal_to for HashKey. Allows the class to be used with STL containers and the BwTree index.
 * @tparam KeySize number of bytes for the key's internal buffer
 */
template <uint16_t KeySize>
struct equal_to<terrier::storage::index::HashKey<KeySize>> {
  /**
   * @param lhs first key to be compared
   * @param rhs second key to be compared
   * @return true if first key is equal to the second key
   */
  bool operator()(const terrier::storage::index::HashKey<KeySize> &lhs,
                  const terrier::storage::index::HashKey<KeySize> &rhs) const {
    // you're technically comparing more bytes than you need to, but hopefully key size isn't wildly over-provisioned
    return std::memcmp(lhs.KeyData(), rhs.KeyData(), terrier::storage::index::HashKey<KeySize>::key_size_byte) == 0;
  }
};
}  // namespace std
