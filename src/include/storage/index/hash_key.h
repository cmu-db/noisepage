#pragma once

#include <algorithm>
#include <cstring>
#include <functional>
#include <vector>

#include "storage/index/index_metadata.h"
#include "storage/projected_row.h"
#include "xxHash/xxh3.h"

namespace terrier::storage::index {

// This is the maximum number of bytes to pack into a single HashKey template. This constraint is arbitrary and can
// be increased if 256 bytes is too small for future workloads.
constexpr uint16_t HASHKEY_MAX_SIZE = 256;

/**
 * HashKey - Composite key type for simple (currently defined as integral and not NULL-able) index key attributes. It
 * basically just relies on memcpy from the incoming ProjectedRow. This is safe because there won't be any padding
 * between integral attributes.
 *
 * @tparam KeySize number of bytes for the key's internal buffer
 */
template <uint16_t KeySize>
class HashKey {
 public:
  /**
   * key size in bytes, exposed for hasher and comparators
   */
  static_assert(KeySize > 0 && KeySize <= HASHKEY_MAX_SIZE);

  /**
   * @return underlying byte array, exposed for hasher and comparators
   */
  const byte *KeyData() const { return key_data_; }

  /**
   * Set the HashKey's data based on a ProjectedRow and associated index metadata
   * @param from ProjectedRow to generate HashKey representation of
   * @param metadata index information, primarily attribute sizes and the precomputed offsets to translate PR layout to
   * HashKey
   * @param num_attrs Number of attributes
   */
  void SetFromProjectedRow(const storage::ProjectedRow &from, const IndexMetadata &metadata,
                           UNUSED_ATTRIBUTE size_t num_attrs) {
    // we hash and compare KeySize bytes in all of our operations. Since there might be over-provisioned bytes, we want
    // to make sure the entire key is memset to 0
    std::memset(key_data_, 0, KeySize);

    const auto key_size = metadata.KeySize();

    // NOLINTNEXTLINE (Matt): tidy thinks this has side-effects. @jrolli verified this uses const methods
    TERRIER_ASSERT(std::invoke([&]() -> bool {
                     for (uint16_t i = 0; i < from.NumColumns(); i++) {
                       if (from.IsNull(i)) return false;
                     }
                     return true;
                   }),
                   "There should not be any NULL attributes in this key.");

    // NOLINTNEXTLINE (Matt): tidy thinks this has side-effects. @jrolli verified this uses const methods
    TERRIER_ASSERT(std::invoke([&]() -> bool {
                     for (const auto &i : metadata.GetSchema().GetColumns()) {
                       if (i.Nullable()) return false;
                     }
                     return true;
                   }),
                   "There should not be any NULL attributes in this schema.");

    TERRIER_ASSERT(
        std::invoke([&]() -> bool {
          // we want the smallest attr size to add to the address of the last attribute since attributes are
          // ordered by size in a ProjectedRow
          const uint8_t smallest_attr_size =
              *(std::min_element(metadata.GetAttributeSizes().cbegin(), metadata.GetAttributeSizes().cend()));
          const auto start_address = reinterpret_cast<uintptr_t>(from.AccessWithNullCheck(0));
          // the last attribute's location plus its size should give us the end of the PR
          const auto end_address =
              reinterpret_cast<uintptr_t>(from.AccessWithNullCheck(from.NumColumns() - 1)) + smallest_attr_size;
          // we want to assert that the end address is equal to the start_address plus the sum of the attr
          // sizes. This should suffice to guarantee no padding assuming we can trust other areas of the code that
          // generate these metadata
          return end_address == start_address + key_size;
        }),
        "There should not be any padding in this ProjectedRow.");

    std::memcpy(key_data_, from.AccessWithNullCheck(0), key_size);
  }

 private:
  byte key_data_[KeySize];
};

extern template class HashKey<8>;
extern template class HashKey<16>;
extern template class HashKey<32>;
extern template class HashKey<64>;
extern template class HashKey<128>;
extern template class HashKey<256>;

}  // namespace terrier::storage::index

namespace std {

/**
 * Implements std::hash for HashKey. Allows the class to be used with containers that expect STL interface.
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
    return static_cast<size_t>(XXH3_64bits(reinterpret_cast<const void *>(key.KeyData()), KeySize));
  }
};

/**
 * Implements std::equal_to for HashKey. Allows the class to be used with containers that expect STL interface.
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
    return std::memcmp(lhs.KeyData(), rhs.KeyData(), KeySize) == 0;
  }
};
}  // namespace std
