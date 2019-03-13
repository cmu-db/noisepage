#pragma once

#include <boost/functional/hash.hpp>
#include <cstring>
#include <functional>
#include <vector>

#include "storage/projected_row.h"
#include "storage/storage_defs.h"

namespace terrier::storage::index {

#define GENERICKEY_MAX_SIZE 256

/*
 * class GenericKey - Key used for indexing with opaque data
 *
 * This key type uses an fixed length array to hold data for indexing
 * purposes, the actual size of which is specified and instantiated
 * with a template argument.
 */
template <uint16_t KeySize>
class GenericKey {
 public:
  // This is the actual byte size of the key
  static constexpr size_t key_size_byte = KeySize;

  /*
   * ZeroOut() - Sets all bits to zero
   */
  void ZeroOut() { std::memset(key_data, 0x00, key_size_byte); }

  void SetFromProjectedRow(const storage::ProjectedRow &from, const std::vector<uint8_t> &attr_sizes,
                           const std::vector<uint8_t> &compact_ints_offsets) {
    TERRIER_ASSERT(attr_sizes.size() == from.NumColumns(), "attr_sizes and ProjectedRow must be equal in size.");
    TERRIER_ASSERT(attr_sizes.size() == compact_ints_offsets.size(),
                   "attr_sizes and attr_offsets must be equal in size.");
    TERRIER_ASSERT(!attr_sizes.empty(), "attr_sizes has too few values.");
    ZeroOut();

    // TODO(Matt): alignment?
    std::memcpy(key_data, &from, from.Size());
  }

  byte key_data[KeySize];

  const catalog::Schema *schema;
};

/**
 * Function object returns true if lhs < rhs, used for trees
 */
template <uint16_t KeySize>
class FastGenericComparator {
 public:
  inline bool operator()(const GenericKey<KeySize> &lhs, const GenericKey<KeySize> &rhs) const { return false; }

  FastGenericComparator(const FastGenericComparator &) = default;
  FastGenericComparator() = default;
};

/**
 * Equality-checking function object
 */
template <uint16_t KeySize>
class GenericEqualityChecker {
 public:
  inline bool operator()(const GenericKey<KeySize> &lhs, const GenericKey<KeySize> &rhs) const {
    const auto pr_size = reinterpret_cast<const ProjectedRow *const>(lhs.key_data)->Size();
    return std::memcmp(lhs.key_data, rhs.key_data, pr_size) == 0;
  }

  GenericEqualityChecker(const GenericEqualityChecker &) = default;
  GenericEqualityChecker() = default;
};

/**
 * Hash function object for an array of SlimValues
 */
template <uint16_t KeySize>
struct GenericHasher : std::unary_function<GenericKey<KeySize>, std::size_t> {
  /** Generate a 64-bit number for the key value */
  inline size_t operator()(GenericKey<KeySize> const &p) const { return 1; }

  GenericHasher(const GenericHasher &) = default;
  GenericHasher() = default;
};

}  // namespace terrier::storage::index
