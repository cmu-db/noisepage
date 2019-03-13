#pragma once

#include <boost/functional/hash.hpp>
#include <cstring>
#include <functional>
#include <vector>

#include "storage/index/index_metadata.h"
#include "storage/projected_row.h"
#include "storage/storage_defs.h"

namespace terrier::storage::index {

#define GENERICKEY_MAX_SIZE 256

template <uint16_t KeySize>
class GenericKeyEqualityChecker;
template <uint16_t KeySize>
class GenericKeyComparator;

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

  void SetFromProjectedRow(const storage::ProjectedRow &from, const IndexMetadata &metadata) {
    TERRIER_ASSERT(from.NumColumns() == schema->GetColumns().size(),
                   "ProjectedRow should have the same number of columns at the original key schema.");

    ZeroOut();

    std::memcpy(GetProjectedRow(), &from, from.Size());
  }

 private:
  friend class GenericKeyEqualityChecker<KeySize>;
  friend class GenericKeyComparator<KeySize>;

  void ZeroOut() { std::memset(key_data, 0x00, key_size_byte); }

  ProjectedRow *const GetProjectedRow() const {
    auto *const pr = reinterpret_cast<ProjectedRow *const>(StorageUtil::AlignedPtr(sizeof(uint64_t), key_data));
    TERRIER_ASSERT(reinterpret_cast<uintptr_t>(pr) % sizeof(uint64_t) == 0,
                   "ProjectedRow must be aligned to 8 bytes for atomicity guarantees.");
    TERRIER_ASSERT(reinterpret_cast<uintptr_t>(pr) + pr->Size() < reinterpret_cast<uintptr_t>(this) + key_size_byte,
                   "ProjectedRow will access out of bounds.");
    return pr;
  }

  byte key_data[key_size_byte];

  const catalog::Schema *schema;
};

template <uint16_t KeySize>
class GenericKeyComparator {
 public:
  bool operator()(const GenericKey<KeySize> &lhs, const GenericKey<KeySize> &rhs) const {
    TERRIER_ASSERT(lhs.schema == rhs.schema, "Keys must have the same schema.");
    return false;
  }

  GenericKeyComparator(const GenericKeyComparator &) = default;
  GenericKeyComparator() = default;
};

template <uint16_t KeySize>
class GenericKeyEqualityChecker {
 public:
  bool operator()(const GenericKey<KeySize> &lhs, const GenericKey<KeySize> &rhs) const {
    TERRIER_ASSERT(lhs.schema == rhs.schema, "Keys must have the same schema.");
    return std::memcmp(lhs.key_data, rhs.key_data, GenericKey<KeySize>::key_size_byte) == 0;
  }

  GenericKeyEqualityChecker(const GenericKeyEqualityChecker &) = default;
  GenericKeyEqualityChecker() = default;
};

template <uint16_t KeySize>
struct GenericKeyHasher : std::unary_function<GenericKey<KeySize>, std::size_t> {
  size_t operator()(GenericKey<KeySize> const &p) const { return 1; }

  GenericKeyHasher(const GenericKeyHasher &) = default;
  GenericKeyHasher() = default;
};

}  // namespace terrier::storage::index
