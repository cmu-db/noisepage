#pragma once

#include <utility>
#include <vector>
#include "bwtree/bwtree.h"
#include "catalog/catalog_defs.h"
#include "storage/index/bwtree_index.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/index.h"
#include "storage/index/index_defs.h"
#include "storage/index/index_metadata.h"

namespace terrier::storage::index {

class IndexBuilder {
 private:
  catalog::index_oid_t index_oid_{0};
  ConstraintType constraint_type_ = ConstraintType::INVALID;
  KeySchema key_schema_;

 public:
  IndexBuilder() = default;

  Index *Build() const {
    TERRIER_ASSERT(!key_schema_.empty(), "Cannot build an index without a KeySchema.");
    TERRIER_ASSERT(constraint_type_ != ConstraintType::INVALID, "Cannot build an index without a ConstraintType.");

    // compute attribute sizes
    std::vector<uint8_t> attr_sizes;
    attr_sizes.reserve(key_schema_.size());
    for (const auto &k : key_schema_) {
      attr_sizes.emplace_back(type::TypeUtil::GetTypeSize(k.type_id));
    }

    // figure out if we can use CompactIntsKey
    bool use_compact_ints = true;
    uint32_t key_size = 0;

    for (auto i = 0; use_compact_ints && i < key_schema_.size(); i++) {
      const auto &attr = key_schema_[i];
      use_compact_ints = use_compact_ints && !attr.is_nullable && CompactIntsOk(attr.type_id);  // key type ok?
      key_size += type::TypeUtil::GetTypeSize(attr.type_id);
      use_compact_ints = use_compact_ints && key_size <= sizeof(uint64_t) * INTSKEY_MAX_SLOTS;  // key size fits?
    }

    TERRIER_ASSERT(use_compact_ints, "Currently, we only have integer keys of limited size.");

    IndexMetadata metadata(key_schema_, attr_sizes);

    return BuildBwTreeIntsKey(index_oid_, constraint_type_, key_size, metadata);
  }

  IndexBuilder &SetOid(const catalog::index_oid_t index_oid) {
    index_oid_ = index_oid;
    return *this;
  }

  IndexBuilder &SetConstraintType(const ConstraintType constraint_type) {
    constraint_type_ = constraint_type;
    return *this;
  }

  IndexBuilder &SetKeySchema(const KeySchema &key_schema) {
    key_schema_ = key_schema;
    return *this;
  }

 private:
  /**
   * @return true if attr_type can be represented with CompactIntsKey
   */
  static bool CompactIntsOk(type::TypeId attr_type) {
    switch (attr_type) {
      case type::TypeId::TINYINT:
      case type::TypeId::SMALLINT:
      case type::TypeId::INTEGER:
      case type::TypeId::BIGINT:
        return true;
      default:
        break;
    }
    return false;
  }

  Index *BuildBwTreeIntsKey(catalog::index_oid_t index_oid, ConstraintType constraint_type, uint32_t key_size,
                            const IndexMetadata &metadata) const {
    TERRIER_ASSERT(key_size <= sizeof(uint64_t) * INTSKEY_MAX_SLOTS, "Not enough slots for given key size.");
    Index *index = nullptr;
    if (key_size <= sizeof(uint64_t)) {
      index = new BwTreeIndex<CompactIntsKey<1>, CompactIntsComparator<1>, CompactIntsEqualityChecker<1>,
                              CompactIntsHasher<1>>(index_oid, constraint_type, metadata);
    } else if (key_size <= sizeof(uint64_t) * 2) {
      index = new BwTreeIndex<CompactIntsKey<2>, CompactIntsComparator<2>, CompactIntsEqualityChecker<2>,
                              CompactIntsHasher<2>>(index_oid, constraint_type, metadata);
    } else if (key_size <= sizeof(uint64_t) * 3) {
      index = new BwTreeIndex<CompactIntsKey<3>, CompactIntsComparator<3>, CompactIntsEqualityChecker<3>,
                              CompactIntsHasher<3>>(index_oid, constraint_type, metadata);
    } else if (key_size <= sizeof(uint64_t) * 4) {
      index = new BwTreeIndex<CompactIntsKey<4>, CompactIntsComparator<4>, CompactIntsEqualityChecker<4>,
                              CompactIntsHasher<4>>(index_oid, constraint_type, metadata);
    }
    TERRIER_ASSERT(index != nullptr, "Failed to create an IntsKey index.");
    return index;
  }
};

}  // namespace terrier::storage::index
