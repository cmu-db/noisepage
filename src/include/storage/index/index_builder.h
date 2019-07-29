#pragma once

#include <utility>
#include <vector>
#include "bwtree/bwtree.h"
#include "catalog/catalog_defs.h"
#include "catalog/index_schema.h"
#include "storage/index/bwtree_index.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/generic_key.h"
#include "storage/index/index.h"
#include "storage/index/index_defs.h"
#include "storage/index/index_metadata.h"
#include "storage/projected_row.h"

namespace terrier::storage::index {

/**
 * The IndexBuilder automatically creates the best possible index for the given parameters.
 */
class IndexBuilder {
 private:
  catalog::index_oid_t index_oid_{0};
  ConstraintType constraint_type_ = ConstraintType::INVALID;
  catalog::IndexSchema key_schema_;

 public:
  IndexBuilder() = default;

  /**
   * @return a new best-possible index for the current parameters
   */
  Index *Build() const {
    TERRIER_ASSERT(!key_schema_.GetColumns().empty(), "Cannot build an index without a KeySchema.");
    TERRIER_ASSERT(constraint_type_ != ConstraintType::INVALID, "Cannot build an index without a ConstraintType.");
    TERRIER_ASSERT((constraint_type_ == ConstraintType::DEFAULT && !key_schema_.Unique()) ||
                       (constraint_type_ == ConstraintType::UNIQUE && key_schema_.Unique()),
                   "ContraintType should match the IndexSchema's is_unique flag.");

    IndexMetadata metadata(key_schema_);

    // figure out if we can use CompactIntsKey
    bool use_compact_ints = true;
    uint32_t key_size = 0;

    auto key_cols = key_schema_.GetColumns();

    for (uint16_t i = 0; use_compact_ints && i < key_cols.size(); i++) {
      const auto &attr = key_cols[i];
      use_compact_ints = use_compact_ints && !attr.Nullable() && CompactIntsOk(attr.Type());  // key type ok?
      key_size += type::TypeUtil::GetTypeSize(attr.Type());
      use_compact_ints = use_compact_ints && key_size <= sizeof(uint64_t) * INTSKEY_MAX_SLOTS;  // key size fits?
    }

    if (use_compact_ints) return BuildBwTreeIntsKey(index_oid_, constraint_type_, key_size, std::move(metadata));
    return BuildBwTreeGenericKey(index_oid_, constraint_type_, std::move(metadata));
  }

  /**
   * @param index_oid the index oid
   * @return the builder object
   */
  IndexBuilder &SetOid(const catalog::index_oid_t index_oid) {
    index_oid_ = index_oid;
    return *this;
  }

  /**
   * @param constraint_type the type of index
   * @return the builder object
   */
  IndexBuilder &SetConstraintType(const ConstraintType constraint_type) {
    constraint_type_ = constraint_type;
    return *this;
  }

  /**
   * @param key_schema the index key schema
   * @return the builder object
   */
  IndexBuilder &SetKeySchema(const catalog::IndexSchema &key_schema) {
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
                            IndexMetadata metadata) const {
    TERRIER_ASSERT(key_size <= sizeof(uint64_t) * INTSKEY_MAX_SLOTS, "Not enough slots for given key size.");
    Index *index = nullptr;
    if (key_size <= sizeof(uint64_t)) {
      index = new BwTreeIndex<CompactIntsKey<1>>(index_oid, constraint_type, std::move(metadata));
    } else if (key_size <= sizeof(uint64_t) * 2) {
      index = new BwTreeIndex<CompactIntsKey<2>>(index_oid, constraint_type, std::move(metadata));
    } else if (key_size <= sizeof(uint64_t) * 3) {
      index = new BwTreeIndex<CompactIntsKey<3>>(index_oid, constraint_type, std::move(metadata));
    } else if (key_size <= sizeof(uint64_t) * 4) {
      index = new BwTreeIndex<CompactIntsKey<4>>(index_oid, constraint_type, std::move(metadata));
    }
    TERRIER_ASSERT(index != nullptr, "Failed to create an IntsKey index.");
    return index;
  }

  Index *BuildBwTreeGenericKey(catalog::index_oid_t index_oid, ConstraintType constraint_type,
                               IndexMetadata metadata) const {
    const auto pr_size = metadata.GetInlinedPRInitializer().ProjectedRowSize();
    Index *index = nullptr;

    const auto key_size =
        (pr_size + 8) +
        sizeof(uintptr_t);  // account for potential padding of the PR and the size of the pointer for metadata

    if (key_size <= 64) {
      index = new BwTreeIndex<GenericKey<64>>(index_oid, constraint_type, std::move(metadata));
    } else if (key_size <= 128) {
      index = new BwTreeIndex<GenericKey<128>>(index_oid, constraint_type, std::move(metadata));
    } else if (key_size <= 256) {
      index = new BwTreeIndex<GenericKey<256>>(index_oid, constraint_type, std::move(metadata));
    }
    TERRIER_ASSERT(index != nullptr, "Failed to create an IntsKey index.");
    return index;
  }
};

}  // namespace terrier::storage::index
