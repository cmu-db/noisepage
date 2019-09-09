#pragma once

#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "catalog/index_schema.h"
#include "storage/index/bwtree_index.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_index.h"
#include "storage/index/hash_key.h"
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
  catalog::IndexSchema key_schema_;
  bool ordered_ = true;

 public:
  IndexBuilder() = default;

  /**
   * @return a new best-possible index for the current parameters
   */
  Index *Build() const {
    TERRIER_ASSERT(!key_schema_.GetColumns().empty(), "Cannot build an index without a KeySchema.");

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

    if (ordered_) {
      if (use_compact_ints) return BuildBwTreeIntsKey(key_size, std::move(metadata));
      return BuildBwTreeGenericKey(std::move(metadata));
    }
    if (use_compact_ints) return BuildHashIntsKey(std::move(metadata));
    return BuildHashGenericKey(std::move(metadata));
  }

  /**
   * @param key_schema the index key schema
   * @return the builder object
   */
  IndexBuilder &SetKeySchema(const catalog::IndexSchema &key_schema) {
    key_schema_ = key_schema;
    return *this;
  }

  /**
   * @param ordered true if the index should support ordered scans, false otherwise (point queries only)
   * @return the builder object
   */
  IndexBuilder &SetOrdered(const bool ordered) {
    ordered_ = ordered;
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

  Index *BuildBwTreeIntsKey(uint32_t key_size, IndexMetadata metadata) const {
    TERRIER_ASSERT(key_size <= sizeof(uint64_t) * INTSKEY_MAX_SLOTS, "Not enough slots for given key size.");
    Index *index = nullptr;
    if (key_size <= sizeof(uint64_t)) {
      index = new BwTreeIndex<CompactIntsKey<1>>(std::move(metadata));
    } else if (key_size <= sizeof(uint64_t) * 2) {
      index = new BwTreeIndex<CompactIntsKey<2>>(std::move(metadata));
    } else if (key_size <= sizeof(uint64_t) * 3) {
      index = new BwTreeIndex<CompactIntsKey<3>>(std::move(metadata));
    } else if (key_size <= sizeof(uint64_t) * 4) {
      index = new BwTreeIndex<CompactIntsKey<4>>(std::move(metadata));
    }
    TERRIER_ASSERT(index != nullptr, "Failed to create an IntsKey index.");
    return index;
  }

  Index *BuildBwTreeGenericKey(IndexMetadata metadata) const {
    const auto pr_size = metadata.GetInlinedPRInitializer().ProjectedRowSize();
    Index *index = nullptr;

    const auto key_size =
        (pr_size + 8) +
        sizeof(uintptr_t);  // account for potential padding of the PR and the size of the pointer for metadata

    if (key_size <= 64) {
      index = new BwTreeIndex<GenericKey<64>>(std::move(metadata));
    } else if (key_size <= 128) {
      index = new BwTreeIndex<GenericKey<128>>(std::move(metadata));
    } else if (key_size <= 256) {
      index = new BwTreeIndex<GenericKey<256>>(std::move(metadata));
    }
    TERRIER_ASSERT(index != nullptr, "Failed to create an GenericKey index.");
    return index;
  }

  Index *BuildHashIntsKey(IndexMetadata metadata) const {
    const auto key_size = metadata.KeySize();
    TERRIER_ASSERT(metadata.KeySize() <= HASHKEY_MAX_SIZE, "Not enough space for given key size.");
    Index *index = nullptr;
    if (key_size <= 8) {
      index = new HashIndex<HashKey<8>>(std::move(metadata));
    } else if (key_size <= 16) {
      index = new HashIndex<HashKey<16>>(std::move(metadata));
    } else if (key_size <= 32) {
      index = new HashIndex<HashKey<32>>(std::move(metadata));
    } else if (key_size <= 64) {
      index = new HashIndex<HashKey<64>>(std::move(metadata));
    } else if (key_size <= 128) {
      index = new HashIndex<HashKey<128>>(std::move(metadata));
    } else if (key_size <= 256) {
      index = new HashIndex<HashKey<256>>(std::move(metadata));
    } else if (key_size <= 512) {
      index = new HashIndex<HashKey<512>>(std::move(metadata));
    } else if (key_size <= 1024) {
      index = new HashIndex<HashKey<1024>>(std::move(metadata));
    } else if (key_size <= 2048) {
      index = new HashIndex<HashKey<2048>>(std::move(metadata));
    } else if (key_size <= 4096) {
      index = new HashIndex<HashKey<4096>>(std::move(metadata));
    }
    TERRIER_ASSERT(index != nullptr, "Failed to create an IntsKey index.");
    return index;
  }

  Index *BuildHashGenericKey(IndexMetadata metadata) const {
    const auto pr_size = metadata.GetInlinedPRInitializer().ProjectedRowSize();
    Index *index = nullptr;

    const auto key_size =
        (pr_size + 8) +
        sizeof(uintptr_t);  // account for potential padding of the PR and the size of the pointer for metadata

    if (key_size <= 64) {
      index = new HashIndex<GenericKey<64>>(std::move(metadata));
    } else if (key_size <= 128) {
      index = new HashIndex<GenericKey<128>>(std::move(metadata));
    } else if (key_size <= 256) {
      index = new HashIndex<GenericKey<256>>(std::move(metadata));
    }
    TERRIER_ASSERT(index != nullptr, "Failed to create an IntsKey index.");
    return index;
  }
};

}  // namespace terrier::storage::index
