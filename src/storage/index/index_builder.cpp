#include "storage/index/index_builder.h"
#include <utility>

namespace terrier::storage::index {
Index *IndexBuilder::Build() const {
  TERRIER_ASSERT(!key_schema_.empty(), "Cannot build an index without a KeySchema.");
  TERRIER_ASSERT(constraint_type_ != ConstraintType::INVALID, "Cannot build an index without a ConstraintType.");

  IndexMetadata metadata(key_schema_);

  // figure out if we can use CompactIntsKey
  bool use_compact_ints = true;
  uint32_t key_size = 0;

  for (uint16_t i = 0; use_compact_ints && i < key_schema_.size(); i++) {
    const auto &attr = key_schema_[i];
    use_compact_ints = use_compact_ints && !attr.IsNullable() && CompactIntsOk(attr.GetType());  // key type ok?
    key_size += type::TypeUtil::GetTypeSize(attr.GetType());
    use_compact_ints = use_compact_ints && key_size <= sizeof(uint64_t) * INTSKEY_MAX_SLOTS;  // key size fits?
  }

  if (use_compact_ints) return BuildBwTreeIntsKey(index_oid_, constraint_type_, key_size, std::move(metadata));
  return BuildBwTreeGenericKey(index_oid_, constraint_type_, std::move(metadata));
}

IndexBuilder &IndexBuilder::SetOid(const catalog::index_oid_t &index_oid) {
  index_oid_ = index_oid;
  return *this;
}

IndexBuilder &IndexBuilder::SetConstraintType(const ConstraintType &constraint_type) {
  constraint_type_ = constraint_type;
  return *this;
}

IndexBuilder &IndexBuilder::SetKeySchema(const IndexKeySchema &key_schema) {
  key_schema_ = key_schema;
  return *this;
}

Index *IndexBuilder::BuildBwTreeIntsKey(catalog::index_oid_t index_oid, ConstraintType constraint_type,
                                        uint32_t key_size, IndexMetadata metadata) const {
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

Index *IndexBuilder::BuildBwTreeGenericKey(catalog::index_oid_t index_oid, ConstraintType constraint_type,
                                           IndexMetadata metadata) const {
  const auto pr_size = metadata.GetProjectedRowInitializer().ProjectedRowSize();
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
}  // namespace terrier::storage::index
