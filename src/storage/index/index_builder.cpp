#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "storage/index/bwtree_index.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_index.h"
#include "storage/index/hash_key.h"
#include "storage/index/index.h"
#include "storage/index/index_builder.h"
#include "storage/index/index_defs.h"
#include "storage/index/index_metadata.h"
#include "storage/projected_row.h"

namespace terrier::storage::index {

Index *IndexBuilder::Build() const {
  TERRIER_ASSERT(!key_schema_.GetColumns().empty(), "Cannot build an index without a KeySchema.");

  IndexMetadata metadata(key_schema_);
  const auto &key_cols = key_schema_.GetColumns();

  // Check if it's a simple key: that is all attributes are integral and not NULL-able. Simple keys are compatible
  // with CompactIntsKey and HashKey. Otherwise we fall back to GenericKey.
  bool simple_key = true;
  for (uint16_t i = 0; simple_key && i < key_cols.size(); i++) {
    const auto &attr = key_cols[i];
    simple_key = simple_key && !attr.Nullable();
    simple_key = simple_key && (std::count(NUMERIC_KEY_TYPES.cbegin(), NUMERIC_KEY_TYPES.cend(), attr.Type()) > 0);
  }

  switch (key_schema_.Type()) {
    case IndexType::BWTREE: {
      if (simple_key && metadata.KeySize() <= COMPACTINTSKEY_MAX_SIZE) return BuildBwTreeIntsKey(std::move(metadata));
      return BuildBwTreeGenericKey(std::move(metadata));
    }
    case IndexType::HASHMAP: {
      if (simple_key && metadata.KeySize() <= HASHKEY_MAX_SIZE) return BuildHashIntsKey(std::move(metadata));
      return BuildHashGenericKey(std::move(metadata));
    }
    default:
      return nullptr;
  }
}

IndexBuilder &IndexBuilder::SetKeySchema(const catalog::IndexSchema &key_schema) {
  key_schema_ = key_schema;
  return *this;
}

IndexBuilder &IndexBuilder::SetSqlTableAndTransactionContext(const common::ManagedPointer<transaction::TransactionContext> txn,
                                               common::ManagedPointer<storage::SqlTable> sql_table,
                                               const catalog::IndexSchema &key_schema) {
  TERRIER_ASSERT((sql_table == nullptr && txn == nullptr) || (sql_table != nullptr && txn != nullptr),
                 "sql_table / txn is null and txn / sql_table is not.");
  txn_ = txn;
  sql_table_ = sql_table;
  key_schema_ = key_schema;
  return *this;
}

bool IndexBuilder::Insert(common::ManagedPointer<storage::index::Index> index, storage::TupleSlot table_tuple_slot) const {
  // Initialize index pr
  const auto index_pr_initializer = index->GetProjectedRowInitializer();
  const uint32_t index_pr_size = index_pr_initializer.ProjectedRowSize();
  byte *index_pr_buffer = common::AllocationUtil::AllocateAligned(index_pr_size);
  ProjectedRow *index_pr = index_pr_initializer.InitializeRow(index_pr_buffer);
  // get all index col oid and Initialize table pr
  const auto &indexed_attributes = key_schema_.GetIndexedColOids();
  const auto table_pr_initializer = sql_table_->InitializerForProjectedRow(indexed_attributes);
  const uint32_t table_pr_size = table_pr_initializer.ProjectedRowSize();
  byte *table_pr_buffer = common::AllocationUtil::AllocateAligned(table_pr_size);
  ProjectedRow *table_pr = table_pr_initializer.InitializeRow(table_pr_buffer);

  auto pr_map = sql_table_->ProjectionMapForOids(indexed_attributes);

  sql_table_->Select(txn_, table_tuple_slot, table_pr);

  // Insert into the index
  auto num_index_cols = key_schema_.GetColumns().size();
  TERRIER_ASSERT(num_index_cols == indexed_attributes.size(), "Only support index keys that are a single column oid");
  for (uint32_t col_idx = 0; col_idx < num_index_cols; col_idx++) {
    const auto &col = key_schema_.GetColumn(col_idx);
    auto index_col_oid = col.Oid();
    const catalog::col_oid_t &table_col_oid = indexed_attributes[col_idx];
    if (table_pr->IsNull(pr_map[table_col_oid])) {
      index_pr->SetNull(index->GetKeyOidToOffsetMap().at(index_col_oid));
    } else {
      // TODO(Wuwen): This may not be thread safe
      auto size = AttrSizeBytes(col.AttrSize());
      std::memcpy(index_pr->AccessForceNotNull(index->GetKeyOidToOffsetMap().at(index_col_oid)),
                  table_pr->AccessWithNullCheck(pr_map[table_col_oid]), size);
    }
  }
  auto insert_result = index->Insert(txn_, *index_pr, table_tuple_slot);

  delete[] index_pr_buffer;
  delete[] table_pr_buffer;
  return insert_result;
}

Index *IndexBuilder::BuildBwTreeIntsKey(IndexMetadata metadata) const {
  metadata.SetKeyKind(IndexKeyKind::COMPACTINTSKEY);
  const auto key_size = metadata.KeySize();
  TERRIER_ASSERT(key_size <= COMPACTINTSKEY_MAX_SIZE, "Key size exceeds maximum for this key type.");
  Index *index = nullptr;
  if (key_size <= 8) {
    index = new BwTreeIndex<CompactIntsKey<8>>(std::move(metadata));
  } else if (key_size <= 16) {
    index = new BwTreeIndex<CompactIntsKey<16>>(std::move(metadata));
  } else if (key_size <= 24) {
    index = new BwTreeIndex<CompactIntsKey<24>>(std::move(metadata));
  } else if (key_size <= 32) {
    index = new BwTreeIndex<CompactIntsKey<32>>(std::move(metadata));
  }
  TERRIER_ASSERT(index != nullptr, "Failed to create an IntsKey index.");
  return index;
}

Index *IndexBuilder::BuildBwTreeGenericKey(IndexMetadata metadata) const {
  metadata.SetKeyKind(IndexKeyKind::GENERICKEY);
  const auto pr_size = metadata.GetInlinedPRInitializer().ProjectedRowSize();
  Index *index = nullptr;

  const auto key_size =
      (pr_size + 8) +
      sizeof(uintptr_t);  // account for potential padding of the PR and the size of the pointer for metadata
  TERRIER_ASSERT(key_size <= GENERICKEY_MAX_SIZE, "Key size exceeds maximum for this key type.");

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

Index *IndexBuilder::BuildHashIntsKey(IndexMetadata metadata) const {
  metadata.SetKeyKind(IndexKeyKind::HASHKEY);
  const auto key_size = metadata.KeySize();
  TERRIER_ASSERT(metadata.KeySize() <= HASHKEY_MAX_SIZE, "Key size exceeds maximum for this key type.");
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
  }
  TERRIER_ASSERT(index != nullptr, "Failed to create an IntsKey index.");
  return index;
}

Index *IndexBuilder::BuildHashGenericKey(IndexMetadata metadata) const {
  metadata.SetKeyKind(IndexKeyKind::GENERICKEY);
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
}  // namespace terrier::storage::index
