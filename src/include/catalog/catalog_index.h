#pragma once

#include <memory>
#include <tuple>
#include "storage/index/index.h"
#include "storage/index/index_builder.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog {
/**
 * Convenience wrapper around the storage layer's indexes.
 * TODO: Add real support of indexes
 */
class CatalogIndex {
 public:
  /**
   * Constructor
   * @param txn transaction to use
   * @param index_oid oid of the index
   * @param constraint_type type of the constraint
   * @param schema index key's schema
   */
  CatalogIndex(transaction::TransactionContext *txn, index_oid_t index_oid,
               storage::index::ConstraintType constraint_type, const storage::index::IndexKeySchema &schema)
      : metadata_{schema} {
    storage::index::IndexBuilder builder;
    builder.SetConstraintType(constraint_type).SetKeySchema(metadata_.GetKeySchema()).SetOid(index_oid);
    index_.reset(builder.Build());
  }

  /**
   * @return underlying index
   */
  std::shared_ptr<storage::index::Index> GetIndex() { return index_; }

  /**
   * @return metadata of this index
   */
  storage::index::IndexMetadata *GetMetadata() { return &metadata_; }

  /**
   * @return corresponding table_oid
   */
  table_oid_t GetTable() { return table_oid_; }

  /**
   * Sets the index's corresponding table_oid
   * @param table_oid table oid
   */
  void SetTable(table_oid_t table_oid) {
    table_oid_ = table_oid;
  }

 private:
  // Metadata
  storage::index::IndexMetadata metadata_;
  // Underlying index
  std::shared_ptr<storage::index::Index> index_ = nullptr;
  // Corresponding table_oid
  table_oid_t table_oid_;
};
}  // namespace terrier::catalog
