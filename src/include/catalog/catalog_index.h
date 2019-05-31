#pragma once

#include <memory>
#include <utility>
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
   * @return corresponding db_oid, table_oid pair
   */
  std::tuple<db_oid_t, namespace_oid_t, table_oid_t> GetTable() { return {db_oid_, ns_oid_, table_oid_}; }

  /**
   * Sets the index's corresponding db_oid and table_oid
   * @param db_oid database oid
   * @param ns_oid namespace oid
   * @param table_oid table oid
   */
  void SetTable(db_oid_t db_oid, namespace_oid_t ns_oid, table_oid_t table_oid) {
    db_oid_ = db_oid;
    ns_oid_ = ns_oid;
    table_oid_ = table_oid;
  }

 private:
  // Metadata
  storage::index::IndexMetadata metadata_;
  // Underlying index
  std::shared_ptr<storage::index::Index> index_ = nullptr;
  // Corresponding db_oid
  db_oid_t db_oid_;
  // Corresponding ns_oid
  namespace_oid_t ns_oid_;
  // Corresponding table_oid
  table_oid_t table_oid_;
};
}  // namespace terrier::catalog
