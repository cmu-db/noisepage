#pragma once

#include "storage/index/index.h"
#include "storage/index/index_builder.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog {
class CatalogIndex {
 public:
  CatalogIndex(transaction::TransactionContext *txn, index_oid_t index_oid,
               storage::index::ConstraintType constraint_type,
               const storage::index::IndexKeySchema &schema)
      : metadata_{schema} {
    storage::index::IndexBuilder builder;
    builder.SetConstraintType(constraint_type)
        .SetKeySchema(metadata_.GetKeySchema())
        .SetOid(index_oid);
    index_.reset(builder.Build());
  }

  std::shared_ptr<storage::index::Index> GetIndex() { return index_; }

  storage::index::IndexMetadata *GetMetadata() { return &metadata_; }

  std::pair<db_oid_t, table_oid_t> GetTable() { return {db_oid_, table_oid_}; }

  void SetTable(db_oid_t db_oid, table_oid_t table_oid) {
    db_oid_ = db_oid;
    table_oid_ = table_oid;
  }

 private:
  storage::index::IndexMetadata metadata_;
  std::shared_ptr<storage::index::Index> index_ = nullptr;
  db_oid_t db_oid_;
  table_oid_t table_oid_;
};
}  // namespace terrier::catalog