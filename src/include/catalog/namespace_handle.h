#pragma once

#include <memory>
#include <utility>

#include "catalog/catalog_defs.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

class NamespaceHandle {
 public:
  class NamespaceEntry {
   public:
    NamespaceEntry(nsp_oid_t oid, storage::ProjectedRow *row, storage::ProjectionMap map)
        : oid_(oid), row_(row), map_(std::move(map)) {}

    byte *GetValue(col_oid_t col) { return row_->AccessWithNullCheck(map_[col]); }

    /**
     * Return the nsp_oid of the underlying database
     * @return nsp_oid of the database
     */
    nsp_oid_t GetNamespaceOid() { return oid_; }

    /**
     * Destruct database entry. It frees the memory for storing the projected row.
     */
    ~NamespaceEntry() {
      TERRIER_ASSERT(row_ != nullptr, "namespace entry should always represent a valid row");
      delete[] reinterpret_cast<byte *>(row_);
    }

   private:
    nsp_oid_t oid_;
    storage::ProjectedRow *row_;
    storage::ProjectionMap map_;
  };

  explicit NamespaceHandle(std::shared_ptr<storage::SqlTable> pg_namename) : pg_namespace_(std::move(pg_namename)) {}

  std::shared_ptr<NamespaceEntry> GetNamespaceEntry(transaction::TransactionContext *txn, nsp_oid_t oid);

 private:
  std::shared_ptr<storage::SqlTable> pg_namespace_;
};

}  // namespace terrier::catalog
