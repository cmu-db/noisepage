#pragma once

#include <loggers/catalog_logger.h>
#include <memory>
#include <utility>
#include <vector>
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

class DatabaseHandle {
 public:
  class DatabaseEntry {
   public:
    DatabaseEntry(transaction::TransactionContext *txn, oid_t oid, storage::ProjectedRow *row,
                  storage::ProjectionMap map)
        : txn_(txn), oid_(oid), row_(row), map_(std::move(map)) {}

    byte *GetValue(col_oid_t col) { return row_->AccessWithNullCheck(map_[col]); }
    ~DatabaseEntry() {
      if (row_) delete[] reinterpret_cast<byte *>(row_);
    }

   private:
    transaction::TransactionContext *txn_;
    oid_t oid_;
    storage::ProjectedRow *row_;
    storage::ProjectionMap map_;
  };

  DatabaseHandle(oid_t oid, std::shared_ptr<storage::SqlTable> pg_database) : oid_(oid), pg_database_(pg_database) {}
  ~DatabaseHandle() = default;

  std::shared_ptr<DatabaseEntry> GetDatabaseEntry(transaction::TransactionContext *txn, oid_t oid);

 private:
  oid_t oid_;
  std::shared_ptr<storage::SqlTable> pg_database_;
};

}  // namespace terrier::catalog
