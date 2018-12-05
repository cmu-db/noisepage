#pragma once

#include <memory>
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

class DatabaseHandle {
 public:
  class DatabaseEntry {
   public:
    DatabaseEntry(transaction::TransactionContext *txn, oid_t oid, storage::ProjectedRow *row,
                  storage::ProjectionMap &map)  
        : txn_(txn), oid_(oid), row_(row), map_(map){};

    byte *GetValue(col_oid_t col) { return row_->AccessWithNullCheck(map_[col]); }

   private:
    transaction::TransactionContext *txn_;
    oid_t oid_;
    storage::ProjectedRow *row_;
    storage::ProjectionMap map_;
  };

  DatabaseHandle(oid_t oid, std::shared_ptr<storage::SqlTable> pg_database) : oid_(oid), pg_database_(pg_database){};
  ~DatabaseHandle(){};

  // std::vector<NameSpaceHandle> GetNameSpaceHandles(oid_t ns_oid);

  std::shared_ptr<DatabaseEntry> GetDatabaseEntry(transaction::TransactionContext *txn, oid_t oid) {
    // Each database handle can only see entry with the same oid
    if (oid_ != oid) return nullptr;

    // TODO: we can cache this
    // create columns
    std::vector<col_oid_t> cols;
    for (auto &c : pg_database_->GetSchema().GetColumns()) {
      cols.emplace_back(c.GetOid());
    }
    auto row_pair = pg_database_->InitializerForProjectedRow(cols);

    auto read_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
    storage::ProjectedRow *read = row_pair.first.InitializeRow(read_buffer);

    // try to get the row
    auto tuple_iter = pg_database_->begin();
    for (; tuple_iter != pg_database_->end(); tuple_iter++) {
      pg_database_->Select(txn, *tuple_iter, read);
      if ((*reinterpret_cast<oid_t *>(read->AccessForceNotNull(row_pair.second[cols[0]]))) == oid_) {
        return std::shared_ptr<DatabaseEntry>(new DatabaseEntry(txn, oid_, read, row_pair.second));
      }
    }
    return nullptr;
  };

 private:
  oid_t oid_;
  std::shared_ptr<storage::SqlTable> pg_database_;
};

}  // namespace terrier::catalog
