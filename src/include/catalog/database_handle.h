#pragma once

#include <memory>
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

class DatabaseHandle {
 public:
  class DatabaseEntry {
   public:
    DatabaseEntry(transaction::TransactionContext *txn, oid_t oid, storage::ProjectedRow *row)
        : txn_(txn), oid_(oid), row_(row){};

    //    byte *GetValue(col_oid_t col){
    //
    //    }

   private:
    transaction::TransactionContext *txn_;
    oid_t oid_;
    storage::ProjectedRow *row_;
  };

  DatabaseHandle(oid_t oid, std::shared_ptr<storage::SqlTable> pg_database) : oid_(oid), pg_database_(pg_database){};
  ~DatabaseHandle(){};

  // std::vector<NameSpaceHandle> GetNameSpaceHandles(oid_t ns_oid);

  std::vector<DatabaseEntry> GetDatabaseEntries(transaction::TransactionContext *txn) {
    std::vector<DatabaseEntry> results;
    // get an initializer
    // get the block layout and a map from col_oid_t => col_id_t
    Schema schema = pg_database_->GetSchema();
    auto layout_pair = storage::StorageUtil::BlockLayoutFromSchema(schema);

    // get all the
    auto cols = schema.GetColumns();
    std::vector<storage::col_id_t> col_ids;
    for (auto &col : cols) {
      col_ids.emplace_back(layout_pair.second[col.GetOid()]);
    }
    // get an initializer
    storage::ProjectedRowInitializer initializer(layout_pair.first, col_ids);
    // get a read_buffer
    auto read_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    storage::ProjectedRow *read = initializer.InitializeRow(read_buffer);

    // try to get the row
    auto tuple_iter = pg_database_->begin();
    for (; tuple_iter != pg_database_->end(); tuple_iter++) {
      pg_database_->Select(txn, *tuple_iter, read);
      if ((*reinterpret_cast<oid_t *>(read->AccessForceNotNull(0))) == oid_) {
        results.emplace_back(txn, oid_, read);
      }
    }
    TERRIER_ASSERT(results.size() == 1, "Should get only one database entry");
    return results;
  };

 private:
  oid_t oid_;
  std::shared_ptr<storage::SqlTable> pg_database_;
};

}  // namespace terrier::catalog
