#pragma once

#include <memory>
#include "storage/sql_table.h"
namespace terrier::catalog {

class DatabaseHandle {
 public:
  class DatabaseEntry {
   public:
    DatabaseEntry(oid_t oid, storage::SqlTable *table) : oid_(oid), pg_database_(table){};

    storage::ProjectedRow *GetRow(transaction::TransactionContext *const txn, std::vector<col_oid_t> cols);

   private:
    oid_t oid_;
    storage::SqlTable *pg_database_;
  };

  DatabaseHandle(oid_t oid) : oid_(oid){};
  ~DatabaseHandle(){};

  std::vector<NameSpaceHandle> GetNameSpaceHandles(oid_t ns_oid);

  DatabaseEntry GetDatabaseEntry() { return DatabaseEntry(oid_, pg_database_); };

 private:
  oid_t oid_;
  storage::SqlTable *pg_database_;
};

}  // namespace terrier::catalog
