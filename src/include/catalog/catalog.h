#pragma once
#include <memory>
#include <mutex>
#include <unordered_map>
#include "catalog/catalog_defs.h"
#include "catalog/database_handle.h"
#include "common/strong_typedef.h"
namespace terrier::catalog {

extern std::atomic<uint32_t> oid_counter;

class Catalog {
 public:
  // Global Singleton
  Catalog();

  void Bootstrap();

  DatabaseHandle GetDatabase(transaction::TransactionContext *const txn,
                             oid_t db_oid){// TODO: don't know how to do select
                                           pg_database_->Select(txn, )};

  // Deconstruct the catalog database when destroying the catalog.
  ~Catalog() {
    if (pg_database_) delete pg_database_;
  };

 private:
  // block store to use
  storage::BlockStore block_store_{100, 100};

  // need to create an actual table;
  // the schema for the pg_database;
  std::vector<Schema::Column> cols_;
  storage::SqlTable *pg_database_;
};

extern std::shared_ptr<Catalog> terrier_catalog;

}  // namespace terrier::catalog