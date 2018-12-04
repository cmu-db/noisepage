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

  DatabaseHandle &GetDatabase(oid_t db_oid) { return db_map_[db_oid]; };

  // Deconstruct the catalog database when destroying the catalog.
  ~Catalog() {
    if (pg_database_) delete pg_database_;
  };

 private:
  // need to create an actual table;
  // the schema for the pg_database;
  std::vector<Schema::Column> cols_;
  storage::BlockStore block_store_{100, 100};
  storage::SqlTable *pg_database_;

  // key: database oid
  // value: database handlers
  std::unordered_map<oid_t, DatabaseHandle> db_map_;
};

extern std::shared_ptr<Catalog> terrier_catalog;

}  // namespace terrier::catalog