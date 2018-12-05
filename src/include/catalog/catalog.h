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

  DatabaseHandle GetDatabase(oid_t db_oid) { return DatabaseHandle(db_oid, pg_database_); };

  // Deconstruct the catalog database when destroying the catalog.
  ~Catalog(){};

 private:
  // block store to use
  storage::BlockStore block_store_{100, 100};

  // pg_database
  std::shared_ptr<storage::SqlTable> pg_database_;
};

extern std::shared_ptr<Catalog> terrier_catalog;

}  // namespace terrier::catalog