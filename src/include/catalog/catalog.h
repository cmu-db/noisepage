#pragma once
#include <memory>
#include <unordered_map>
#include "catalog/catalog_defs.h"
#include "catalog/database_handle.h"
#include "common/strong_typedef.h"
namespace terrier::catalog {

extern std::atomic<uint32_t> oid_counter;

class Catalog {
 public:
  /**
   * Initialize catalog, including
   * 1) Create all global catalog tables
   * 2) Populate global catalogs (bootstrapping)
   * @param txn_manager the global transaction manager
   */
  Catalog(transaction::TransactionManager *txn_manager);

  DatabaseHandle GetDatabase(oid_t db_oid) { return DatabaseHandle(db_oid, pg_database_); }

  ~Catalog() = default;

 private:
  void Bootstrap();

 private:
  transaction::TransactionManager *txn_manager_;
  // block store to create catalog tables
  storage::BlockStore block_store_{100, 100};
  // global catalogs
  std::shared_ptr<storage::SqlTable> pg_database_;
};

extern std::shared_ptr<Catalog> terrier_catalog;

}  // namespace terrier::catalog
