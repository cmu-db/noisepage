#pragma once
#include <memory>
#include <unordered_map>
#include "catalog/catalog_defs.h"
#include "catalog/database_handle.h"
#include "common/strong_typedef.h"
namespace terrier::catalog {

extern std::atomic<uint32_t> oid_counter;

/**
 * The global catalog object. It contains all the information about global catalog tables. It's also
 * the entry point for transactions to access any data in any sql table.
 */
class Catalog {
 public:
  /**
   * Initialize catalog, including
   * 1) Create all global catalog tables
   * 2) Populate global catalogs (bootstrapping)
   * @param txn_manager the global transaction manager
   */
  Catalog(transaction::TransactionManager *txn_manager);

  /**
   * Return a database handle for given db_oid.
   * @param db_oid the given db_oid
   * @return the corresponding database handle
   */
  DatabaseHandle GetDatabase(db_oid_t db_oid) { return DatabaseHandle(db_oid, pg_database_); }

 private:
  /**
   * Bootstrap all the catalog tables so that new coming transactions can
   * correctly perform SQL queries.
   * 1) It creates a default database named "terrier"
   * 2) It populates all the global catalogs and database-specific catalogs for "terrier"
   */
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
