#pragma once
#include <memory>
#include <unordered_map>
#include "catalog/catalog_defs.h"
#include "catalog/database_handle.h"
#include "common/strong_typedef.h"
namespace terrier::catalog {

/**
 * The global catalog object. It contains all the information about global catalog tables. It's also
 * the entry point for transactions to access any data in any sql table.
 *
 * OID assignment:
 * Note that we do not have a concept of oid_t anymore. Instead, we have
 *  db_oid_t, nsp_oid_t, table_oid_t, col_oid_t
 * In addition, for nsp_oid_t, table_oid_t, and col_oid_t, we only guarantee uniqueness inside a database, which means
 * that the table_oid for pg_attribute in database A could be the same as pg_attribute in database B.
 *
 * db_oid_t:
 *  0          is reserved for default database - terrier
 *  1+         are used for new databases
 *
 * nsp_oid_t:
 *  0          is reserved for pg_catalog namespace
 *  1          is reserved for public namespace
 *  2          is reserved for temp namespace
 *  100+       are used for other user-defined namesapces
 *
 * table_oid_t:
 *  0-999      are reserved for global catalog tables
 *  1000-9999  are reserved for database-specific catalog tables
 *  10000+     are used for user-defined tables in a database
 *
 * col_oid_t
 *  0-999      are reserved for columns in global catalog tables
 *  1000-9999  are reserved for columns in database-specific catalog tables
 *  10000+     are used for user-defined columns
 */
class Catalog {
 public:
  /**
   * Initialize catalog, including
   * 1) Create all global catalog tables
   * 2) Populate global catalogs (bootstrapping)
   * @param txn_manager the global transaction manager
   */
  explicit Catalog(transaction::TransactionManager *txn_manager);

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
   * 1) It populates all the global catalogs
   * 2) It creates a default database named "terrier"
   */
  void Bootstrap();

  /**
   * Bootstrap a database. Specifically, it
   * 1) creates database-specific catalogs, such as pg_namespace, pg_class
   * 2) populates these catalogs
   *
   * It does not modify any global catalogs. So if you want to create a database,
   * make sure it has been added to pg_database before you call this.
   *
   * Note that the catalogs created by this function have the same table_oid_t
   * for all database. oids are only supposed to be unique within a database.
   *
   * It is used for bootstrapping both default database and user-defined database.
   * After bootstrapping a user-defined database, the initial state is an exact
   * copy of initial state of a template database.
   * * @param db_oid the oid of the database you are trying to bootstrap
   */
  void BootstrapDatabase(transaction::TransactionContext *txn, db_oid_t db_oid);

  /**
   * A dummy call back function for committing bootstrap transaction
   */
  static void BootstrapCallback(void * /*unused*/) {}

 private:
  transaction::TransactionManager *txn_manager_;
  // block store to create catalog tables
  storage::BlockStore block_store_{100, 100};
  // global catalogs
  std::shared_ptr<storage::SqlTable> pg_database_;
  // map from (db_oid, catalog table_oid_t) to sql table
  std::unordered_map<db_oid_t, std::unordered_map<table_oid_t, std::shared_ptr<storage::SqlTable>>> map_;
};

extern std::shared_ptr<Catalog> terrier_catalog;

}  // namespace terrier::catalog
