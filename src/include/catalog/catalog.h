#pragma once
#include <memory>
#include <unordered_map>
#include "catalog/catalog_defs.h"
#include "catalog/catalog_sql_table.h"
#include "catalog/database_handle.h"
#include "common/strong_typedef.h"
#include "storage/sql_table.h"

namespace terrier::catalog {

class DatabaseHandle;
/**
 * The global catalog object. It contains all the information about global catalog tables. It's also
 * the entry point for transactions to access any data in any sql table.
 *
 * OID assignment:
 * Note that we do not have a concept of oid_t anymore. Instead, we have
 *  db_oid_t, namespace_oid_t, table_oid_t, col_oid_t
 * In addition, for namespace_oid_t, table_oid_t, and col_oid_t, we only guarantee uniqueness inside a database,
 * which means that the table_oid for pg_attribute in database A could be the same as pg_attribute in database B.
 *
 * db_oid_t:
 *  0          reserved
 *  1          is reserved for default database - terrier
 *  2+         are used for new databases
 *
 * namespace_oid_t:
 *  0          reserved
 *  1          is reserved for pg_catalog namespace
 *  2          is reserved for public namespace
 *  3          is reserved for temp namespace
 *  100+       are used for other user-defined namespaces
 *
 * table_oid_t:
 *  0          reserved
 *  1-999      are reserved for global catalog tables
 *  1000-9999  are reserved for database-specific catalog tables
 *  10000+     are used for user-defined tables in a database
 *
 * col_oid_t
 *  0          reserved
 *  1-999      are reserved for columns in global catalog tables
 *  1000-9999  are reserved for columns in database-specific catalog tables
 *  10000+     are used for user-defined columns
 */
class Catalog {
 public:
  /**
   * Creates the (global) catalog object, and bootstraps, i.e. creates
   * all the default and system databases and tables.
   * @param txn_manager the global transaction manager
   */
  explicit Catalog(transaction::TransactionManager *txn_manager);

  /**
   * Lookup a database oid and return a database handle.
   * @param db_oid to look up.
   * @return the corresponding database handle
   */
  DatabaseHandle GetDatabaseHandle(db_oid_t db_oid);

  /**
   * Get the pointer to a database-specific catalog sql table.
   * @param db_oid the database the catalog belongs to
   * @param table_oid the table oid of the catalog
   * @return a pointer to the catalog
   */
  std::shared_ptr<storage::SqlTable> GetDatabaseCatalog(db_oid_t db_oid, table_oid_t table_oid);

  /**
   * Get the next database_oid
   * @return next database_oid
   */
  db_oid_t GetNextDBOid();

  /**
   * Get the next namespace_oid
   * @return next namespace_oid
   */
  namespace_oid_t GetNextNamespaceOid();

  /**
   * Get the next table_oid
   * @return next table_oid
   */
  table_oid_t GetNextTableOid();

  /**
   * Get the next col_oid
   * @return next col_oid
   */
  col_oid_t GetNextColOid();

  std::shared_ptr<catalog::SqlTableRW> GetPGDatabase() {
    return pg_database_;
  }

 private:
  /**
   * Bootstrap all the catalog tables so that new coming transactions can
   * correctly perform SQL queries.
   * 1) It creates and populates all the global catalogs
   * 2) It creates a default database named "terrier"
   * 2) It bootstraps the default database.
   */
  void Bootstrap();

  /**
   * Bootstrap a database, i.e create all the catalogs local to this database, and do all other initialization.
   * 1) Create pg_namespace (catalog)
   * 2) Create pg_class (catalog)
   * 3) TODO(pakhtar) -  other catalogs for Postgres compatibility
   * 4) populates these catalogs
   *
   * It is used for bootstrapping both default database and user-defined database.
   * After bootstrapping a user-defined database, the initial state is an exact
   * copy of initial state of a template database.
   *
   * Notes:
   * 1) Caller must add the database to pg_database.
   * 2) Table oids are only unique within a database (not globally unique).
   * *
   * * @param db_oid the oid of the database you are trying to bootstrap
   */
  void BootstrapDatabase(transaction::TransactionContext *txn, db_oid_t db_oid);

  /**
   * A dummy call back function for committing bootstrap transaction
   */
  static void BootstrapCallback(void * /*unused*/) {}
  /**
   * Creates pg_database, the global catalog of all databases.
   * - creates the storage (a SQL table) for pg_database
   * - inserts an entry (row) for the default database, terrier, into pg_database
   * @param txn the bootstrapping transaction
   * @param table_oid the table oid of pg_database
   * @param start_col_oid the starting col oid for columns in pg_database.
   */
  void CreatePGDatabase(table_oid_t table_oid);

  void PopulatePGDatabase(transaction::TransactionContext *txn);
  void PopulatePGTablespace(transaction::TransactionContext *txn);

  /**
   * Creates pg_tablespace SQL table and populates pg_tablespace
   * Note: pg_tablespace is not currently used.
   * @param txn the bootstrapping transaction
   * @param table_oid the table oid of pg_tablespace
   * @param start_col_oid the starting col oid for columns in pg_tablespace.
   */
  void CreatePGTablespace(table_oid_t table_oid);



 private:
  transaction::TransactionManager *txn_manager_;
  // block store to create catalog tables
  // storage::BlockStore block_store_{100, 100};
  // global catalogs
  // std::shared_ptr<storage::SqlTable> pg_database_;
  // std::shared_ptr<storage::SqlTable> pg_tablespace_;

  // replacement for the above
  // global catalogs
  std::shared_ptr<catalog::SqlTableRW> pg_database_;
  std::shared_ptr<catalog::SqlTableRW> pg_tablespace_;

  // map from (db_oid, catalog table_oid_t) to sql table
  std::unordered_map<db_oid_t, std::unordered_map<table_oid_t, std::shared_ptr<storage::SqlTable>>> map_;

  std::atomic<db_oid_t> db_oid_;
  std::atomic<namespace_oid_t> namespace_oid_;
  std::atomic<table_oid_t> table_oid_;
  std::atomic<col_oid_t> col_oid_;
};

extern std::shared_ptr<Catalog> terrier_catalog;

}  // namespace terrier::catalog
