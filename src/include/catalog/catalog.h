#pragma once
#include <memory>
#include <string>
#include <unordered_map>
#include "catalog/catalog_defs.h"
#include "catalog/catalog_sql_table.h"
#include "catalog/database_handle.h"
#include "catalog/tablespace_handle.h"
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
 * db_oid_t, namespace_oid_t, table_oid_t, col_oid_t come from the same global counter, so, inside a database, the
 * values of oids should never be the same.
 *
 * TODO(yangjuns): Each database should have its own global counter
 *
 * NOTE: At this point we don't support varlen. For tables that needs to store strings, we use integers instead.
 *     "terrier"        12345
 *
 *     "pg_database"    10001
 *     "pg_tablespace"  10002
 *     "pg_namespace"   10003
 *     "pg_class"       10004
 *
 *     "pg_global"      20001
 *     "pg_default"     20002
 *
 *     "pg_catalog"     30001
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
   * Return a tablespace handle.
   * @return the tablespace handle
   */
  TablespaceHandle GetTablespaceHandle();

  /**
   * Get the pointer to a catalog in a database by db_oid, including global catalogs.
   *
   * @param db_oid the database the catalog belongs to
   * @param table_oid the table oid of the catalog
   * @return a pointer to the catalog
   * @throw out_of_range exception if either oid doesn't exist or the catalog doesn't exist.
   */
  std::shared_ptr<storage::SqlTable> GetDatabaseCatalog(db_oid_t db_oid, table_oid_t table_oid);

  /**
   * Get the pointer to a catalog in a database by name, including global catalogs.
   *
   * @param db_oid the database the catalog belongs to
   * @param table_name the name of the catalog
   * @return a pointer to the catalog
   * @throw out_of_range exception if either oid doesn't exist or the catalog doesn't exist.
   */
  std::shared_ptr<storage::SqlTable> GetDatabaseCatalog(db_oid_t db_oid, const std::string &table_name);

  /**
   * The global counter for getting next oid. The return result should be converted into corresponding oid type
   *
   * This function is atomic.
   *
   * @return uint32_t the next oid available
   */
  uint32_t GetNextOid();

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
   * Creates pg_database SQL table populates pg_database by inserting a default database row, terrier, into the
   * pg_database table
   * @param txn the bootstrapping transaction
   * @param table_oid the table oid of pg_database
   * @param start_col_oid the starting col oid for columns in pg_database.
   */
  void CreatePGDatabase(transaction::TransactionContext *txn, table_oid_t table_oid);

  /**
   * Creates pg_tablespace SQL table and populates pg_tablespace
   * @param txn the bootstrapping transaction
   * @param table_oid the table oid of pg_tablespace
   * @param start_col_oid the starting col oid for columns in pg_tablespace.
   */
  void CreatePGTablespace(transaction::TransactionContext *txn, table_oid_t table_oid);

  /**
   * Bootstrap a database, i.e create all the catalogs local to this database, and do all other initialization.
   * 1) Create pg_namespace (catalog)
   * 2) Create pg_class (catalog)
   * 3) TODO(pakhtar) -  other catalogs for Postgres compatibility
   * 4) populates these catalogs
   * @param db_oid the oid of the database you are trying to bootstrap
   *
   * Notes:
   * 1) Caller must add the database to pg_database.
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

  void CreatePGNameSpace(transaction::TransactionContext *txn, db_oid_t db_oid);
  void CreatePGClass(transaction::TransactionContext *txn, db_oid_t db_oid);

  /**
   * A dummy call back function for committing bootstrap transaction
   */
  static void BootstrapCallback(void * /*unused*/) {}

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
  // map from (db_oid, catalog name) to sql table
  std::unordered_map<db_oid_t, std::unordered_map<std::string, table_oid_t>> name_map_;
  // this oid serves as a global counter for different strong types of oid
  std::atomic<uint32_t> oid_;
};

extern std::shared_ptr<Catalog> terrier_catalog;

}  // namespace terrier::catalog
