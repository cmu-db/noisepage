#pragma once
#include <memory>
#include <string>
#include <unordered_map>
#include "catalog/catalog_defs.h"
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
 *     "pg_tablespce"   10002
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
   * Initializes catalog object, and automatically starts the bootstrapping process
   * @param txn_manager the global transaction manager
   */
  explicit Catalog(transaction::TransactionManager *txn_manager);

  /**
   * Return a database handle for given db_oid.
   * @param db_oid the given db_oid
   * @return the corresponding database handle
   */
  DatabaseHandle GetDatabaseHandle(db_oid_t db_oid);

  /**
   * Return a database handle for given db_oid.
   * @param db_oid the given db_oid
   * @return the corresponding database handle
   */
  TablespaceHandle GetTablespaceHandle();

  /**
   * Get the pointer to a catalog in a database by db_oid, including global catalogs.
   * @param db_oid the database the catalog belongs to
   * @param table_oid the table oid of the catalog
   * @return a pointer to the catalog
   */
  std::shared_ptr<storage::SqlTable> GetDatabaseCatalog(db_oid_t db_oid, table_oid_t table_oid);

  /**
   * Get the pointer to a catalog in a database by name, including global catalogs.
   * @param db_oid the database the catalog belongs to
   * @param table_name the name of the catalog
   * @return a pointer to the catalog
   */
  std::shared_ptr<storage::SqlTable> GetDatabaseCatalog(db_oid_t db_oid, const std::string &table_name);

  /**
   * Get the next database_oid
   * @return next database_oid
   */
  db_oid_t GetNextDBOid();

  /**
   * Get the next namespace_oid
   * @return next namespace_oid
   */
  namespace_oid_t GetNextNamepsaceOid();

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

  uint32_t GetNextOid();

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

  void CreatePGNameSpace(transaction::TransactionContext *txn, db_oid_t db_oid);

  void CreatePGClass(transaction::TransactionContext *txn, db_oid_t db_oid);

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
  std::shared_ptr<storage::SqlTable> pg_tablespace_;
  // map from (db_oid, catalog table_oid_t) to sql table
  std::unordered_map<db_oid_t, std::unordered_map<table_oid_t, std::shared_ptr<storage::SqlTable>>> map_;
  // map from (db_oid, catalog name) to sql table
  std::unordered_map<db_oid_t, std::unordered_map<std::string, table_oid_t>> name_map_;
  // this oid serves as a global counter for different strong types of oid
  std::atomic<uint32_t> oid_;
};

extern std::shared_ptr<Catalog> terrier_catalog;

}  // namespace terrier::catalog
