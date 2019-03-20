#pragma once
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "catalog/catalog_sql_table.h"
#include "catalog/database_handle.h"
#include "catalog/tablespace_handle.h"
#include "common/strong_typedef.h"
#include "loggers/catalog_logger.h"
#include "storage/sql_table.h"

namespace terrier::catalog {

class DatabaseHandle;
class TablespaceHandle;
class SettingsHandle;

/**
 * Schema column for used/unused schema rows.
 */
struct SchemaCol {
  /** column no */
  int32_t col_num;
  /** column name */
  const char *col_name;
  /** column type id */
  type::TypeId type_id;
};

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
 * TODO(Yesheng): Port over to TransientValue
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
   * Create a database (no tables are created). Insert the name into
   * the catalogs and setup everything related.
   *
   * @param txn transaction to use
   * @param name of the database
   */
  void CreateDatabase(transaction::TransactionContext *txn, const char *name);

  /**
   * Delete a database.
   *
   * @param txn transaction to use
   * @param db_name of the database
   */
  void DeleteDatabase(transaction::TransactionContext *txn, const char *db_name);

  /**
   * Create a table with schema
   * @param txn transaction to use
   * @param db_oid oid of the database
   * @param table_name table name
   * @param schema schema to use
   */
  void CreateTable(transaction::TransactionContext *txn, db_oid_t db_oid, const std::string &table_name,
                   const Schema &schema);

  /**
   * Lookup a database oid and return a database handle.
   * @return the corresponding database handle
   */
  DatabaseHandle GetDatabaseHandle();

  /**
   * Return a tablespace handle.
   * @return the tablespace handle
   */
  TablespaceHandle GetTablespaceHandle();

  /**
   * Return a tablespace handle.
   * @return the tablespace handle
   */
  SettingsHandle GetSettingsHandle();

  /**
   * Get the pointer to a catalog in a database by db_oid, including global catalogs.
   *
   * @param db_oid the database the catalog belongs to
   * @param table_oid the table oid of the catalog
   * @return a pointer to the catalog
   * @throw out_of_range exception if either oid doesn't exist or the catalog doesn't exist.
   */
  std::shared_ptr<catalog::SqlTableRW> GetDatabaseCatalog(db_oid_t db_oid, table_oid_t table_oid);

  /**
   * Get the pointer to a catalog in a database by name, including global catalogs.
   *
   * @param db_oid the database the catalog belongs to
   * @param table_name the name of the catalog
   * @return a pointer to the catalog
   * @throw out_of_range exception if either oid doesn't exist or the catalog doesn't exist.
   */
  std::shared_ptr<catalog::SqlTableRW> GetDatabaseCatalog(db_oid_t db_oid, const std::string &table_name);

  /**
   * The global counter for getting next oid. The return result should be converted into corresponding oid type
   *
   * This function is atomic.
   *
   * @return uint32_t the next oid available
   */
  uint32_t GetNextOid();

  /*
   * Destructor
   */
  ~Catalog() = default;

  //  ~Catalog() {
  //    // destroy all DB
  //    // DestroyDB(DEFAULT_DATABASE_OID);
  //  }

  // methods for catalog initializations
  /**
   * Add a catalog to the catalog mapping
   * @param db_oid database oid
   * @param table_oid table oid
   * @param name name of the catalog
   * @param table_rw_p catalog storage table
   */
  void AddToMaps(db_oid_t db_oid, table_oid_t table_oid, const std::string &name,
                 std::shared_ptr<SqlTableRW> table_rw_p) {
    map_[db_oid][table_oid] = std::move(table_rw_p);
    name_map_[db_oid][name] = table_oid;
  }

  /**
   * Utility function for adding columns in a table to pg_attribute. To use this function, pg_attribute has to exist.
   * @param txn the transaction that's adding the columns
   * @param db_oid the database the pg_attribute belongs to
   * @param table the table which the columns belong to
   */
  void AddColumnsToPGAttribute(transaction::TransactionContext *txn, db_oid_t db_oid,
                               const std::shared_ptr<storage::SqlTable> &table);

  /**
   * Set values for unused columns.
   * @param vec append to this vector of values
   * @param cols vector of column types
   */
  void SetUnusedColumns(std::vector<type::Value> *vec, const std::vector<SchemaCol> &cols);

  /**
   * Convert type id to schema type
   * @param type_id type id
   * @return schema type
   */
  type::Value ValueTypeIdToSchemaType(type::TypeId type_id);

  /**
   * -------------
   * Debug support
   * -------------
   */

  void Dump(transaction::TransactionContext *txn);

 private:
  /**
   * Add a row into pg_database
   */
  void AddEntryToPGDatabase(transaction::TransactionContext *txn, db_oid_t oid, const char *name);

  /**
   * Add columns created for Postgres compatibility, but unused, to the schema
   * @param db_p - shared_ptr to database
   * @param cols - vector specifying the columns
   *
   */
  void AddUnusedSchemaColumns(const std::shared_ptr<catalog::SqlTableRW> &db_p, const std::vector<SchemaCol> &cols);

  /**
   * Bootstrap all the catalog tables so that new coming transactions can
   * correctly perform SQL queries.
   * 1) It creates and populates all the global catalogs
   * 2) It creates a default database named "terrier"
   * 2) It bootstraps the default database.
   */
  void Bootstrap();

  void CreatePGDatabase(table_oid_t table_oid);

  void CreatePGTablespace(table_oid_t table_oid);

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
   * Add initial contents to pg_database, during startup.
   * @param txn_manager the global transaction manager
   */
  void PopulatePGDatabase(transaction::TransactionContext *txn);

  /**
   * Add initial contents to pg_tablespace, during startup.
   * @param txn_manager the global transaction manager
   */
  void PopulatePGTablespace(transaction::TransactionContext *txn);

  /**
   * During startup, create pg_namespace table (local to db_oid)
   * @param txn_manager the global transaction manager
   */
  void CreatePGNameSpace(transaction::TransactionContext *txn, db_oid_t db_oid);

  /**
   * During startup, create pg_class table (local to db_oid)
   * @param txn_manager the global transaction manager
   */
  void CreatePGClass(transaction::TransactionContext *txn, db_oid_t db_oid);

  /**
   * During startup, create pg_attribute table (local to db_oid)
   * @param txn_manager the global transaction manager
   */
  void CreatePGAttribute(transaction::TransactionContext *txn, db_oid_t db_oid);

  /**
   * During startup, create pg_type table (local to db_oid)
   * @param txn_manager the global transaction manager
   */
  void CreatePGType(transaction::TransactionContext *txn, db_oid_t db_oid);

  /**
   * For catalog shutdown.
   * Delete all user created tables.
   * @param oid - database from which tables are to be deleted.
   */
  void DestroyDB(db_oid_t oid);

 private:
  transaction::TransactionManager *txn_manager_;
  // global catalogs
  std::shared_ptr<catalog::SqlTableRW> pg_database_;
  std::shared_ptr<catalog::SqlTableRW> pg_tablespace_;
  std::shared_ptr<catalog::SqlTableRW> pg_settings_;

  // map from (db_oid, catalog table_oid_t) to sql table rw wrapper
  std::unordered_map<db_oid_t, std::unordered_map<table_oid_t, std::shared_ptr<catalog::SqlTableRW>>> map_;
  // map from (db_oid, catalog name) to table_oid
  std::unordered_map<db_oid_t, std::unordered_map<std::string, table_oid_t>> name_map_;
  // this oid serves as a global counter for different strong types of oid
  std::atomic<uint32_t> oid_;

  /**
   * pg_database specific items. Should be in a pg_database util class
   */

  std::vector<SchemaCol> pg_tablespace_unused_cols_ = {{2, "spcowner", type::TypeId::INTEGER},
                                                       {3, "spcacl", type::TypeId::VARCHAR},
                                                       {4, "spcoptions", type::TypeId::VARCHAR}};
};

extern std::shared_ptr<Catalog> terrier_catalog;

}  // namespace terrier::catalog
