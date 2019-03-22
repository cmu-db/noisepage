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
 * The global catalog object.
 *
 * The catalog is modelled upon the Postgres catalog, modified for terrier.
 *
 * pg_database, pg_tablespace and pg_settings are global catalogs
 * (only a single instance). Other catalogs are "local" to each database.
 * For example, a pg_class catalog exists for each database.
 *
 * OID assignment:
 * In terrier, each table row is identified by an oid. Further, as strong
 * typing is used in terrier, each (catalog) table has it's own oid type,
 * e.g. db_oid_t, namespace_oid_t etc.
 *
 * The values for all oid types are generated from a single value space
 * i.e. every oid regardless of type, is unique.
 *
 * TODO(Yesheng): Port over to TransientValue
 */
class Catalog {
 public:
  /**
   * Creates the (global) catalog object, and bootstraps.
   * A default database is created. Its tables are created and populated.
   *
   * @param txn_manager the global transaction manager
   */
  explicit Catalog(transaction::TransactionManager *txn_manager);

  /**
   * Create a database, and all its tables. Set initial content
   * for the tables, and update the catalogs to reflect the new
   * database (and its tables).
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
   * Delete a table
   * @param txn transaction to use
   * @param db_oid oid of the database
   * @param table_oid table to delete
   */

  void DeleteTable(transaction::TransactionContext *txn, db_oid_t db_oid, table_oid_t table_oid);

  /**
   * Return a database handle.
   * @return the database handle
   */
  DatabaseHandle GetDatabaseHandle();

  /**
   * Return a tablespace handle.
   * @return the tablespace handle
   */
  TablespaceHandle GetTablespaceHandle();

  /**
   * Return a settings handle.
   * @return the settings handle
   */
  SettingsHandle GetSettingsHandle();

  /**
   * Get a pointer to the storage table.
   * Supports both catalog tables, and user created tables.
   *
   * @param db_oid database that owns the table
   * @param table_oid returns the storage table pointer for this table_oid
   * @return a pointer to the catalog
   * @throw out_of_range exception if either oid doesn't exist or the catalog doesn't exist.
   */
  std::shared_ptr<catalog::SqlTableRW> GetDatabaseCatalog(db_oid_t db_oid, table_oid_t table_oid);

  /**
   * Get a pointer to the storage table, by table_name.
   * Supports both catalog tables, and user created tables.
   *
   * @param db_oid database that owns the table
   * @param table_name returns the storage table point for this table
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

  /**
   * TODO(pakhtar): Implement shutdown.
   */

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
   * @param name of the catalog
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

  void Dump(transaction::TransactionContext *txn, db_oid_t db_oid);

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

  void CreatePGTablespace(db_oid_t db_oid, table_oid_t table_oid);

  /**
   * Initialize a database. This:
   * 1) Creates all the catalogs "local" to this database, e.g. pg_namespace
   * 2) Initializes their contents
   * 3) Adds them to the catalog maps
   * @param db_oid the oid of the database to be initialized
   *
   * Notes:
   * - Caller is responsible for adding the database to pg_database,
   *   and for adding it to the catalog maps.
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
   * Note:
   * - pg_tablespace is currently not used in terrier
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
   * TODO(pakhtar): needs changes.
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
};

extern std::shared_ptr<Catalog> terrier_catalog;

}  // namespace terrier::catalog
