#pragma once
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "catalog/catalog_sql_table.h"
#include "catalog/database_handle.h"
#include "catalog/table_handle.h"
#include "catalog/tablespace_handle.h"
#include "common/strong_typedef.h"
#include "loggers/catalog_logger.h"
#include "storage/sql_table.h"

namespace terrier::catalog {

class DatabaseCatalogTable;
class TablespaceCatalogTable;
class SettingsCatalogTable;

/**
 * Schema column for used/unused schema rows.
 * This is only used to transfer schema definition information to the sql
 * table for Create. Thereafter, the this structure should not be used and
 * schema information can be obtained from the catalog or from
 * sql table GetSchema. Note that col_num is NOT used.
 */
struct SchemaCol {
  /** column no */
  int32_t col_num;
  /** true if used, false if defined only for compatibility */
  bool used;
  /** column name */
  const char *col_name;
  /** column type id */
  type::TypeId type_id;
};

enum CatalogTableType {
  ATTRIBUTE = 0,
  ATTRDEF = 1,
  CLASS = 2,
  DATABASE = 3,
  INDEX = 4,  // future use
  NAMESPACE = 5,
  SETTINGS = 6,
  TABLESPACE = 7,
  TYPE = 8
};

/**
 * The global catalog object.
 *
 * The catalog is modeled upon the Postgres catalog, modified for terrier.
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
 */
class Catalog {
 public:
  /**
   * Creates the (global) catalog object, and bootstraps.
   * A default database is created. Its tables are created and populated.
   *
   * @param txn_manager the global transaction manager
   * @param txn to be used for bootstrapping
   */
  Catalog(transaction::TransactionManager *txn_manager, transaction::TransactionContext *txn);

  /**
   * Create a database, and all its tables. Set initial content
   * for the tables, and update the catalogs to reflect the new
   * database (and its tables).
   *
   * @param txn transaction to use
   * @param name of the database
   */
  void CreateDatabase(transaction::TransactionContext *txn, const std::string &name);

  /**
   * Delete a database.
   *
   * @param txn transaction to use
   * @param db_name of the database
   */
  void DeleteDatabase(transaction::TransactionContext *txn, const std::string &db_name);

  /**
   * Create a Namespace.
   *
   * @param txn transaction to use
   * @param db_oid oid of database in which to create the namespace
   * @param name of the namespaceBootstrapDatabase
   */
  namespace_oid_t CreateNameSpace(transaction::TransactionContext *txn, db_oid_t db_oid, const std::string &name);

  /**
   * Delete a Namespace.
   *
   * @param txn transaction to use
   * @param db_oid oid of database from which to delete the namespace
   * @param ns_oid oid of namespace to delete
   */
  void DeleteNameSpace(transaction::TransactionContext *txn, db_oid_t db_oid, namespace_oid_t ns_oid);

  /**
   * Create a user table with schema
   * @param txn transaction to use
   * @param db_oid oid of the database
   * @param ns_oid namespace oid
   * @param table_name table name
   * @param schema schema to use
   */
  table_oid_t CreateUserTable(transaction::TransactionContext *txn, db_oid_t db_oid, namespace_oid_t ns_oid,
                              const std::string &table_name, const Schema &schema);

  // TODO(pakhtar): these delete just from the catalog tables. Fix to delete sql table too... or
  // rename.
  /**
   * Delete a user table, by name
   * @param txn transaction
   * @param db_oid database oid
   * @param ns_oid namespace oid
   * @param table_name
   */
  void DeleteUserTable(transaction::TransactionContext *txn, db_oid_t db_oid, namespace_oid_t ns_oid,
                       const std::string &table_name);

  /**
   * Delete a user table, by oid
   * @param txn transaction
   * @param db_oid database oid
   * @param ns_oid namespace oid
   * @param tbl_oid table oid
   */
  void DeleteUserTable(transaction::TransactionContext *txn, db_oid_t db_oid, namespace_oid_t ns_oid,
                       table_oid_t tbl_oid);

  /**
   * Return a database handle.
   * @return the database handle
   */
  DatabaseCatalogTable GetDatabaseHandle();

  /**
   * Return a tablespace handle.
   * @return the tablespace handle
   */
  TablespaceCatalogTable GetTablespaceHandle();

  /**
   * Return a settings handle.
   * @return the settings handle
   */
  SettingsCatalogTable GetSettingsHandle();

  /**
   * Get a pointer to a user storage table.
   * @param txn transaction
   * @param db_oid database
   * @param ns_oid namespace
   * @param table_oid table
   * @return a pointer to the Sqltable helper class
   * @throw out_of_range exception if either oid doesn't exist or the catalog doesn't exist. ??
   */
  SqlTableHelper *GetUserTable(transaction::TransactionContext *txn, db_oid_t db_oid, namespace_oid_t ns_oid,
                               table_oid_t table_oid);

  /**
   * Get a pointer to a user storage table.
   * @param txn transaction
   * @param db_oid database
   * @param ns_oid namespace
   * @param name table name
   * @return a pointer to the Sqltable helper class
   * @throw out_of_range exception if either oid doesn't exist or the catalog doesn't exist. ??
   */
  SqlTableHelper *GetUserTable(transaction::TransactionContext *txn, db_oid_t db_oid, namespace_oid_t ns_oid,
                               const std::string &name);

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
  ~Catalog() {
    // TODO(pakhtar): delete all user tables
    // iterate over all databases and delete all system catalog tables
    auto db_oid_map = cat_map_.begin();
    while (db_oid_map != cat_map_.end()) {
      db_oid_t db_oid = db_oid_map->first;
      CATALOG_LOG_DEBUG("Deleting db_oid {}", !db_oid);
      // delete all non-global tables
      DeleteDatabaseTables(db_oid);
      db_oid_map++;
    }
    // delete global tables
    delete pg_database_;
    delete pg_tablespace_;
    delete pg_settings_;
  }

  // methods for catalog initializations
  /**
   * Add a catalog to the catalog mapping
   * @param db_oid database oid
   * @param cttype system catalog table type
   * @param table_p catalog storage table
   */
  void AddToMap(db_oid_t db_oid, CatalogTableType cttype, SqlTableHelper *table_p) {
    cat_map_[db_oid][cttype] = table_p;
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
  void SetUnusedColumns(std::vector<type::TransientValue> *vec, const std::vector<SchemaCol> &cols);

  /**
   * -------------
   * Debug support
   * -------------
   */

  void Dump(transaction::TransactionContext *txn, db_oid_t db_oid);

 protected:
  /**
   * Get a pointer to a catalog storage table helper, by table type. For use ONLY on catalog tables (which are in
   * the pg_catalog namespace).
   *
   * @param db_oid database that owns the table
   * @param cttype catalog table type
   * @return a pointer to the sql table helper for the requested table
   */
  SqlTableHelper *GetCatalogTable(db_oid_t db_oid, CatalogTableType cttype);

 private:
  /**
   * Add a row into pg_database
   */
  void AddEntryToPGDatabase(transaction::TransactionContext *txn, db_oid_t oid, const std::string &name);

  /**
   * Bootstrap all the catalog tables so that new coming transactions can
   * correctly perform SQL queries.
   * 1) It creates and populates all the global catalogs
   * 2) It creates a default database named "terrier"
   * 3) It bootstraps the default database.
   * @param txn to be used for bootstrapping
   */
  void Bootstrap(transaction::TransactionContext *txn);

  /**
   * Create pg_database catalog
   * @param table_oid to set for pg_database
   */
  void CreatePGDatabase(table_oid_t table_oid);

  /**
   * Create pg_tablespace catalog
   * @param db_oid database oid, in which to create pg_tablespace
   * @param table_oid to set for pg_tablespace
   * Note: pg_tablespace is not used (present for compatibility only)
   */
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
  void CreatePGNamespace(transaction::TransactionContext *txn, db_oid_t db_oid);

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
   * During startup, create pg_attrdef table (local to db_oid)
   * @param txn_manager the global transaction manager
   */
  void CreatePGAttrDef(transaction::TransactionContext *txn, db_oid_t db_oid);

  /**
   * During startup, create pg_type table (local to db_oid)
   * @param txn_manager the global transaction manager
   */
  void CreatePGType(transaction::TransactionContext *txn, db_oid_t db_oid);

  void DeleteDatabaseTables(db_oid_t db_oid);

  /**
   * TODO(pakhtar): needs changes.
   * For catalog shutdown.
   * Delete all user created tables.
   * @param oid - database from which tables are to be deleted.
   */
  void DestroyDB(db_oid_t oid);

  /**
   * @param txn transaction
   * @param db_oid
   * @param ns_oid
   * @return TableHandle
   */
  TableCatalogView GetUserTableHandle(transaction::TransactionContext *txn, db_oid_t db_oid, namespace_oid_t ns_oid);

  /**
   * Convert type id (type specified in storage layer schema) to the string used in catalog pg_type to identify
   * a type.
   * @param type_id type id
   * @return type name used by the catalog in pg_type
   */
  std::string ValueTypeIdToSchemaType(type::TypeId type_id);

 private:
  transaction::TransactionManager *txn_manager_;
  // global catalogs
  catalog::SqlTableHelper *pg_database_;
  catalog::SqlTableHelper *pg_tablespace_;
  catalog::SqlTableHelper *pg_settings_;

  // map from db_oid, catalog enum type to sql table rw wrapper. For
  // system catalog tables only.
  std::unordered_map<db_oid_t, std::unordered_map<enum CatalogTableType, catalog::SqlTableHelper *>> cat_map_;

  // all oid types are generated by this global counter
  std::atomic<uint32_t> oid_;

  friend class DatabaseCatalogTable;
  friend class AttrDefCatalogTable;
  friend class NamespaceCatalogTable;
};

extern std::shared_ptr<Catalog> terrier_catalog;

}  // namespace terrier::catalog
