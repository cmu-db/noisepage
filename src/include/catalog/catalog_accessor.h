#pragma once

#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/default_value.h"
#include "catalog/index_key_schema.h"
#include "catalog/schema.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"
#include "type/type_id.h"

namespace terrier::catalog {
class Catalog;

/**
 * A stateful wrapper around the catalog that provides the primary mechanisms
 * for the execution engine to interface with the catalog.  The execution engine
 * should not access the catalog object or the underlying tables directly
 * because that will lock us into the underlying implementation (currently
 * PostgreSQL catalog tables) and makes any future changes or optimizations
 * more difficult.
 *
 * Additionally, the catalog is not responsible for creating or managing
 * anything at the storage layer (except for whatever backs the catalog data).
 * In other words, 'CreateTable' only creates an entry inside of the catalog
 * that gets assigned an OID.  It does not call out to the constructor of
 * 'SqlTable' which is left to the execution engine.  This separation of
 * responsibilities allows for certain policy choices (lazy table instantiation)
 * as well as reinforces the design decision that the catalog is responsible
 * only for managing metadata and not the lifecycle of storage objects.
 */
class CatalogAccessor {
 public:

  /**
   * Given a database name, resolve it to the corresponding OID
   * @param name of the database
   * @return OID for the database, INVALID_DATABASE_OID if the database does not exist
   */
  db_oid_t GetDatabaseOid(const std::string &name);

  /**
   * Given a database name, create a new database entry in the catalog and assign it an OID
   * @param name of the new database
   * @return OID for the database, INVALID_DATABASE_OID if the database already exists
   */
  db_oid_t CreateDatabase(const std::string &name);

  /**
   * Drop all entries in the catalog that belong to the database, including the database entry
   * @param db the OID of the database to drop
   * @return true, unless there was no database entry with the given OID
   */
  bool DropDatabase(db_oid_t db);

  /**
   * Sets the search path of namespaces that should be checked when looking up an
   * index or table by name.
   * @param namespaces the namespaces to search given in priority order
   */
  void SetSearchPath(std::vector<namespace_oid_t> namespaces);

  /**
   * @return the current default namespace (first one in search path)
   */
  namespace_oid_t GetDefaultNamespace() { return search_path_[0]; }

  /**
   * Given a namespace name, resolve it to the corresponding OID
   * @param name of the namespace
   * @return OID of the namespace, INVALID_NAMESPACE_OID if the namespace was not found
   */
  namespace_oid_t GetNamespaceOid(const std::string &name);

  /**
   * Given a namespace name, resolve it to the corresponding OID
   * @param name of the namespace
   * @return OID of the namespace, INVALID_NAMESPACE_OID if the namespace was not found
   */
  namespace_oid_t CreateNamespace(const std::string &name);

  /**
   * Drop all entries in the catalog that belong to the namespace, including the namespace entry
   * @param ns the OID of the namespace to drop
   * @return true, unless there was no namespace entry with the given OID
   */
  bool DropNamespace(namespace_oid_t ns);

  /**
   * Given a table name, resolve it to the corresponding OID
   * @param name of the table
   * @return OID of the table, INVALID_TABLE_OID if the table was not found
   */
  table_oid_t GetTableOid(const std::string &name);

  /**
   * Given a table name and its owning namespace, resolve it to the corresponding OID
   * @param ns in which to search for the table
   * @param name of the table
   * @return OID of the table, INVALID_TABLE_OID if the table was not found
   */
  table_oid_t GetTableOid(namespace_oid_t ns, const std::string &name);

  /**
   * Given a table name, create a new table entry in the catalog and assign it an OID. This
   * function does not instantiate the storage object for the table.
   * @param ns in which the new table will exist
   * @param name of the new table
   * @param columns is the vector of definitions for the columns
   * @return OID for the table, INVALID_TABLE_OID if the table already exists
   */
  table_oid_t CreateTable(namespace_oid_t ns, const std::string &name, std::vector<ColumnDefinition> columns);

  /**
   * Rename the table from its current string to the new one.  The renaming could fail
   * if the table OID is invalid, the new name already exists, or the table entry
   * is write-locked in the catalog.
   * @param table which is to be renamed
   * @param new_table_name is the string of the new name
   * @return whether the renaming was successful.
   *
   * @note This operation will write-lock the table entry until the transaction closes.
   */
  bool RenameTable(table_oid_t table, const std::string &new_table_name);

  /**
   * Drop the table and all corresponding indices from the catalog.
   * @param table the OID of the table to drop
   * @return true, unless there was no table entry for the given OID or the entry
   *         was write-locked by a different transaction
   */
  bool DropTable(table_oid_t table);

  /**
   * Inform the catalog of where the underlying storage for a table is
   * @param table OID in the catalog
   * @param table_ptr to the memory where the storage is
   * @return whether the operation was successful
   */
  bool SetTablePointer(table_oid_t table, storage::SqlTable *table_ptr);

  /**
   * Obtain the storage pointer for a SQL table
   * @param table to which we want the storage object
   * @return the storage object corresponding to the passed OID
   */
  storage::SqlTable *GetTable(table_oid_t table);

  /**
   * Updates the table's schema to include the new columns
   * @param table OID to be modified
   * @param columns that are to be added
   * @return vector of column OIDs returned in the same order as the definitions,
   *         if any column could not be added, all of the values will be INVALID_COLUMN_OID.
   *
   * @note This will increment the schema version number by one, locking the table
   *       entry in the catalog until this is committed
   */
  std::vector<col_oid_t> AddColumns(table_oid_t table, std::vector<ColumnDefinition> columns);

  /**
   * Updates the table's schema to remove the listed columns
   * @param table OID to be modified
   * @param columns that are to be removed
   * @return vector of booleans corresponding to whether each column could be removed,
   *         false indicates the column could not be deleted because the entry was
   *         write-locked by a different process.
   *
   * @note This will increment the schema version number by one, locking the table
   *       entry in the catalog until this is committed.  It will also lock all of
   *        the corresponding column entries.
   */
  std::vector<bool> DropColumns(table_oid_t table, std::vector<col_oid_t> columns);

  /**
   * Updates the table's schema by changing the nullability of the column.  The operation
   * could fail because the column does not exist, the column entry is write-locked by
   * another transaction or because the column already has that nullability setting.
   * @param table OID to be modified
   * @param column that is affected
   * @param nullable is whether the column can be null
   * @return success
   *
   * @note This will increment the schema version number by one, locking the table
   *       entry in the catalog until this is committed.  It will also lock the
   *       column entry in the catalog.
   */
  bool SetColumnNullable(table_oid_t table, col_oid_t column, bool nullable);

  /**
   * Updates the table's schema by changing the data type of the column.  The operation
   * could fail because the column does not exist, the column entry is write-locked by
   * another transaction, or because the column already has that type.
   * @param table OID to be modified
   * @param column that is affected
   * @param new_type for the column
   * @return success
   *
   * @note This will increment the schema version number by one, locking the table
   *       entry in the catalog until this is committed.  It will also lock the
   *       column entry in the catalog.
   */
  bool SetColumnType(table_oid_t table, col_oid_t column, type::TypeId new_type);

  /**
   * Updates the table's schema by changing the default value of the column.  The operation
   * could fail because the column does not exist or the column entry is write-locked by
   * another transaction.
   * @param table OID to be modified
   * @param column that is affected
   * @param default value to be applied
   * @return success
   */
  bool SetColumnDefaultValue(table_oid_t table, col_oid_t column, const DefaultValue &default_value);

  /**
   * Rename the column from its current string to the new one.  The renaming could fail
   * if the column OID is invalid, the new name already exists, or the column entry
   * is write-locked in the catalog.
   * @param table to which the column belongs
   * @param column which is to be renamed
   * @param new_column_name is the string of the new name
   * @return whether the renaming was successful.
   *
   * @note This operation will write-lock the column entry until the transaction closes.
   */
  bool RenameColumn(table_oid_t table, col_oid_t column, const std::string &new_column_name);

  /**
   * @param table corresponding to the requested schema
   * @return the visible schema object for the identified table
   */
  Schema *GetSchema(table_oid_t table);

  /**
   * Given an index name, resolve it to the corresponding OID
   * @param name of the index
   * @return OID of the index, INVALID_INDEX_OID if the index was not found
   */
  index_oid_t GetIndexOid(const std::string &name);

  /**
   * Given an index name and the owning namespace, resolve it to the corresponding OID
   * @param ns in which to search for the index
   * @param name of the index
   * @return OID of the index, INVALID_INDEX_OID if the index was not found
   */
  index_oid_t GetIndexOid(namespace_oid_t ns, const std::string &name);

  /**
   * Given a table, find all indexes for data in that table
   * @param table OID being queried
   * @return vector of index OIDs that reference the queried table
   */
  std::vector<index_oid_t> GetIndexOids(table_oid_t table);

  /**
   * Given the index name and its specification, add it to the catalog
   * @param ns is the namespace in which the index will exist
   * @param table on which this index exists
   * @param name of the index
   * @param constraint type of the index
   * @param keys is a vector of definitions for the individual keys of the index
   * @return OID for the index, INVALID_INDEX_OID if the operation failed
   */
  index_oid_t CreateIndex(namespace_oid_t ns, table_oid_t table, const std::string &name,
                          storage::index::ConstraintType constraint, const std::vector<IndexKeyDefinition> &keys);

  /**
   * @param index corresponding to the requested key schema
   * @return the key schema for this index
   */
  IndexKeySchema *GetKeySchema(index_oid_t index);

  /**
   * Drop the corresponding index from the catalog.
   * @param index to be dropped
   * @return whether the operation succeeded
   */
  bool DropIndex(index_oid_t index);

  /**
   * Inform the catalog of where the underlying implementation of the index is
   * @param index OID in the catalog
   * @param index_ptr to the memory where the index is
   * @return whether the operation was successful
   */
  bool SetIndexPointer(index_oid_t index, storage::index::Index *index_ptr);

  /**
   * Obtain the pointer to the index
   * @param index to which we want a pointer
   * @return the pointer to the index
   */
  storage::index::Index *GetIndex(index_oid_t index);

 private:
  Catalog *catalog_;
  transaction::TransactionContext *txn_;
  db_oid_t db_;
  std::vector<namespace_oid_t> search_path_;

  /**
   * Instantiates a new accessor into the catalog for the given database.
   * @param catalog pointer to the catalog being accessed
   * @param txn the transaction context for this accessor
   * @param database the OID of the database
   */
  CatalogAccessor(Catalog *catalog, transaction::TransactionContext *txn, db_oid_t database);
  friend class Catalog;
};

}  // namespace terrier::catalog
