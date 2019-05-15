#pragma once

#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
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
 *
 * TODO(John):  With this interface in-place, we should probably make the
 * constructors/destructors for Schema and Column private with the catalog
 * classes as the only friends.  However, this may break code (especially
 * tests).
 */
class CatalogAccessor {
 public:
  /**
   * This class decouples the definition of a column from its existence in a
   * schema.  This allows us to delegate all OID assignments to the catalog
   * (rather than requiring the caller to reason about them) and allows us to
   * treat Schema and Column objects as immutable outside of the catalog.
   */
  class ColumnDefinition {
   public:
    /**
     * Instantiates a definition for a fixed-width column
     * @param name column name
     * @param type SQL type for this column (must be a type with fixed-width)
     * @param nullable true if the column is nullable, false otherwise
     */
    ColumnDefinition(std::string name, const type::TypeId type, const bool nullable)
        : name_(std::move(name)), type_(type), nullable_(nullable) {}

    /**
     * Instantiates a definition for a varlen column
     * @param name column name
     * @param type SQL type for this column (must be a type with variable length)
     * @param max_varlen_size the maximum length of the varlen entry
     * @param nullable true if the column is nullable, false otherwise
     */
    ColumnDefinition(std::string name, const type::TypeId type, const uint16_t max_varlen_size, const bool nullable)
        : name_(std::move(name)), type_(type), max_varlen_size_(max_varlen_size), nullable_(nullable) {}

    /**
     * @return the name of the column
     */
    const std::string &GetName() const { return name_; }

    /**
     * @return the SQL type of the column
     */
    const type::TypeId &GetType() const { return type_; }

    /**
     * @return the max size of a varlen entry in this column (only valid for VARLEN columns)
     */
    const uint16_t &GetMaxVarlenSize() const { return max_varlen_size_; }

    /**
     * @return whether the column can be NULL
     */
    const bool &IsNullable() const { return nullable_; }

   private:
    std::string name_;
    type::TypeId type_;
    uint16_t max_varlen_size_;
    bool nullable_;
  };

  /**
   * Given a database name, resolve it to the corresponding OID
   * @param name of the database
   * @return OID for the database, INVALID_DATABASE_OID if the database does not exist
   */
  db_oid_t GetDatabaseOid(std::string name);

  /**
   * Given a database name, create a new database entry in the catalog and assign it an OID
   * @param name of the new database
   * @return OID for the database, INVALID_DATABASE_OID if the database already exists
   */
  db_oid_t CreateDatabase(std::string name);

  /**
   * Drop all entries in the catalog that belong to the database, including the database entry
   * @param db the OID of the database to drop
   * @return true, unless there was no database entry with the given OID
   *
   * @warning This function does not handle deallocation and therefore must only
   * be called after the deallocation of indexes and tables in the database have
   * been scheduled with the GC (or at a minimum the transaction has all necessary
   * references).  Failure to handle this beforehand will cause a memory leak
   * because this call will effectively hide all of these objects from this
   * transaction once invoked.
   */
  bool DropDatabase(db_oid_t db);

  /**
   * Sets the search path of namespaces that should be checked when looking up an
   * index or table by name.
   * @param namespaces the namespaces to search given in priority order
   */
  void SetSearchPath(std::vector<namespace_oid_t> namespaces);

  /**
   * Given a namespace name, resolve it to the corresponding OID
   * @param name of the namespace
   * @return OID of the namespace, INVALID_NAMESPACE_OID if the namespace was not found
   */
  namespace_oid_t GetNamespaceOid(std::string name);

  /**
   * Given a namespace name, resolve it to the corresponding OID
   * @param name of the namespace
   * @return OID of the namespace, INVALID_NAMESPACE_OID if the namespace was not found
   */
  namespace_oid_t CreateNamespace(std::string name);

  /**
   * Drop all entries in the catalog that belong to the namespace, including the namespace entry
   * @param ns the OID of the namespace to drop
   * @return true, unless there was no namespace entry with the given OID
   *
   * @warning This function does not handle deallocation and therefore must only
   * be called after the deallocation of indexes and tables in the namespace have
   * been scheduled with the GC (or at a minimum the transaction has all necessary
   * references).  Failure to handle this beforehand will cause a memory leak
   * because this call will effectively hide all of these objects from this
   * transaction once invoked.
   */
  bool DropNamespace(namespace_oid_t ns);

  /**
   * Given a table name, resolve it to the corresponding OID
   * @param name of the table
   * @return OID of the table, INVALID_TABLE_OID if the table was not found
   */
  table_oid_t GetTableOid(std::string name);

  /**
   * Given a table name and its owning namespace, resolve it to the corresponding OID
   * @param ns in which to search for the table
   * @param name of the table
   * @return OID of the table, INVALID_TABLE_OID if the table was not found
   */
  table_oid_t GetTableOid(namespace_oid_t ns, std::string name);

  /**
   * Given a table name, create a new table entry in the catalog and assign it an OID. This
   * function does not instantiate the storage object for the table.
   * @param ns in which the new table will exist
   * @param name of the new table
   * @param columns is the vector of definitions for the columns
   * @return OID for the table, INVALID_TABLE_OID if the table already exists
   */
  table_oid_t CreateTable(namespace_oid_t ns, std::string table_name, std::vector<ColumnDefinition> columns);

  /**
   * Rename the table from its current string to the new one.  The renaming could fail
   * if the table OID is invalid, the new name already exists, or the table entry
   * is write-locked in the catalog.
   * @param table which is to be renamed
   * @param new_table_name is the string of the new name
   * @param whether the renaming was successful.
   *
   * @note This operation will write-lock the table entry until the transaction closes.
   */
  bool RenameTable(table_oid_t table, std::string new_table_name);

  /**
   * Drop the table and all corresponding indices from the catalog.
   * @param table the OID of the table to drop
   * @return true, unless there was no table entry for the given OID or the entry
   *         was write-locked by a different transaction
   *
   * @warning This function does not handle deallocation and therefore must only
   * be called after the deallocation of indexes and and the table have been
   * scheduled with the GC (or at a minimum the transaction has all necessary
   * references).  Failure to handle this beforehand will cause a memory leak
   * because this call will effectively hide all of these objects from this
   * transaction once invoked.
   */
  bool DropTable(table_oid_t table);

  /**
   * Inform the catalog of where the underlying storage for a table is
   * @param table OID in the catalog
   * @param table_ptr to the memory where the storage is
   * @return whether the operation was successful
   *
   * TODO(John): Should these pointers only be set once?  If so, then we should
   * update "return" to state that false will be returned when the pointer was already set.
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
   * @return success
   *
   * @note This will increment the schema version number by one, locking the table
   *       entry in the catalog until this is committed.  It will also lock the
   *       column entry in the catalog.
   */
  bool SetColumnNullable(table_oid_t table, col_oid_t column, bool nullable);
  // bool SetColumnType(table_oid_t table, col_oid_t column, type::TypeId new_type);

  /**
   * Rename the column from its current string to the new one.  The renaming could fail
   * if the column OID is invalid, the new name already exists, or the column entry
   * is write-locked in the catalog.
   * @param table to which the column belongs
   * @param column which is to be renamed
   * @param new_column_name is the string of the new name
   * @param whether the renaming was successful.
   *
   * @note This operation will write-lock the column entry until the transaction closes.
   */
  bool RenameColumn(table_oid_t table, col_oid_t column, std::string new_column_name);
  // bool SetColumnDefaultValue(table_oid_t table, col_oid_t column, DefaultValue default);

  /**
   * @return the visible schema object for the identified table
   */
  const Schema &GetSchema(table_oid_t table);

  /**
   * Given an index name, resolve it to the corresponding OID
   * @param name of the index
   * @return OID of the index, INVALID_INDEX_OID if the index was not found
   */
  index_oid_t GetIndexOid(std::string name);

  /**
   * Given an index name and the owning namespace, resolve it to the corresponding OID
   * @param ns in which to search for the index
   * @param name of the index
   * @return OID of the index, INVALID_INDEX_OID if the index was not found
   */
  index_oid_t GetIndexOid(namespace_oid_t ns, std::string name);

  /**
   * Given a table, find all indexes for data in that table
   * @param table OID being queried
   * @return vector of index OIDs that reference the queried table
   *
   * TODO(John): Should this return the actual index pointers as well?
   */
  std::vector<index_oid_t> GetIndexOIDs(table_oid_t table);
  // index_oid_t CreateIndex(namespace_oid_t namespace, std::string index, [[constructor stuff]])

  /**
   * Drop the corresponding index from the catalog.
   * @param index to be dropped
   * @return whether the operation succeeded
   *
   * @warning This operation does not handle deallocation of the index.  The
   *          caller is responsible for scheduling the indexes deallocation with
   *          the GC if this is successful and commits.  Additionally, the caller
   *          should have a handle to the index prior to calling this function
   *          because they will not be able to use 'GetIndex' to retrieve it
   *          if this call succeeds.
   */
  bool DropIndex(index_oid_t index);

  /**
   * Inform the catalog of where the underlying implementation of the index is
   * @param index OID in the catalog
   * @param index_ptr to the memory where the index is
   * @return whether the operation was successful
   *
   * TODO(John): Should these pointers only be set once?  If so, then we should update
   * "return" to state that false will be returned when the pointer was already set.
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
  db_oid_t db_;
  transaction::TransactionContext *txn_;
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
