#pragma once

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/database_catalog.h"
#include "catalog/index_schema.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/schema.h"
#include "common/managed_pointer.h"
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
  db_oid_t GetDatabaseOid(std::string name) const;

  /**
   * Given a database name, create a new database entry in the catalog and assign it an OID
   * @param name of the new database
   * @return OID for the database, INVALID_DATABASE_OID if the database already exists
   */
  db_oid_t CreateDatabase(std::string name) const;

  /**
   * Drop all entries in the catalog that belong to the database, including the database entry
   * @param db the OID of the database to drop
   * @return true, unless there was no database entry with the given OID
   */
  bool DropDatabase(db_oid_t db) const;

  /**
   * Sets the search path of namespaces that should be checked when looking up an
   * index or table by name.
   * @param namespaces the namespaces to search given in priority order
   */
  void SetSearchPath(std::vector<namespace_oid_t> namespaces);

  /**
   * @return the current default namespace (first one in search path)
   */
  namespace_oid_t GetDefaultNamespace() const { return default_namespace_; }

  /**
   * Given a namespace name, resolve it to the corresponding OID
   * @param name of the namespace
   * @return OID of the namespace, INVALID_NAMESPACE_OID if the namespace was not found
   */
  namespace_oid_t GetNamespaceOid(std::string name) const;

  /**
   * Given a namespace name, resolve it to the corresponding OID
   * @param name of the namespace
   * @return OID of the namespace, INVALID_NAMESPACE_OID if the namespace was not found
   */
  namespace_oid_t CreateNamespace(std::string name) const;

  /**
   * Drop all entries in the catalog that belong to the namespace, including the namespace entry
   * @param ns the OID of the namespace to drop
   * @return true, unless there was no namespace entry with the given OID
   */
  bool DropNamespace(namespace_oid_t ns) const;

  /**
   * Given a table name, resolve it to the corresponding OID
   * @param name of the table
   * @return OID of the table, INVALID_TABLE_OID if the table was not found
   */
  table_oid_t GetTableOid(std::string name) const;

  /**
   * Given a table name and its owning namespace, resolve it to the corresponding OID
   * @param ns in which to search for the table
   * @param name of the table
   * @return OID of the table, INVALID_TABLE_OID if the table was not found
   */
  table_oid_t GetTableOid(namespace_oid_t ns, std::string name) const;

  /**
   * Given a table name, create a new table entry in the catalog and assign it an OID. This
   * function does not instantiate the storage object for the table.
   * @param ns in which the new table will exist
   * @param name of the new table
   * @param schema object describing the new table
   * @return OID for the table, INVALID_TABLE_OID if the table already exists
   * @warning The catalog accessor assumes it takes ownership of the schema object
   * that is passed.  As such, there is no guarantee that the pointer is still
   * valid when this function returns.  If the caller needs to reference the
   * schema object after this call, they should use the GetSchema function to
   * obtain the authoritative schema for this table.
   */
  table_oid_t CreateTable(namespace_oid_t ns, std::string name, const Schema &schema) const;

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
  bool RenameTable(table_oid_t table, std::string new_table_name) const;

  /**
   * Drop the table and all corresponding indices from the catalog.
   * @param table the OID of the table to drop
   * @return true, unless there was no table entry for the given OID or the entry
   *         was write-locked by a different transaction
   */
  bool DropTable(table_oid_t table) const;

  /**
   * Inform the catalog of where the underlying storage for a table is
   * @param table OID in the catalog
   * @param table_ptr to the memory where the storage is
   * @return whether the operation was successful
   * @warning The table pointer that is passed in must be on the heap as the
   * catalog will take ownership of it and schedule its deletion with the GC
   * at the appropriate time.
   */
  bool SetTablePointer(table_oid_t table, storage::SqlTable *table_ptr) const;

  /**
   * Obtain the storage pointer for a SQL table
   * @param table to which we want the storage object
   * @return the storage object corresponding to the passed OID
   */
  common::ManagedPointer<storage::SqlTable> GetTable(table_oid_t table) const;

  /**
   * Apply a new schema to the given table.  The changes should modify the latest
   * schema as provided by the catalog.  There is no guarantee that the OIDs for
   * modified columns will be stable across a schema change.
   * @param table OID of the modified table
   * @param new_schema object describing the table after modification
   * @return true if the operation succeeded, false otherwise
   * @warning The catalog accessor assumes it takes ownership of the schema object
   * that is passed.  As such, there is no guarantee that the pointer is still
   * valid when this function returns.  If the caller needs to reference the
   * schema object after this call, they should use the GetSchema function to
   * obtain the authoritative schema for this table.
   */
  bool UpdateSchema(table_oid_t table, Schema *new_schema) const;

  /**
   * Get the visible schema describing the table.
   * @param table corresponding to the requested schema
   * @return the visible schema object for the identified table
   */
  const Schema &GetSchema(table_oid_t table) const;

  /**
   * A list of all constraints on this table
   * @param table being queried
   * @return vector of OIDs for all of the constraints that apply to this table
   */
  std::vector<constraint_oid_t> GetConstraints(table_oid_t table) const;

  /**
   * A list of all indexes on the given table
   * @param table being queried
   * @return vector of OIDs for all of the indexes on this table
   */
  std::vector<index_oid_t> GetIndexes(table_oid_t table) const;

  /**
   * Given an index name, resolve it to the corresponding OID
   * @param name of the index
   * @return OID of the index, INVALID_INDEX_OID if the index was not found
   */
  index_oid_t GetIndexOid(std::string name) const;

  /**
   * Given an index name and the owning namespace, resolve it to the corresponding OID
   * @param ns in which to search for the index
   * @param name of the index
   * @return OID of the index, INVALID_INDEX_OID if the index was not found
   */
  index_oid_t GetIndexOid(namespace_oid_t ns, std::string name) const;

  /**
   * Given a table, find all indexes for data in that table
   * @param table OID being queried
   * @return vector of index OIDs that reference the queried table
   */
  std::vector<index_oid_t> GetIndexOids(table_oid_t table) const;

  /**
   * Given the index name and its specification, add it to the catalog
   * @param ns is the namespace in which the index will exist
   * @param table on which this index exists
   * @param name of the index
   * @param schema describing the new index
   * @return OID for the index, INVALID_INDEX_OID if the operation failed
   */
  index_oid_t CreateIndex(namespace_oid_t ns, table_oid_t table, std::string name, const IndexSchema &schema) const;

  /**
   * Gets the schema that was used to define the index
   * @param index corresponding to the requested key schema
   * @return the key schema for this index
   */
  const IndexSchema &GetIndexSchema(index_oid_t index) const;

  /**
   * Drop the corresponding index from the catalog.
   * @param index to be dropped
   * @return whether the operation succeeded
   */
  bool DropIndex(index_oid_t index) const;

  /**
   * Inform the catalog of where the underlying implementation of the index is
   * @param index OID in the catalog
   * @param index_ptr to the memory where the index is
   * @return whether the operation was successful
   * @warning The index pointer that is passed in must be on the heap as the
   * catalog will take ownership of it and schedule its deletion with the GC
   * at the appropriate time.
   */
  bool SetIndexPointer(index_oid_t index, storage::index::Index *index_ptr) const;

  /**
   * Obtain the pointer to the index
   * @param index to which we want a pointer
   * @return the pointer to the index
   */
  common::ManagedPointer<storage::index::Index> GetIndex(index_oid_t index) const;

  /**
   * Instantiates a new accessor into the catalog for the given database.
   * @param catalog pointer to the catalog being accessed
   * @param dbc pointer to the database catalog being accessed
   * @param txn the transaction context for this accessor
   * @warning This constructor should never be called directly.  Instead you should get accessors from the catalog.
   */
  CatalogAccessor(common::ManagedPointer<Catalog> catalog, common::ManagedPointer<DatabaseCatalog> dbc,
                  transaction::TransactionContext *txn)
      : catalog_(catalog),
        dbc_(dbc),
        txn_(txn),
        search_path_({NAMESPACE_CATALOG_NAMESPACE_OID, NAMESPACE_DEFAULT_NAMESPACE_OID}),
        default_namespace_(NAMESPACE_DEFAULT_NAMESPACE_OID) {}

 private:
  const common::ManagedPointer<Catalog> catalog_;
  const common::ManagedPointer<DatabaseCatalog> dbc_;
  transaction::TransactionContext *const txn_;
  std::vector<namespace_oid_t> search_path_;
  namespace_oid_t default_namespace_;

  /**
   * A helper function to ensure that user-defined object names are standardized prior to doing catalog operations
   * @param name of object that should be sanitized/normalized
   */
  static void NormalizeObjectName(std::string *name) {
    std::transform(name->begin(), name->end(), name->begin(), [](auto &&c) { return std::tolower(c); });
  }
};

}  // namespace terrier::catalog
