#pragma once

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/postgres/pg_proc.h"
#include "catalog/schema.h"
#include "common/managed_pointer.h"
#include "type/type_id.h"

namespace noisepage::storage {
class SqlTable;
namespace index {
class Index;
}
}  // namespace noisepage::storage

namespace noisepage::execution::functions {
class FunctionContext;
}

namespace noisepage::transaction {
class TransactionContext;
}

namespace noisepage::catalog {
class Catalog;
class DatabaseCatalog;
class CatalogCache;
class IndexSchema;

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
class EXPORT CatalogAccessor {
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
   * @param ns the OID of the namespace to drop, this must be a valid oid from GetNamespaceOid. Invalid input will
   * trigger an assert
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
   * @param table the OID of the table to drop, this must be a valid oid from GetTableOid. Invalid input will trigger an
   * assert
   * @return true, unless the entry was write-locked by a different transaction
   */
  bool DropTable(table_oid_t table) const;

  /**
   * Inform the catalog of where the underlying storage for a table is
   * @param table OID in the catalog, this must be a valid oid from GetTableOid. Invalid input will trigger an assert
   * @param table_ptr to the memory where the storage is
   * @return whether the operation was successful
   * @warning The table pointer that is passed in must be on the heap as the
   * catalog will take ownership of it and schedule its deletion with the GC
   * at the appropriate time.
   */
  bool SetTablePointer(table_oid_t table, storage::SqlTable *table_ptr) const;

  /**
   * Obtain the storage pointer for a SQL table
   * @param table to which we want the storage object, this must be a valid oid from GetTableOid. Invalid input will
   * trigger an assert
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
   * @warning If the caller needs to reference the schema object after this call, they should use the GetSchema function
   * to obtain the authoritative schema for this table.
   */
  bool UpdateSchema(table_oid_t table, Schema *new_schema) const;

  /**
   * Get the visible schema describing the table.
   * @param table corresponding to the requested schema, this must be a valid oid from GetTableOid. Invalid input will
   * trigger an assert
   * @return the visible schema object for the identified table
   */
  const Schema &GetSchema(table_oid_t table) const;

  /**
   * A list of all constraints on this table
   * @param table being queried, this must be a valid oid from GetTableOid. Invalid input will trigger an assert
   * @return vector of OIDs for all of the constraints that apply to this table
   */
  std::vector<constraint_oid_t> GetConstraints(table_oid_t table) const;

  /**
   * A list of all indexes on the given table
   * @param table being queried
   * @return vector of OIDs for all of the indexes on this table
   */
  std::vector<index_oid_t> GetIndexOids(table_oid_t table) const;

  /**
   * Returns index pointers and schemas for every index on a table. Provides much better performance than individual
   * calls to GetIndex and GetIndexSchema
   * @param table table to get index objects for, this must be a valid oid from GetTableOid. Invalid input will trigger
   * an assert
   * @return vector of pairs of index pointers and their corresponding schemas
   */
  std::vector<std::pair<common::ManagedPointer<storage::index::Index>, const IndexSchema &>> GetIndexes(
      table_oid_t table);

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
   * @param index corresponding to the requested key schema, this must be a valid oid from GetIndexOid. Invalid input
   * will trigger an assert
   * @return the key schema for this index
   */
  const IndexSchema &GetIndexSchema(index_oid_t index) const;

  /**
   * Drop the corresponding index from the catalog.
   * @param index to be dropped, this must be a valid oid from GetIndexOid. Invalid input will trigger an assert
   * @return whether the operation succeeded
   */
  bool DropIndex(index_oid_t index) const;

  /**
   * Inform the catalog of where the underlying implementation of the index is
   * @param index OID in the catalog, this must be a valid oid from GetIndexOid. Invalid input will trigger an assert
   * @param index_ptr to the memory where the index is
   * @return whether the operation was successful
   * @warning The index pointer that is passed in must be on the heap as the
   * catalog will take ownership of it and schedule its deletion with the GC
   * at the appropriate time.
   */
  bool SetIndexPointer(index_oid_t index, storage::index::Index *index_ptr) const;

  /**
   * Obtain the pointer to the index
   * @param index to which we want a pointer, this must be a valid oid from GetIndexOid. Invalid input will trigger an
   * assert
   * @return the pointer to the index
   */
  common::ManagedPointer<storage::index::Index> GetIndex(index_oid_t index) const;

  /**
   * Adds a language to the catalog (with default parameters for now) if
   * it doesn't exist in pg_language already
   * @param lanname name of language
   * @return oid of added language if it didn't exist before or INVALID_LANGUAGE_OID if else
   */
  language_oid_t CreateLanguage(const std::string &lanname);

  /**
   * Gets a language's oid from the catalog if it exists in pg_language
   * @param lanname name of language
   * @return oid of requested language if it exists or INVALID_LANGUAGE_OID if else
   */
  language_oid_t GetLanguageOid(const std::string &lanname);

  /**
   * Drops a language from the catalog
   * @param language_oid oid of the language to drop
   * @return true iff the langauge was successfully found and dropped
   */
  bool DropLanguage(language_oid_t language_oid);

  /**
   * Creates a procedure for the pg_proc table
   * @param procname name of process to add
   * @param language_oid oid of language this process is written in
   * @param procns namespace of process to add
   * @param args names of arguments to this proc
   * @param arg_types types of arguments to this proc in the same order as in args (only for in and inout
   *        arguments)
   * @param all_arg_types types of all arguments
   * @param arg_modes modes of arguments in the same order as in args
   * @param rettype oid of the type of return value
   * @param src source code of proc
   * @param is_aggregate true iff this is an aggregate procedure
   * @return oid of created proc entry
   * @warning does not support variadics yet
   */
  proc_oid_t CreateProcedure(const std::string &procname, language_oid_t language_oid, namespace_oid_t procns,
                             const std::vector<std::string> &args, const std::vector<type_oid_t> &arg_types,
                             const std::vector<type_oid_t> &all_arg_types,
                             const std::vector<postgres::PgProc::ArgModes> &arg_modes, type_oid_t rettype,
                             const std::string &src, bool is_aggregate);

  /**
   * Drops a procedure from the pg_proc table
   * @param proc_oid oid of process to drop
   * @return true iff the process was successfully found and dropped
   */
  bool DropProcedure(proc_oid_t proc_oid);

  /**
   * Gets the oid of a procedure from pg_proc given a requested name and namespace
   * This lookup will return the first one found through a sequential scan through
   * the current search path
   * @param procname name of the proc to lookup
   * @param all_arg_types vector of types of arguments of procedure to look up
   * @return the oid of the found proc if found else INVALID_PROC_OID
   */
  proc_oid_t GetProcOid(const std::string &procname, const std::vector<type_oid_t> &all_arg_types);

  /**
   * Sets the proc context pointer column of proc_oid to func_context
   * @param proc_oid The proc_oid whose pointer column we are setting here
   * @param func_context The context object to set to
   * @return False if the given proc_oid is invalid, True if else
   */
  bool SetFunctionContextPointer(proc_oid_t proc_oid, const execution::functions::FunctionContext *func_context);

  /**
   * Gets the proc context pointer column of proc_oid
   * @param proc_oid The proc_oid whose pointer column we are getting here
   * @return nullptr if proc_oid is either invalid or there is no context object set for this proc_oid
   */
  common::ManagedPointer<execution::functions::FunctionContext> GetFunctionContext(proc_oid_t proc_oid);

  /**
   * Returns the type oid of the given TypeId in pg_type
   * @param type
   * @return type_oid of type in pg_type
   */
  type_oid_t GetTypeOidFromTypeId(type::TypeId type);

  /**
   * @return BlockStore to be used for CREATE operations
   */
  common::ManagedPointer<storage::BlockStore> GetBlockStore() const;

  /**
   * @return managed pointer to transaction context
   */
  common::ManagedPointer<transaction::TransactionContext> GetTxn() const { return txn_; }

  /**
   * Instantiates a new accessor into the catalog for the given database.
   * @param catalog pointer to the catalog being accessed
   * @param dbc pointer to the database catalog being accessed
   * @param txn the transaction context for this accessor
   * @param cache CatalogCache object for this connection, or nullptr if disabled
   * @warning This constructor should never be called directly.  Instead you should get accessors from the catalog.
   */
  CatalogAccessor(const common::ManagedPointer<Catalog> catalog, const common::ManagedPointer<DatabaseCatalog> dbc,
                  const common::ManagedPointer<transaction::TransactionContext> txn,
                  const common::ManagedPointer<CatalogCache> cache)
      : catalog_(catalog),
        dbc_(dbc),
        txn_(txn),
        search_path_({postgres::PgNamespace::NAMESPACE_CATALOG_NAMESPACE_OID,
                      postgres::PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID}),
        default_namespace_(postgres::PgNamespace::NAMESPACE_DEFAULT_NAMESPACE_OID),
        cache_(cache) {}

 private:
  const common::ManagedPointer<Catalog> catalog_;
  const common::ManagedPointer<DatabaseCatalog> dbc_;
  const common::ManagedPointer<transaction::TransactionContext> txn_;
  std::vector<namespace_oid_t> search_path_;
  namespace_oid_t default_namespace_;
  const common::ManagedPointer<CatalogCache> cache_ = nullptr;

  /**
   * A helper function to ensure that user-defined object names are standardized prior to doing catalog operations
   * @param name of object that should be sanitized/normalized
   */
  static void NormalizeObjectName(std::string *name) {
    std::transform(name->begin(), name->end(), name->begin(), [](auto &&c) { return std::tolower(c); });
  }
};

}  // namespace noisepage::catalog
