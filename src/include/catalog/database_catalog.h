#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/postgres/pg_class.h"
#include "catalog/postgres/pg_language.h"
#include "catalog/postgres/pg_proc.h"
#include "catalog/postgres/pg_type.h"
#include "catalog/schema.h"
#include "common/managed_pointer.h"
#include "execution/ast/builtins.h"
#include "storage/projected_row.h"
#include "transaction/transaction_defs.h"

namespace terrier::execution::functions {
class FunctionContext;
}  // namespace terrier::execution::functions

namespace terrier::transaction {
class TransactionContext;
}

namespace terrier::storage {
class GarbageCollector;
class RecoveryManager;
class SqlTable;
namespace index {
class Index;
}
}  // namespace terrier::storage

namespace terrier::catalog {

class IndexSchema;

/**
 * The catalog stores all of the metadata about user tables and user defined
 * database objects so that other parts of the system (i.e. binder, optimizer,
 * and execution engine) can reason about and execute operations on these
 * objects.
 *
 * @warning Only Catalog and CatalogAccessor (and possibly the recovery system)
 * should be using the interface below.  All other code should use the
 * CatalogAccessor API which enforces scoping to a specific database and handles
 * namespace resolution for finding tables within that database.
 */
class DatabaseCatalog {
 public:
  /**
   * Adds the default/mandatory entries into the catalog that describe itself
   * @param txn for the operation
   */
  void Bootstrap(common::ManagedPointer<transaction::TransactionContext> txn);

  /**
   * Creates a new namespace within the database
   * @param txn for the operation
   * @param name of the new namespace
   * @return OID of the new namespace or INVALID_NAMESPACE_OID if the operation failed
   */
  namespace_oid_t CreateNamespace(common::ManagedPointer<transaction::TransactionContext> txn, const std::string &name);

  /**
   * Deletes the namespace and any objects assigned to the namespace.  The
   * 'public' namespace cannot be deleted.  This operation will fail if any
   * objects within the namespace cannot be deleted (i.e. write-write conflicts
   * exist).
   * @param txn for the operation
   * @param ns_oid OID to be deleted
   * @return true if the deletion succeeded, otherwise false
   */
  bool DeleteNamespace(common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t ns_oid);

  /**
   * Resolve a namespace name to its OID.
   * @param txn for the operation
   * @param name of the namespace
   * @return OID of the namespace or INVALID_NAMESPACE_OID if it does not exist
   */
  namespace_oid_t GetNamespaceOid(common::ManagedPointer<transaction::TransactionContext> txn, const std::string &name);

  /**
   * Create a new user table in the catalog.
   * @param txn for the operation
   * @param ns OID of the namespace the table belongs to
   * @param name of the new table
   * @param schema columns of the new table
   * @return OID of the new table or INVALID_TABLE_OID if the operation failed
   * @warning This function does not allocate the storage for the table.  The
   * transaction is responsible for setting the table pointer via a separate
   * function call prior to committing.
   * @see src/include/catalog/table_details.h
   */
  table_oid_t CreateTable(common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t ns,
                          const std::string &name, const Schema &schema);

  /**
   * Deletes a table and all child objects (i.e columns, indexes, etc.) from
   * the database.
   * @param txn for the operation
   * @param table to be deleted
   * @return true if the deletion succeeded, otherwise false
   */
  bool DeleteTable(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table);

  /**
   * Resolve a table name to its OID
   * @param txn for the operation
   * @param ns OID of the namespace the table belongs to
   * @param name of the table
   * @return OID of the table or INVALID_TABLE_OID if the table does not exist
   */
  table_oid_t GetTableOid(common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t ns,
                          const std::string &name);

  /**
   * Rename a table.
   * @param txn for the operation
   * @param table to be renamed
   * @param name which the table will now have
   * @return true if the operation succeeded, otherwise false
   */
  bool RenameTable(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table,
                   const std::string &name);

  /**
   * Inform the catalog of where the underlying storage for a table is
   * @param txn for the operation
   * @param table OID in the catalog
   * @param table_ptr to the memory where the storage is
   * @return whether the operation was successful
   * @warning The table pointer that is passed in must be on the heap as the
   * catalog will take ownership of it and schedule its deletion with the GC
   * at the appropriate time.
   * @warning It is unsafe to call delete on the SqlTable pointer after calling
   * this function regardless of the return status.
   */
  bool SetTablePointer(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table,
                       const storage::SqlTable *table_ptr);

  /**
   * Obtain the storage pointer for a SQL table
   * @param txn for the operation
   * @param table to which we want the storage object
   * @return the storage object corresponding to the passed OID
   */
  common::ManagedPointer<storage::SqlTable> GetTable(common::ManagedPointer<transaction::TransactionContext> txn,
                                                     table_oid_t table);

  /**
   * Apply a new schema to the given table.  The changes should modify the latest
   * schema as provided by the catalog.  There is no guarantee that the OIDs for
   * modified columns will be stable across a schema change.
   * @param txn for the operation
   * @param table OID of the modified table
   * @param new_schema object describing the table after modification
   * @return true if the operation succeeded, false otherwise
   * @warning The catalog accessor assumes it takes ownership of the schema object
   * that is passed.  As such, there is no guarantee that the pointer is still
   * valid when this function returns.  If the caller needs to reference the
   * schema object after this call, they should use the GetSchema function to
   * obtain the authoritative schema for this table.
   */
  bool UpdateSchema(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table, Schema *new_schema);

  /**
   * Get the visible schema describing the table.
   * @param txn for the operation
   * @param table corresponding to the requested schema
   * @return the visible schema object for the identified table
   */
  const Schema &GetSchema(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table);

  /**
   * A list of all constraints on this table
   * @param txn for the operation
   * @param table being queried
   * @return vector of OIDs for all of the constraints that apply to this table
   */
  std::vector<constraint_oid_t> GetConstraints(common::ManagedPointer<transaction::TransactionContext> txn,
                                               table_oid_t table);

  /**
   * A list of all indexes on the given table
   * @param txn for the operation
   * @param table being queried
   * @return vector of OIDs for all of the indexes on this table
   */
  std::vector<index_oid_t> GetIndexOids(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table);

  /**
   * Create the catalog entries for a new index.
   * @param txn for the operation
   * @param ns OID of the namespace under which the index will fall
   * @param name of the new index
   * @param table on which the new index exists
   * @param schema describing the new index
   * @return OID of the new index or INVALID_INDEX_OID if creation failed
   */
  index_oid_t CreateIndex(common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t ns,
                          const std::string &name, table_oid_t table, const IndexSchema &schema);

  /**
   * Delete an index.  Any constraints that utilize this index must be deleted
   * or transitioned to a different index prior to deleting an index.
   * @param txn for the operation
   * @param index to be deleted
   * @return true if the deletion succeeded, otherwise false.
   */
  bool DeleteIndex(common::ManagedPointer<transaction::TransactionContext> txn, index_oid_t index);

  /**
   * Resolve an index name to its OID
   * @param txn for the operation
   * @param ns OID for the namespace in which the index belongs
   * @param name of the index
   * @return OID of the index or INVALID_INDEX_OID if it does not exist
   */
  index_oid_t GetIndexOid(common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t ns,
                          const std::string &name);

  /**
   * Gets the schema used to define the index
   * @param txn for the operation
   * @param index being queried
   * @return the index schema
   */
  const IndexSchema &GetIndexSchema(common::ManagedPointer<transaction::TransactionContext> txn, index_oid_t index);

  /**
   * Inform the catalog of where the underlying implementation of the index is
   * @param txn for the operation
   * @param index OID in the catalog
   * @param index_ptr to the memory where the index is
   * @return whether the operation was successful
   * @warning The index pointer that is passed in must be on the heap as the
   * catalog will take ownership of it and schedule its deletion with the GC
   * at the appropriate time.
   * @warning It is unsafe to call delete on the Index pointer after calling
   * this function regardless of the return status.
   */
  bool SetIndexPointer(common::ManagedPointer<transaction::TransactionContext> txn, index_oid_t index,
                       storage::index::Index *index_ptr);

  /**
   * Obtain the pointer to the index
   * @param txn transaction to use
   * @param index to which we want a pointer
   * @return the pointer to the index
   */
  common::ManagedPointer<storage::index::Index> GetIndex(common::ManagedPointer<transaction::TransactionContext> txn,
                                                         index_oid_t index);

  /**
   * Returns index pointers and schemas for every index on a table. Provides much better performance than individual
   * calls to GetIndex and GetIndexSchema
   * @param txn transaction to use
   * @param table table to get index objects for
   * @return vector of pairs of index pointers and their corresponding schemas
   */
  std::vector<std::pair<common::ManagedPointer<storage::index::Index>, const IndexSchema &>> GetIndexes(
      common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table);

  /**
   * Creates a language entry into the pg_language table
   * @param txn transaction to use
   * @param lanname name of language to insert
   * @return oid of created entry if successful else INVALID_LANGUAGE_OID
   */
  language_oid_t CreateLanguage(common::ManagedPointer<transaction::TransactionContext> txn,
                                const std::string &lanname);

  /**
   * Looks up a language entry in the pg_language table
   * @param txn transaction to use
   * @param lanname name of language to look up
   * @return oid of requested entry if found else INVALID_LANGUAGE_OID if not found
   */
  language_oid_t GetLanguageOid(common::ManagedPointer<transaction::TransactionContext> txn,
                                const std::string &lanname);

  /**
   * Deletes a language entry from the pg_language table
   * @param txn transaction to use
   * @param oid oid of entry
   * @return true if deletion is successful
   */
  bool DropLanguage(common::ManagedPointer<transaction::TransactionContext> txn, language_oid_t oid);

  /**
   * Creates a procedure for the pg_proc table
   * @param txn transaction to use
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
   * @return oid of created proc entry or INVALID_PROC_ENTRY if the creation failed
   * @warning does not support variadics yet
   */
  proc_oid_t CreateProcedure(common::ManagedPointer<transaction::TransactionContext> txn, const std::string &procname,
                             language_oid_t language_oid, namespace_oid_t procns, const std::vector<std::string> &args,
                             const std::vector<type_oid_t> &arg_types, const std::vector<type_oid_t> &all_arg_types,
                             const std::vector<postgres::ProArgModes> &arg_modes, type_oid_t rettype,
                             const std::string &src, bool is_aggregate);

  /**
   * Creates a procedure for the pg_proc table
   * @param txn transaction to use
   * @param oid oid of procedure to create
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
   * @return if the creation was a success
   * @warning does not support variadics yet
   */
  bool CreateProcedure(common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t oid,
                       const std::string &procname, language_oid_t language_oid, namespace_oid_t procns,
                       const std::vector<std::string> &args, const std::vector<type_oid_t> &arg_types,
                       const std::vector<type_oid_t> &all_arg_types,
                       const std::vector<postgres::ProArgModes> &arg_modes, type_oid_t rettype, const std::string &src,
                       bool is_aggregate);

  /**
   * Drops a procedure from the pg_proc table
   * @param txn transaction to use
   * @param proc oid of process to drop
   * @return true iff the process was successfully found and dropped
   */
  bool DropProcedure(common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc);

  /**
   * Gets the oid of a procedure from pg_proc given a requested name and namespace
   * @param txn transaction to use
   * @param procns namespace of the process to lookup
   * @param procname name of the proc to lookup
   * @param all_arg_types types of all arguments in this function
   * @return the oid of the found proc if found else INVALID_PROC_OID
   */
  proc_oid_t GetProcOid(common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t procns,
                        const std::string &procname, const std::vector<type_oid_t> &all_arg_types);

  /**
   * Sets the proc context pointer column of proc_oid to func_context
   * @param txn transaction to use
   * @param proc_oid The proc_oid whose pointer column we are setting here
   * @param func_context The context object to set to
   * @return False if the given proc_oid is invalid, True if else
   */
  bool SetProcCtxPtr(common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc_oid,
                     const execution::functions::FunctionContext *func_context);

  /**
   * Gets a functions context object for a given proc if it is null for a valid proc id then the functions context
   * object is reconstructed, put in pg_proc and returned
   * @param txn the transaction to use
   * @param proc_oid the proc_oid we are querying here for the context object
   * @return nullptr if proc_oid is invalid else a valid functions context object for this proc_oid
   */
  common::ManagedPointer<execution::functions::FunctionContext> GetFunctionContext(
      common::ManagedPointer<transaction::TransactionContext> txn, catalog::proc_oid_t proc_oid);

  /**
   * Gets the proc context pointer column of proc_oid
   * @param txn transaction to use
   * @param proc_oid The proc_oid whose pointer column we are getting here
   * @return nullptr if proc_oid is either invalid or there is no context object set for this proc_oid
   */
  common::ManagedPointer<execution::functions::FunctionContext> GetProcCtxPtr(
      common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc_oid);

  /**
   * Returns oid for built in type. Currently, we simply use the underlying int for the enum as the oid
   * @param type internal type
   * @return oid for internal type
   */
  type_oid_t GetTypeOidForType(type::TypeId type);

 private:
  // TODO(tanujnay112) Add support for other parameters

  /**
   * Creates a language entry into the pg_language table
   * @param txn transaction to use
   * @param lanname name of language to insert
   * @param oid oid of entry
   * @return true if insertion is successful
   */
  bool CreateLanguage(common::ManagedPointer<transaction::TransactionContext> txn, const std::string &lanname,
                      language_oid_t oid);

  /**
   * Create a namespace with a given ns oid
   * @param txn transaction to use
   * @param name name of the namespace
   * @param ns_oid oid of the namespace
   * @return true if creation is successful
   */
  bool CreateNamespace(common::ManagedPointer<transaction::TransactionContext> txn, const std::string &name,
                       namespace_oid_t ns_oid);

  /**
   * Add entry to pg_attribute
   * @tparam Column type of column (IndexSchema::Column or Schema::Column)
   * @param txn txn to use
   * @param class_oid oid of table or index
   * @param col column to insert
   * @param default_val default value
   * @return whether insertion is successful
   */
  template <typename Column, typename ClassOid, typename ColOid>
  bool CreateColumn(common::ManagedPointer<transaction::TransactionContext> txn, ClassOid class_oid, ColOid col_oid,
                    const Column &col);
  /**
   * Get entries from pg_attribute
   * @tparam Column type of columns
   * @param txn txn to use
   * @param class_oid oid of table or index
   * @return the column from pg_attribute
   */
  template <typename Column, typename ClassOid, typename ColOid>
  std::vector<Column> GetColumns(common::ManagedPointer<transaction::TransactionContext> txn, ClassOid class_oid);

  /**
   * A list of all oids and their postgres::ClassKind from pg_class on the given namespace. This is currently designed
   * as an internal function, though could be exposed via the CatalogAccessor if desired in the future.
   * @param txn for the operation
   * @param ns being queried
   * @return vector of OIDs for all of the objects on this namespace
   */
  std::vector<std::pair<uint32_t, postgres::ClassKind>> GetNamespaceClassOids(
      common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t ns_oid);

  /**
   * Delete entries from pg_attribute
   * @tparam Column type of columns
   * @param txn txn to use
   * @return the column from pg_attribute
   */
  template <typename Column, typename ClassOid>
  bool DeleteColumns(common::ManagedPointer<transaction::TransactionContext> txn, ClassOid class_oid);

  storage::SqlTable *namespaces_;
  storage::index::Index *namespaces_oid_index_;
  storage::index::Index *namespaces_name_index_;
  storage::ProjectedRowInitializer pg_namespace_all_cols_pri_;
  storage::ProjectionMap pg_namespace_all_cols_prm_;
  storage::ProjectedRowInitializer delete_namespace_pri_;
  storage::ProjectedRowInitializer get_namespace_pri_;

  storage::SqlTable *classes_;
  storage::index::Index *classes_oid_index_;
  storage::index::Index *classes_name_index_;  // indexed on namespace OID and name
  storage::index::Index *classes_namespace_index_;
  storage::ProjectedRowInitializer pg_class_all_cols_pri_;
  storage::ProjectionMap pg_class_all_cols_prm_;
  storage::ProjectedRowInitializer get_class_oid_kind_pri_;
  storage::ProjectedRowInitializer set_class_pointer_pri_;
  storage::ProjectedRowInitializer set_class_schema_pri_;
  storage::ProjectedRowInitializer get_class_pointer_kind_pri_;
  storage::ProjectedRowInitializer get_class_schema_pointer_kind_pri_;
  storage::ProjectedRowInitializer get_class_object_and_schema_pri_;
  storage::ProjectionMap get_class_object_and_schema_prm_;

  storage::SqlTable *indexes_;
  storage::index::Index *indexes_oid_index_;
  storage::index::Index *indexes_table_index_;
  storage::ProjectedRowInitializer get_indexes_pri_;
  storage::ProjectedRowInitializer delete_index_pri_;
  storage::ProjectionMap delete_index_prm_;
  storage::ProjectedRowInitializer pg_index_all_cols_pri_;
  storage::ProjectionMap pg_index_all_cols_prm_;

  storage::SqlTable *columns_;
  storage::index::Index *columns_oid_index_;   // indexed on class OID and column OID
  storage::index::Index *columns_name_index_;  // indexed on class OID and column name
  storage::ProjectedRowInitializer pg_attribute_all_cols_pri_;
  storage::ProjectionMap pg_attribute_all_cols_prm_;
  storage::ProjectedRowInitializer get_columns_pri_;
  storage::ProjectionMap get_columns_prm_;
  storage::ProjectedRowInitializer delete_columns_pri_;
  storage::ProjectionMap delete_columns_prm_;

  storage::SqlTable *types_;
  storage::index::Index *types_oid_index_;
  storage::index::Index *types_name_index_;  // indexed on namespace OID and name
  storage::index::Index *types_namespace_index_;
  storage::ProjectedRowInitializer pg_type_all_cols_pri_;
  storage::ProjectionMap pg_type_all_cols_prm_;

  storage::SqlTable *constraints_;
  storage::index::Index *constraints_oid_index_;
  storage::index::Index *constraints_name_index_;  // indexed on namespace OID and name
  storage::index::Index *constraints_namespace_index_;
  storage::index::Index *constraints_table_index_;
  storage::index::Index *constraints_index_index_;
  storage::index::Index *constraints_foreigntable_index_;

  storage::SqlTable *languages_;
  storage::index::Index *languages_oid_index_;
  storage::index::Index *languages_name_index_;  // indexed on language name and namespace
  storage::ProjectedRowInitializer pg_language_all_cols_pri_;
  storage::ProjectionMap pg_language_all_cols_prm_;

  storage::SqlTable *procs_;
  storage::index::Index *procs_oid_index_;
  storage::index::Index *procs_name_index_;
  storage::ProjectedRowInitializer pg_proc_all_cols_pri_;
  storage::ProjectionMap pg_proc_all_cols_prm_;
  storage::ProjectedRowInitializer pg_proc_ptr_pri_;

  std::atomic<uint32_t> next_oid_;
  std::atomic<transaction::timestamp_t> write_lock_;

  const db_oid_t db_oid_;
  const common::ManagedPointer<storage::GarbageCollector> garbage_collector_;

  DatabaseCatalog(const db_oid_t oid, const common::ManagedPointer<storage::GarbageCollector> garbage_collector)
      : write_lock_(transaction::INITIAL_TXN_TIMESTAMP), db_oid_(oid), garbage_collector_(garbage_collector) {}

  void TearDown(common::ManagedPointer<transaction::TransactionContext> txn);
  bool CreateTableEntry(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table_oid,
                        namespace_oid_t ns_oid, const std::string &name, const Schema &schema);

  friend class Catalog;
  friend class postgres::Builder;
  friend class storage::RecoveryManager;

  /**
   * Internal function to DatabaseCatalog to disallow concurrent DDL changes. This also disallows older txns to enact
   * DDL changes after a newer transaction has committed one. This effectively follows the same timestamp ordering logic
   * as the version pointer MVCC stuff in the storage layer. It also serializes all DDL within a database.
   * @param txn Requesting txn. This is used to inspect the timestamp and register commit/abort events to release the
   * lock if it is acquired.
   * @return true if lock was acquired, false otherwise
   * @warning this requires that commit actions be performed after the commit time is stored in the
   * TransactionContext's FinishTime.
   */
  bool TryLock(common::ManagedPointer<transaction::TransactionContext> txn);

  /**
   * Atomically updates the next oid counter to the max of the current count and the provided next oid
   * @param oid next oid to move oid counter to
   */
  void UpdateNextOid(uint32_t oid) {
    uint32_t expected, desired;
    do {
      expected = next_oid_.load();
      desired = std::max(expected, oid);
    } while (!next_oid_.compare_exchange_weak(expected, desired));
  }

  /**
   * Helper method to create index entries into pg_class and pg_indexes.
   * @param txn txn for the operation
   * @param ns_oid  OID of the namespace under which the index will fall
   * @param table_oid table OID on which the new index exists
   * @param index_oid OID for the index to create
   * @param name name of the new index
   * @param schema describing the new index
   * @return true if creation succeeded, false otherwise
   */
  bool CreateIndexEntry(common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t ns_oid,
                        table_oid_t table_oid, index_oid_t index_oid, const std::string &name,
                        const IndexSchema &schema);

  /**
   * Delete all of the indexes for a given table. This is currently designed as an internal function, though could be
   * exposed via the CatalogAccessor if desired in the future.
   * @param txn for the operation
   * @param table to remove all indexes for
   * @return true if the deletion succeeded, otherwise false.
   */
  bool DeleteIndexes(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table);

  /**
   * Bootstraps the built-in types found in type::Type
   * @param txn transaction to insert into catalog with
   */
  void BootstrapTypes(common::ManagedPointer<transaction::TransactionContext> txn);

  /**
   * Bootstraps the built-in languages found in pg_languages
   * @param txn transaction to insert into catalog with
   */
  void BootstrapLanguages(common::ManagedPointer<transaction::TransactionContext> txn);

  /**
   * Bootstraps the built-in procs found in pg_proc
   * @param txn transaction to insert into catalog with
   */
  void BootstrapProcs(common::ManagedPointer<transaction::TransactionContext> txn);

  /**
   * Bootstraps the proc functions contexts in pg_proc
   * @param txn transaction to insert into catalog with
   */
  void BootstrapProcContexts(common::ManagedPointer<transaction::TransactionContext> txn);

  /**
   * Internal helper method to reduce copy-paste code for populating proc contexts. Allocates the FunctionContext and
   * inserts the pointer.
   * @param txn transaction to insert into catalog with
   * @param proc_oid oid to associate with this proc's and its context
   * @param func_name Name of function
   * @param func_ret_type Return type of function
   * @param args_type Vector of argument types
   * @param builtin Which builtin this context refers to
   * @param is_exec_ctx_required true if this function requires an execution context var as its first argument
   */
  void BootstrapProcContext(common::ManagedPointer<transaction::TransactionContext> txn, proc_oid_t proc_oid,
                            std::string &&func_name, type::TypeId func_ret_type, std::vector<type::TypeId> &&args_type,
                            execution::ast::Builtin builtin, bool is_exec_ctx_required);

  /**
   * Creates all of the ProjectedRowInitializers and ProjectionMaps for the catalog. These can be stashed because the
   * catalog shouldn't undergo schema changes at runtime
   */
  void BootstrapPRIs();

  /**
   * Helper function to insert a type into PG_Type and the type indexes
   * @param txn transaction to insert with
   * @param type_oid oid of type to insert with
   * @param name type name
   * @param namespace_oid namespace to insert type into
   * @param len length of type in bytes. len should be -1 for varlen types
   * @param by_val true if type should be passed by value. false if passed by reference
   * @param type_category category of type
   */
  void InsertType(common::ManagedPointer<transaction::TransactionContext> txn, type_oid_t type_oid,
                  const std::string &name, namespace_oid_t namespace_oid, int16_t len, bool by_val,
                  postgres::Type type_category);

  /**
   * Helper function to insert a type into PG_Type and the type indexes
   * @param txn transaction to insert with
   * @param internal_type internal type to insert
   * @param name type name
   * @param namespace_oid namespace to insert type into
   * @param len length of type in bytes. len should be -1 for varlen types
   * @param by_val true if type should be passed by value. false if passed by reference
   * @param type_category category of type
   */
  void InsertType(common::ManagedPointer<transaction::TransactionContext> txn, type::TypeId internal_type,
                  const std::string &name, namespace_oid_t namespace_oid, int16_t len, bool by_val,
                  postgres::Type type_category);

  /**
   * Helper function to query the oid and kind from
   * [pg_class](https://www.postgresql.org/docs/9.3/catalog-pg-class.html)
   * @param txn transaction to query
   * @param namespace_oid the namespace oid
   * @param name name of the table, index, view, etc.
   * @return a pair of oid and ClassKind
   */
  std::pair<uint32_t, postgres::ClassKind> GetClassOidKind(common::ManagedPointer<transaction::TransactionContext> txn,
                                                           namespace_oid_t ns_oid, const std::string &name);

  /**
   * Helper function to query an object pointer form pg_class
   * @param txn transaction to query
   * @param oid oid to object
   * @return pair of ptr to and ClassKind of object requested. ptr will be nullptr if no entry was found for the given
   * oid
   */
  std::pair<void *, postgres::ClassKind> GetClassPtrKind(common::ManagedPointer<transaction::TransactionContext> txn,
                                                         uint32_t oid);

  /**
   * Helper function to query a schema pointer form pg_class
   * @param txn transaction to query
   * @param oid oid to object
   * @return pair of ptr to schema and ClassKind of object requested. ptr will be nullptr if no entry was found for the
   * given oid
   */
  std::pair<void *, postgres::ClassKind> GetClassSchemaPtrKind(
      common::ManagedPointer<transaction::TransactionContext> txn, uint32_t oid);

  /**
   * Sets a table's schema in pg_class
   * @warning Should only be used by recovery
   * @param txn transaction to query
   * @param oid oid to object
   * @param schema object schema to insert
   * @return true if succesfull
   */
  bool SetTableSchemaPointer(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t oid,
                             const Schema *schema);

  /**
   * Sets an index's schema in pg_class
   * @warning Should only be used by recovery
   * @param txn transaction to query
   * @param oid oid to object
   * @param schema object schema to insert
   * @return true if succesfull
   */
  bool SetIndexSchemaPointer(common::ManagedPointer<transaction::TransactionContext> txn, index_oid_t oid,
                             const IndexSchema *schema);

  /**
   * Inserts a provided pointer into a given pg_class column. Can be used for class object and schema pointers
   * Helper method since SetIndexPointer/SetTablePointer and SetIndexSchemaPointer/SetTableSchemaPointer
   * are basically indentical outside of input types
   * @tparam ClassOid either index_oid_t or table_oid_t
   * @tparam Ptr either Index or SqlTable
   * @param txn transaction to query
   * @param oid oid to object
   * @param pointer pointer to set
   * @param class_col pg_class column to insert pointer into
   * @return true if successful
   */
  template <typename ClassOid, typename Ptr>
  bool SetClassPointer(common::ManagedPointer<transaction::TransactionContext> txn, ClassOid oid, const Ptr *pointer,
                       col_oid_t class_col);

  /**
   * @tparam Column column type (either index or table)
   * @param pr ProjectedRow to populate
   * @param table_pm ProjectionMap for the ProjectedRow
   * @return heap-allocated column managed by unique_ptr
   */
  template <typename Column, typename ColOid>
  static Column MakeColumn(storage::ProjectedRow *pr, const storage::ProjectionMap &pr_map);
};
}  // namespace terrier::catalog
