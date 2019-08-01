#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/index_schema.h"
#include "catalog/postgres/pg_class.h"
#include "catalog/postgres/pg_type.h"
#include "catalog/schema.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"

namespace terrier::catalog {

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
  void Bootstrap(transaction::TransactionContext *txn);

  /**
   * Creates a new namespace within the database
   * @param txn for the operation
   * @param name of the new namespace
   * @return OID of the new namespace or INVALID_NAMESPACE_OID if the operation failed
   */
  namespace_oid_t CreateNamespace(transaction::TransactionContext *txn, const std::string &name);

  /**
   * Deletes the namespace and any objects assigned to the namespace.  The
   * 'public' namespace cannot be deleted.  This operation will fail if any
   * objects within the namespace cannot be deleted (i.e. write-write conflicts
   * exist).
   * @param txn for the operation
   * @param ns_oid OID to be deleted
   * @return true if the deletion succeeded, otherwise false
   */
  bool DeleteNamespace(transaction::TransactionContext *txn, namespace_oid_t ns_oid);

  /**
   * Resolve a namespace name to its OID.
   * @param txn for the operation
   * @param name of the namespace
   * @return OID of the namespace or INVALID_NAMESPACE_OID if it does not exist
   */
  namespace_oid_t GetNamespaceOid(transaction::TransactionContext *txn, const std::string &name);

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
  table_oid_t CreateTable(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string &name,
                          const Schema &schema);

  /**
   * Deletes a table and all child objects (i.e columns, indexes, etc.) from
   * the database.
   * @param txn for the operation
   * @param table to be deleted
   * @return true if the deletion succeeded, otherwise false
   */
  bool DeleteTable(transaction::TransactionContext *txn, table_oid_t table);

  /**
   * Resolve a table name to its OID
   * @param txn for the operation
   * @param ns OID of the namespace the table belongs to
   * @param name of the table
   * @return OID of the table or INVALID_TABLE_OID if the table does not exist
   */
  table_oid_t GetTableOid(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string &name);

  /**
   * Rename a table.
   * @param txn for the operation
   * @param table to be renamed
   * @param name which the table will now have
   * @return true if the operation succeeded, otherwise false
   */
  bool RenameTable(transaction::TransactionContext *txn, table_oid_t table, const std::string &name);

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
  bool SetTablePointer(transaction::TransactionContext *txn, table_oid_t table, const storage::SqlTable *table_ptr);

  /**
   * Obtain the storage pointer for a SQL table
   * @param txn for the operation
   * @param table to which we want the storage object
   * @return the storage object corresponding to the passed OID
   */
  common::ManagedPointer<storage::SqlTable> GetTable(transaction::TransactionContext *txn, table_oid_t table);

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
  bool UpdateSchema(transaction::TransactionContext *txn, table_oid_t table, Schema *new_schema);

  /**
   * Get the visible schema describing the table.
   * @param txn for the operation
   * @param table corresponding to the requested schema
   * @return the visible schema object for the identified table
   */
  const Schema &GetSchema(transaction::TransactionContext *txn, table_oid_t table);

  /**
   * A list of all constraints on this table
   * @param txn for the operation
   * @param table being queried
   * @return vector of OIDs for all of the constraints that apply to this table
   */
  std::vector<constraint_oid_t> GetConstraints(transaction::TransactionContext *txn, table_oid_t table);

  /**
   * A list of all indexes on the given table
   * @param txn for the operation
   * @param table being queried
   * @return vector of OIDs for all of the indexes on this table
   */
  std::vector<index_oid_t> GetIndexes(transaction::TransactionContext *txn, table_oid_t table);

  /**
   * Create the catalog entries for a new index.
   * @param txn for the operation
   * @param ns OID of the namespace under which the index will fall
   * @param name of the new index
   * @param table on which the new index exists
   * @param schema describing the new index
   * @return OID of the new index or INVALID_INDEX_OID if creation failed
   */
  index_oid_t CreateIndex(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string &name,
                          table_oid_t table, const IndexSchema &schema);

  /**
   * Delete an index.  Any constraints that utilize this index must be deleted
   * or transitioned to a different index prior to deleting an index.
   * @param txn for the operation
   * @param index to be deleted
   * @return true if the deletion succeeded, otherwise false.
   */
  bool DeleteIndex(transaction::TransactionContext *txn, index_oid_t index);

  /**
   * Resolve an index name to its OID
   * @param txn for the operation
   * @param ns OID for the namespace in which the index belongs
   * @param name of the index
   * @return OID of the index or INVALID_INDEX_OID if it does not exist
   */
  index_oid_t GetIndexOid(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string &name);

  /**
   * Gets the schema used to define the index
   * @param txn for the operation
   * @param index being queried
   * @return the index schema
   */
  const IndexSchema &GetIndexSchema(transaction::TransactionContext *txn, index_oid_t index);

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
  bool SetIndexPointer(transaction::TransactionContext *txn, index_oid_t index, const storage::index::Index *index_ptr);

  /**
   * Obtain the pointer to the index
   * @param txn transaction to use
   * @param index to which we want a pointer
   * @return the pointer to the index
   */
  common::ManagedPointer<storage::index::Index> GetIndex(transaction::TransactionContext *txn, index_oid_t index);

 private:
  /**
   * Create a namespace with a given ns oid
   * @param txn transaction to use
   * @param name name of the namespace
   * @param ns_oid oid of the namespace
   * @return true if creation is successful
   */
  bool CreateNamespace(transaction::TransactionContext *txn, const std::string &name, namespace_oid_t ns_oid);

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
  bool CreateColumn(transaction::TransactionContext *txn, ClassOid class_oid, ColOid col_oid, const Column &col);
  /**
   * Get entries from pg_attribute
   * @tparam Column type of columns
   * @param txn txn to use
   * @param class_oid oid of table or index
   * @return the column from pg_attribute
   */
  template <typename Column, typename ClassOid, typename ColOid>
  std::vector<Column> GetColumns(transaction::TransactionContext *txn, ClassOid class_oid);

  /**
   * Delete entries from pg_attribute
   * @tparam Column type of columns
   * @param txn txn to use
   * @return the column from pg_attribute
   */
  template <typename Column, typename ClassOid>
  bool DeleteColumns(transaction::TransactionContext *txn, ClassOid class_oid);

  storage::SqlTable *namespaces_;
  storage::index::Index *namespaces_oid_index_;
  storage::index::Index *namespaces_name_index_;

  storage::SqlTable *classes_;
  storage::index::Index *classes_oid_index_;
  storage::index::Index *classes_name_index_;  // indexed on namespace OID and name
  storage::index::Index *classes_namespace_index_;

  storage::SqlTable *indexes_;
  storage::index::Index *indexes_oid_index_;
  storage::index::Index *indexes_table_index_;

  storage::SqlTable *columns_;
  storage::index::Index *columns_oid_index_;   // indexed on class OID and column OID
  storage::index::Index *columns_name_index_;  // indexed on class OID and column name
  storage::index::Index *columns_class_index_;

  storage::SqlTable *types_;
  storage::index::Index *types_oid_index_;
  storage::index::Index *types_name_index_;  // indexed on namespace OID and name
  storage::index::Index *types_namespace_index_;

  storage::SqlTable *constraints_;
  storage::index::Index *constraints_oid_index_;
  storage::index::Index *constraints_name_index_;  // indexed on namespace OID and name
  storage::index::Index *constraints_namespace_index_;
  storage::index::Index *constraints_table_index_;
  storage::index::Index *constraints_index_index_;
  storage::index::Index *constraints_foreigntable_index_;

  transaction::Action debootstrap;
  std::atomic<uint32_t> next_oid_;

  const db_oid_t db_oid_;

  explicit DatabaseCatalog(db_oid_t oid) : db_oid_(oid) {}

  void TearDown(transaction::TransactionContext *txn);
  bool CreateTableEntry(transaction::TransactionContext *txn, table_oid_t table_oid, namespace_oid_t ns_oid,
                        const std::string &name, const Schema &schema);

  friend class Catalog;
  friend class postgres::Builder;

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
  bool CreateIndexEntry(transaction::TransactionContext *txn, namespace_oid_t ns_oid, table_oid_t table_oid,
                        index_oid_t index_oid, const std::string &name, const IndexSchema &schema);

  /**
   * Bootstraps the built-in types found in type::Type
   * @param txn transaction to insert into catalog with
   */
  void BootstrapTypes(transaction::TransactionContext *txn);

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
  void InsertType(transaction::TransactionContext *txn, type::TypeId internal_type, const std::string &name,
                  namespace_oid_t namespace_oid, int16_t len, bool by_val, postgres::Type type_category);

  /**
   * Returns oid for built in type. Currently, we simply use the underlying int for the enum as the oid
   * @param type internal type
   * @return oid for internal type
   */
  type_oid_t GetTypeOidForType(type::TypeId type);

  /**
   * Helper function to query the oid and kind from
   * [pg_class](https://www.postgresql.org/docs/9.3/catalog-pg-class.html)
   * @param txn transaction to query
   * @param namespace_oid the namespace oid
   * @param name name of the table, index, view, etc.
   * @return a pair of oid and ClassKind
   */
  std::pair<uint32_t, postgres::ClassKind> GetClassOidKind(transaction::TransactionContext *txn, namespace_oid_t ns_oid,
                                                           const std::string &name);

  /**
   * Helper function to query an object pointer form pg_class
   * @param txn transaction to query
   * @param oid oid to object
   * @return pair of ptr to and ClassKind of object requested. ptr will be nullptr if no entry was found for the given
   * oid
   */
  std::pair<void *, postgres::ClassKind> GetClassPtrKind(transaction::TransactionContext *txn, uint32_t oid);

  /**
   * Helper function to query a schema pointer form pg_class
   * @param txn transaction to query
   * @param oid oid to object
   * @return pair of ptr to schema and ClassKind of object requested. ptr will be nullptr if no entry was found for the
   * given oid
   */
  std::pair<void *, postgres::ClassKind> GetClassSchemaPtrKind(transaction::TransactionContext *txn, uint32_t oid);

  /**
   * Helper method since SetIndexPointer and SetTablePointer are basically indentical outside of input types
   * @tparam ClassOid either index_oid_t or table_oid_t
   * @tparam Class either Index or SqlTable
   * @param txn transaction to query
   * @param oid oid to object
   * @param pointer pointer to set
   * @return true if successful
   */
  template <typename ClassOid, typename Class>
  bool SetClassPointer(transaction::TransactionContext *txn, ClassOid oid, const Class *pointer);

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
