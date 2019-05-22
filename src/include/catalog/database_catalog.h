#pragma once

#include <vector>

#include "catalog/catalog_defs.h"
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
   * Initializes the DatabaseCatalog object by creating bootstrapping the catalog
   * tables and inserting the default namespace ("public") and types.  This also
   * constructs the debootstrap logic (i.e. table deallocations) that gets
   * deferred using the action framework in the destructor.
   * @param txn with which to initialize the tables (necessary for GC)
   * @param block_store to use to back catalog tables
   */
  DatabaseCatalog(transaction::TransactionContext *txn, storeage::BlockStore block_store);

  /**
   * Handles destruction of the database catalog by deferring an event using
   * the event framework that handles deallocating all of the objects handled
   * or owned by the database catalog.
   * @warning This destructor assumes that any logically visible user objects
   * referenced by the catalog during destruction need to be deallocated by the
   * deferred action.  Therefore, there cannot be any live transactions when
   * the debootstrap event executes.
   *
   * @warning This is not transactional.  If the database is being logically
   * deleted (and not just deallocated on shutdown), the user must call
   * Catalog::DeleteDatabase to ensure the deallocation is done in a
   * transactionally safe manner.
   */
  ~DatabaseCatalog();

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
   * @param ns OID to be deleted
   * @param true if the deletion succeeded, otherwise false
   */
  bool DeleteNamespace(transaction::TransactionContext *txn, namespace_oid_t ns);

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
   * @param columns that comprise the new table
   * @return OID of the new table or INVALID_TABLE_OID if the operation failed
   * @warning This function does not allocate the storage for the table.  The
   * transaction is responsible for setting the table pointer via a separate
   * function call prior to committing.
   * @see src/include/catalog/table_details.h
   */
  table_oid_t CreateTable(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string &name,
                          std::vector<Schema::Column> columns);

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
   * Get an object with detailed access to a table
   * @param txn for the operation
   * @param table to get details for
   * @return object wrapping the details
   */
  TableDetails GetTableDetails(transaction::TransactionContext *txn, table_oid_t table);

  /**
   * Rename a table.
   * @param txn for the operation
   * @param table to be renamed
   * @param name which the table will now have
   * @return true if the operation succeeded, otherwise false
   */
  bool RenameTable(transaction::TransactionContext *txn, table_oid_t table, const std::string &name);

  /**
   * Create the catalog entries for a new index.
   * @param txn for the operation
   * @param ns OID of the namespace under which the index will fall
   * @param name of the new index
   * @param table on which the new index exists
   * @param key_schema describing the new index
   * @return OID of the new index or INVALID_INDEX_OID if creation failed
   */
  index_oid_t CreateIndex(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string &name,
                          table_oid_t table, IndexKeySchema key_schema);

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
   * Get an object with detailed access to an index
   * @param txn for the operation
   * @param index to get details for
   * @return the object with the index details
   */
  IndexDetails GetIndexDetails(transaction::TransactionContext *txn, index_oid_t index);
 private:
  storage::SqlTable *namespaces_;
  storage::index::Index *namespaces_oid_index_;
  storage::index::Index *namespaces_name_index_;

  storage::SqlTable *classes_;
  storage::index::Index *classes_oid_index_;
  storage::index::Index *classes_name_index_; // indexed on namespace OID and name

  storage::SqlTable *indexes_;
  storage::index::Index *indexes_oid_index_;
  storage::index::Index *indexes_table_index_;

  storage::SqlTable *columns_;
  storage::index::Index *columns_oid_index_; // indexed on class OID and column OID
  storage::index::Index *columns_name_index_; // indexed on class OID and column name

  storage::SqlTable *types_;
  storage::index::Index *types_oid_index_;
  storage::index::Index *types_name_index_; // indexed on namespace OID and name

  storage::SqlTable *constraints_;
  storage::index::Index *constraints_oid_index_;
  storage::index::Index *constraints_name_index_; // indexed on namespace OID and name
  storage::index::Index *constraints_table_index_;
  storage::index::Index *constraints_index_index_;
  storage::index::Index *constraints_foreignkey_index_;

  std::atomic<uint32_t> next_oid_;
  transaction::Action debootstrap;
};
} // namespace terrier::catalog
