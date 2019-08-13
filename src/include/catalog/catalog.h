#pragma once

#include <memory>
#include <string>
#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/database_catalog.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"

namespace terrier::catalog {

class CatalogAccessor;

/**
 * The catalog stores all of the metadata about user tables and user defined
 * database objects so that other parts of the system (i.e. binder, optimizer,
 * and execution engine) can reason about and execute operations on these
 * objects.
 *
 * @warning Only DBMain and CatalogAccessor (and possibly the recovery system)
 * should be using the interface below.  All other code should use the
 * CatalogAccessor API which enforces scoping to a specific database and handles
 * namespace resolution for finding tables within that database.
 */
class Catalog {
 public:
  /**
   * Initializes the Catalog object which creates the primary table for databases
   * and bootstraps the default database ("terrier").  This also constructs the
   * debootstrap logic (i.e. table deallocations) that gets deferred using the
   * action framework in the destructor.
   * @param txn_manager for spawning read-only transactions in destructors
   * @param block_store to use to back catalog tables
   * @warning The catalog requires garbage collection and will leak catalog
   * tables if it is disabled.
   */
  Catalog(transaction::TransactionManager *txn_manager, storage::BlockStore *block_store);

  /**
   * Handles destruction of the catalog's members by calling the destructor on
   * all visible database catalog objects.
   * @warning The catalog assumes that any logically visible database objects
   * referenced by the catalog during destruction need to be deallocated by the
   * deferred action.  Therefore, there cannot be any live transactions when
   * the debootstrap event executes.
   * @note This function will begin and commit a read-only transaction
   * through the transaction manager and therefore must be called before the
   * transaction manager is destructed.  It will also defer events that will
   * begin and commit transactions.
   */
  void TearDown();

  /**
   * Creates a new database instance.
   * @param txn that creates the database
   * @param name of the new database
   * @param bootstrap indicates whether or not to perform bootstrap routine
   * @return OID of the database or INVALID_DATABASE_OID if the operation failed
   *   (which should only occur if there is already a database with that name)
   */
  db_oid_t CreateDatabase(transaction::TransactionContext *txn, const std::string &name, bool bootstrap);

  /**
   * Deletes the given database.  This operation will fail if there is any DDL
   * operation currently in-flight because it will cause a write-write conflict.
   * It could also fail if the OID does not correspond to an existing database.
   * @param txn that deletes the database
   * @param database OID to be deleted
   * @return true if the deletion succeeds, false otherwise
   */
  bool DeleteDatabase(transaction::TransactionContext *txn, db_oid_t database);

  /**
   * Renames the given database.
   * @param txn for the operation
   * @param database OID to be renamed
   * @param name which the database will now have
   * @return true if the operation succeeds, false otherwise
   */
  bool RenameDatabase(transaction::TransactionContext *txn, db_oid_t database, const std::string &name);

  /**
   * Resolve a database name to its OID.
   * @param txn for the catalog query
   * @param name of the database to resolve
   * @return OID of the database or INVALID_DATABASE_OID if it does not exist
   */
  db_oid_t GetDatabaseOid(transaction::TransactionContext *txn, const std::string &name);

  /**
   * Gets the database-specific catalog object.
   * @param txn for the catalog query
   * @param database OID of needed catalog
   * @return DatabaseCatalog object which has catalog information for the
   *   specific database
   */
  common::ManagedPointer<DatabaseCatalog> GetDatabaseCatalog(transaction::TransactionContext *txn, db_oid_t database);

  /**
   * Creates a new accessor into the catalog which will handle transactionality and sequencing of catalog operations.
   * @param txn for all subsequent catalog queries
   * @param database in which this transaction is scoped
   * @return a CatalogAccessor object for use with this transaction
   */
  std::unique_ptr<CatalogAccessor> GetAccessor(transaction::TransactionContext *txn, db_oid_t database);

 private:
  transaction::TransactionManager *txn_manager_;
  storage::BlockStore *catalog_block_store_;
  std::atomic<db_oid_t> next_oid_;

  storage::SqlTable *databases_;
  storage::index::Index *databases_name_index_;
  storage::index::Index *databases_oid_index_;

  /**
   * Creates a new database entry.
   * @param txn that creates the database
   * @param db OID of the database
   * @param name of the new database
   * @param dbc database catalog object for the new database
   * @return true if successful, otherwise false
   */
  bool CreateDatabaseEntry(transaction::TransactionContext *txn, db_oid_t db, const std::string &name,
                           DatabaseCatalog *dbc);

  /**
   * Deletes a database entry without scheduling the catalog object for destruction
   * @param txn that delete the database
   * @param db OID of the database
   * @return pointer to the database object if successful, otherwise nullptr
   */
  DatabaseCatalog *DeleteDatabaseEntry(transaction::TransactionContext *txn, db_oid_t db);

  /**
   * Creates a lambda that captures the necessary values by value and handles
   * the call to teardown and deleting the master object.
   * @param dbc pointing to the appropriate database catalog object
   * @return the action which can be deferred to the appropriate moment.
   */
  transaction::Action DeallocateDatabaseCatalog(DatabaseCatalog *dbc);
};
}  // namespace terrier::catalog
