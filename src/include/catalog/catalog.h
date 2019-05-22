#pragma once

#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/database_catalog.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"

namespace terrier::catalog {

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
   * @param txn with which to initialize the tables (necessary for GC)
   * @param block_store to use to back catalog tables
   * @warning The catalog requires garbage collection and will leak catalog
   * tables if it is disabled.
   */
  Catalog(transaction::TransactionContext *txn, storage::BlockStore block_store);

  /**
   * Handles destruction of the catalog by deferring an event using the event
   * framework that handles deallocating all of the objects handled or owned by
   * the catalog.
   * @warning The catalog assumes that any logically visible database objects
   * referenced by the catalog during destruction need to be deallocated by the
   * deferred action.  Therefore, there cannot be any live transactions when
   * the debootstrap event executes.
   */
  ~Catalog();

  /**
   * Creates a new database instance.
   * @param txn that creates the database
   * @param name of the new database
   * @return OID of the database or INVALID_DATABASE_OID if the operation failed
   *   (which should only occur if there is already a database with that name)
   */

  db_oid_t CreateDatabase(transaction::TransactionContext *txn, const std::string &name);
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
  const DatabaseCatalog &GetDatabaseCatalog(transaction::TransactionContext *txn, db_oid_t database);

  /**
   * Gets the database-specific catalog object.
   * @param txn for the catalog query
   * @param name of the database whose catalog will be returned
   * @return DatabaseCatalog object which has catalog information for the
   *   specific database
   */
  const DatabaseCatalog &GetDatabaseCatalog(transaction::TransactionContext *txn, const std::string &name);

 private:
  storage::SqlTable *databases_;
  storage::index::Index *databases_name_index_;
  storage::index::Index *databases_oid_index_;
  transaction::Action debootstrap;
};
} // namespace terrier::catalog
