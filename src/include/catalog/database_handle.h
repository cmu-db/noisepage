#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/namespace_handle.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

class Catalog;
class NamespaceHandle;

/**
 * A DatabaseHandle provides access to the (global) system pg_database
 * catalog.
 *
 * This pg_database is a subset of Postgres (v11)  pg_database, and
 * contains the following fields:
 *
 * Name    SQL Type     Description
 * ----    --------     -----------
 * oid     integer
 * datname varchar      Database name
 *
 * DatabaseEntry instances provide accessors for individual rows of
 * pg_database.
 */

class DatabaseHandle {
 public:
  /**
   * A database entry represents a row in pg_database catalog.
   */
  class DatabaseEntry {
   public:
    /**
     * Constructs a database entry.
     * @param oid: the db_oid of the underlying database
     * @param entry: the row as a vector of values
     */
    DatabaseEntry(db_oid_t oid, std::vector<type::Value> entry) : oid_(oid), entry_(std::move(entry)) {}

    /**
     * Construct a database entry from a projected column
     */
    DatabaseEntry(std::shared_ptr<DatabaseHandle> handle_p, storage::ProjectedColumns *proj_col_p);

    ~DatabaseEntry() { delete[] reinterpret_cast<byte *>(proj_col_p_); }

    /**
     * Get the value for a given column
     * @param col_num the column index
     * @return the value of the column
     */
    const type::Value &GetColumn(int32_t col_num) { return entry_[col_num]; }

    /**
     * Return the db_oid of the underlying database
     * @return db_oid of the database
     */
    db_oid_t GetDatabaseOid() { return oid_; }

    /**
     * Delete the data (for this entry) from the storage table.
     * After this, the entry object must be deleted as no other
     * operations are possible.
     */
    bool Delete(transaction::TransactionContext *txn);

   private:
    db_oid_t oid_;
    storage::ProjectedColumns *proj_col_p_ = nullptr;
    // we don't really need to store this. GetColumn changes though...
    std::vector<type::Value> entry_;
    std::shared_ptr<DatabaseHandle> handle_p_;
  };

  /**
   * Construct a database handle. It keeps a pointer to pg_database sql table.
   * @param catalog a pointer to the catalog object
   * @param pg_database the pointer to pg_database
   */
  DatabaseHandle(Catalog *catalog, std::shared_ptr<catalog::SqlTableRW> pg_database);

  /**
   * Get a namespace handle for the database.
   * @return A namespace handle
   */
  NamespaceHandle GetNamespaceHandle(transaction::TransactionContext *txn, db_oid_t oid);

  /**
   * Get a database entry for a given db_oid. It's essentially equivalent to reading a
   * row from pg_database. It has to be executed in a transaction context.
   *
   * Since a transaction can only access to a single database, we should get only one
   * database entry. And it is only allowed to get the database entry of the database it
   * is correctly connected to.
   *
   * @param txn the transaction that initiates the read
   * @param oid the db_oid of the database the transaction wants to read
   * @return a shared pointer to database entry; NULL if the transaction is trying to
   * access another database.
   */
  std::shared_ptr<DatabaseEntry> GetDatabaseEntry(transaction::TransactionContext *txn, db_oid_t oid);

  /**
   * Lookup database named db_name and return an entry
   * @param txn the transaction that initiates the read
   * @param db_name the name of the database
   * @return a shared pointer to database entry; NULL if not found
   */
  std::shared_ptr<DatabaseEntry> GetOldDatabaseEntry(transaction::TransactionContext *txn, const char *db_name);

  std::shared_ptr<DatabaseEntry> GetDatabaseEntry(transaction::TransactionContext *txn, const char *db_name);

 private:
  Catalog *catalog_;

  // temporary
 public:
  std::shared_ptr<catalog::SqlTableRW> pg_database_rw_;
};

}  // namespace terrier::catalog
