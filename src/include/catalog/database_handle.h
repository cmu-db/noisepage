#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/attr_def_handle.h"
#include "catalog/attribute_handle.h"
#include "catalog/catalog.h"
#include "catalog/class_handle.h"
#include "catalog/namespace_handle.h"
#include "catalog/type_handle.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog {

class Catalog;
class AttributeHandle;
class AttrDefHandle;
class NamespaceHandle;
class TypeHandle;
struct SchemaCol;

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
    DatabaseEntry(db_oid_t oid, std::vector<type::TransientValue> &&entry) : oid_(oid), entry_(std::move(entry)) {}

    /**
     * Get the value for a given column
     * @param col_num the column index
     * @return the value of the column
     */
    const type::TransientValue &GetColumn(int32_t col_num) { return entry_[col_num]; }

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
    std::vector<type::TransientValue> entry_;
  };

  /**
   * Construct a database handle. It keeps a pointer to pg_database sql table.
   * @param catalog a pointer to the catalog object
   * @param pg_database the pointer to pg_database
   */
  DatabaseHandle(Catalog *catalog, std::shared_ptr<catalog::SqlTableRW> pg_database);

  /**
   * Get a class handle for the database.
   * @return A class handle
   */
  ClassHandle GetClassHandle(transaction::TransactionContext *txn, db_oid_t oid);

  /**
   * Get a namespace handle for the database.
   * @return A namespace handle
   */
  NamespaceHandle GetNamespaceHandle(transaction::TransactionContext *txn, db_oid_t oid);

  /**
   * Get a type handle for the database.
   * @return A type handle
   */
  TypeHandle GetTypeHandle(transaction::TransactionContext *txn, db_oid_t oid);

  /**
   * Get a attribute handle for the database.
   * @return an attribute handle
   */
  AttributeHandle GetAttributeHandle(transaction::TransactionContext *txn, db_oid_t oid);

  /**
   * Get a attribute handle for the database.
   * @return an attribute handle
   */
  AttrDefHandle GetAttrDefHandle(transaction::TransactionContext *txn, db_oid_t oid);

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
  std::shared_ptr<DatabaseEntry> GetDatabaseEntry(transaction::TransactionContext *txn, const char *db_name);

  /**
   * Delete an entry in database handle.
   * @return true on success
   */
  bool DeleteEntry(transaction::TransactionContext *txn, const std::shared_ptr<DatabaseEntry> &entry);

  // start Debug methods

  /**
   * Dump the contents of the table
   * @param txn
   */
  void Dump(transaction::TransactionContext *txn) { pg_database_rw_->Dump(txn); }
  // end Debug methods

  /** Used schema columns */
  static const std::vector<SchemaCol> schema_cols_;
  /** Unused schema columns */
  static const std::vector<SchemaCol> unused_schema_cols_;

 private:
  Catalog *catalog_;
  /**
   * pg_database SQL table
   */
  std::shared_ptr<catalog::SqlTableRW> pg_database_rw_;
};

}  // namespace terrier::catalog
