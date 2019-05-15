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
class AttributeCatalogTable;
class AttrDefCatalogTable;
class NamespaceCatalogTable;
class TypeCatalogTable;
struct SchemaCol;

/**
 * A database entry represents a row in pg_database catalog.
 */
class DatabaseCatalogEntry : public CatalogEntry<db_oid_t> {
 public:
  /**
   * Constructor
   * @param oid database def oid
   * @param sql_table associated with this entry
   * @param entry a row in pg_database that represents this table
   */
  DatabaseCatalogEntry(db_oid_t oid, catalog::SqlTableHelper *sql_table, std::vector<type::TransientValue> &&entry)
      : CatalogEntry(oid, sql_table, std::move(entry)) {}

  // db_oid_t GetOid();
  std::string_view GetDatname();
};

/**
 * A DatabaseCatalogTable provides access to the (global) system pg_database
 * catalog.
 *
 * DatabaseCatalogEntry instances provide accessors for individual rows of
 * pg_database.
 */
class DatabaseCatalogTable {
 public:
  /**
   * Construct a database handle. It keeps a pointer to pg_database sql table.
   * @param catalog a pointer to the catalog object
   * @param pg_database the pointer to pg_database
   */
  DatabaseCatalogTable(Catalog *catalog, SqlTableHelper *pg_database);

  /**
   * Get a class handle for the database.
   * @return A class handle
   */
  ClassCatalogTable GetClassTable(transaction::TransactionContext *txn, db_oid_t oid);

  /**
   * Get a namespace handle for the database.
   * @return A namespace handle
   */
  NamespaceCatalogTable GetNamespaceTable(transaction::TransactionContext *txn, db_oid_t oid);

  /**
   * Get a type handle for the database.
   * @return A type handle
   */
  TypeCatalogTable GetTypeTable(transaction::TransactionContext *txn, db_oid_t oid);

  /**
   * Get a attribute handle for the database.
   * @return an attribute handle
   */
  AttributeCatalogTable GetAttributeTable(transaction::TransactionContext *txn, db_oid_t oid);

  /**
   * Get a attribute handle for the database.
   * @return an attribute handle
   */
  AttrDefCatalogTable GetAttrDefTable(transaction::TransactionContext *txn, db_oid_t oid);

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
  std::shared_ptr<DatabaseCatalogEntry> GetDatabaseEntry(transaction::TransactionContext *txn, db_oid_t oid);

  /**
   * Lookup database named db_name and return an entry
   * @param txn the transaction that initiates the read
   * @param db_name the name of the database
   * @return a shared pointer to database entry; NULL if not found
   */
  std::shared_ptr<DatabaseCatalogEntry> GetDatabaseEntry(transaction::TransactionContext *txn,
                                                         const std::string &db_name);

  /**
   * Delete an entry in database handle.
   * @return true on success
   */
  bool DeleteEntry(transaction::TransactionContext *txn, const std::shared_ptr<DatabaseCatalogEntry> &entry);

  // start Debug methods

  /**
   * Dump the contents of the table
   * @param txn
   */
  void Dump(transaction::TransactionContext *txn) { pg_database_rw_->Dump(txn); }
  // end Debug methods

  /** Used schema columns */
  static const std::vector<SchemaCol> schema_cols_;

 private:
  Catalog *catalog_;
  /**
   * pg_database SQL table
   */
  catalog::SqlTableHelper *pg_database_rw_;
};

}  // namespace terrier::catalog
