#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "catalog/table_handle.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

struct SchemaCol;

/**
 * A namespace entry represent a row in pg_namespace catalog.
 */
class NamespaceCatalogEntry : public CatalogEntry<namespace_oid_t> {
 public:
  /**
   * Constructor
   * @param oid namespace def oid
   * @param sql_table associated with this entry
   * @param entry a row in pg_namespace that represents this table
   */
  NamespaceCatalogEntry(namespace_oid_t oid, catalog::SqlTableHelper *sql_table,
                        std::vector<type::TransientValue> &&entry)
      : CatalogEntry(oid, sql_table, std::move(entry)) {}

  // namespace_oid_t GetOid();
  std::string_view GetNspname();
};

/**
 * A namespace handle contains information about all the namespaces in a database. It is used to
 * retrieve namespace related information and it serves as the entry point for access the tables
 * under different namespaces.
 */
class NamespaceCatalogTable {
 public:
  /**
   * Construct a namespace handle. It keeps a pointer to the pg_namespace sql table.
   * @param catalog a pointer to the catalog
   * @param oid the db oid of the underlying database
   * @param pg_namespace a pointer to pg_namespace sql table rw helper instance
   */
  explicit NamespaceCatalogTable(Catalog *catalog, db_oid_t oid, SqlTableHelper *pg_namespace)
      : catalog_(catalog), db_oid_(oid), pg_namespace_hrw_(pg_namespace) {}

  /**
   * Convert a namespace string to its oid representation
   * @param name the namespace
   * @param txn the transaction context
   * @return the namespace oid
   */
  namespace_oid_t NameToOid(transaction::TransactionContext *txn, const std::string &name);

  /**
   * Get a namespace entry for a given namespace_oid. It's essentially equivalent to reading a
   * row from pg_namespace. It has to be executed in a transaction context.
   *
   * @param txn the transaction that initiates the read
   * @param oid the namespace_oid of the database the transaction wants to read
   * @return a shared pointer to Namespace entry; NULL if the namespace doesn't exist in
   * the database
   */
  std::shared_ptr<NamespaceCatalogEntry> GetNamespaceEntry(transaction::TransactionContext *txn, namespace_oid_t oid);

  /**
   * Get a namespace entry for a given namespace. It's essentially equivalent to reading a
   * row from pg_namespace. It has to be executed in a transaction context.
   *
   * @param txn the transaction that initiates the read
   * @param name the namespace of the database the transaction wants to read
   * @return a shared pointer to Namespace entry; NULL if the namespace doesn't exist in
   * the database
   */
  std::shared_ptr<NamespaceCatalogEntry> GetNamespaceEntry(transaction::TransactionContext *txn,
                                                           const std::string &name);

  /**
   * Add name into the namespace table.
   * Replaces CreateNameSpace.
   */
  void AddEntry(transaction::TransactionContext *txn, const std::string &name);

  /**
   * Delete an entry
   * @param txn transaction
   * @param entry to delete
   * @return true on success
   */
  bool DeleteEntry(transaction::TransactionContext *txn, const std::shared_ptr<NamespaceCatalogEntry> &entry);

  /**
   * Get a table handle under the given namespace
   * @param txn the transaction context
   * @param nsp_name the namespace
   * @return a handle to all the tables under the namespace
   */
  TableCatalogView GetTableHandle(transaction::TransactionContext *txn, const std::string &nsp_name);

  /**
   * Get a table handle under the given namespace
   * @param txn the transaction context
   * @param ns_oid
   * @return a handle to all the tables under the namespace
   */
  TableCatalogView GetTableHandle(transaction::TransactionContext *txn, namespace_oid_t ns_oid);

  /**
   * Create the storage table
   */
  static SqlTableHelper *Create(transaction::TransactionContext *txn, Catalog *catalog, db_oid_t db_oid,
                                const std::string &name);

  /**
   * Debug methods
   */
  void Dump(transaction::TransactionContext *txn) { pg_namespace_hrw_->Dump(txn); }

  /** Used schema columns */
  static const std::vector<SchemaCol> schema_cols_;

 private:
  Catalog *catalog_;
  // database parent of this namespace
  db_oid_t db_oid_;
  catalog::SqlTableHelper *pg_namespace_hrw_;
};

}  // namespace terrier::catalog
