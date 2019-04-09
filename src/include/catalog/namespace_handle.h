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
class NamespaceEntry {
 public:
  /**
   * Constructs a namespace entry.
   * @param oid the namespace_oid of the underlying database
   * @param entry: the row as a vector of values
   */
  NamespaceEntry(namespace_oid_t oid, std::vector<type::TransientValue> &&entry)
      : oid_(oid), entry_(std::move(entry)) {}

  /**
   * Get the value for a given column
   * @param col_num the column index
   * @return the value of the column
   */
  const type::TransientValue &GetColumn(int32_t col_num) { return entry_[col_num]; }

  /**
   * Return the namespace_oid of the underlying database
   * @return namespace_oid of the database
   */
  namespace_oid_t GetNamespaceOid() { return oid_; }

 private:
  namespace_oid_t oid_;
  std::vector<type::TransientValue> entry_;
};

/**
 * A namespace handle contains information about all the namespaces in a database. It is used to
 * retrieve namespace related information and it serves as the entry point for access the tables
 * under different namespaces.
 */
class NamespaceHandle {
 public:
  /**
   * Construct a namespace handle. It keeps a pointer to the pg_namespace sql table.
   * @param catalog a pointer to the catalog
   * @param oid the db oid of the underlying database
   * @param pg_namespace a pointer to pg_namespace sql table rw helper instance
   */
  explicit NamespaceHandle(Catalog *catalog, db_oid_t oid, std::shared_ptr<catalog::SqlTableRW> pg_namespace)
      : catalog_(catalog), db_oid_(oid), pg_namespace_hrw_(std::move(pg_namespace)) {}

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
  std::shared_ptr<NamespaceEntry> GetNamespaceEntry(transaction::TransactionContext *txn, namespace_oid_t oid);

  /**
   * Get a namespace entry for a given namespace. It's essentially equivalent to reading a
   * row from pg_namespace. It has to be executed in a transaction context.
   *
   * @param txn the transaction that initiates the read
   * @param name the namespace of the database the transaction wants to read
   * @return a shared pointer to Namespace entry; NULL if the namespace doesn't exist in
   * the database
   */
  std::shared_ptr<NamespaceEntry> GetNamespaceEntry(transaction::TransactionContext *txn, const std::string &name);

  /**
   * Add name into the namespace table.
   * Replaces CreateNameSpace.
   */
  void AddEntry(transaction::TransactionContext *txn, const std::string &name);

  /**
   * Get a table handle under the given namespace
   * @param txn the transaction context
   * @param nsp_name the namespace
   * @return a handle to all the tables under the namespace
   */
  TableHandle GetTableHandle(transaction::TransactionContext *txn, const std::string &nsp_name);

  /**
   * Create the storage table
   */
  static std::shared_ptr<catalog::SqlTableRW> Create(transaction::TransactionContext *txn, Catalog *catalog,
                                                     db_oid_t db_oid, const std::string &name);

  /**
   * Debug methods
   */
  void Dump(transaction::TransactionContext *txn) { pg_namespace_hrw_->Dump(txn); }

  /** Used schema columns */
  static const std::vector<SchemaCol> schema_cols_;
  /** Unused schema columns */
  static const std::vector<SchemaCol> unused_schema_cols_;

 private:
  Catalog *catalog_;
  // database parent of this namespace
  db_oid_t db_oid_;
  std::shared_ptr<catalog::SqlTableRW> pg_namespace_hrw_;
};

}  // namespace terrier::catalog
