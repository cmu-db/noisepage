#pragma once

#include <memory>
#include <string>
#include <utility>

#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "catalog/table_handle.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

/**
 * A namespace handle contains information about all the namespaces in a database. It is used to
 * retrieve namespace related information and it serves as the entry point for access the tables
 * under different namespaces.
 */
class NamespaceHandle {
 public:
  /**
   * A namespace entry represent a row in pg_namespace catalog.
   */
  class NamespaceEntry {
   public:
    /**
     * Constructs a namespace entry.
     * @param oid the namespace_oid of the underlying database
     * @param row a pointer points to the projection of the row
     * @param map a map that encodes how to access attributes of the row
     * @param pg_namespace a pointer to the pg_namespace sql table
     */
    NamespaceEntry(namespace_oid_t oid, storage::ProjectedRow *row, storage::ProjectionMap map,
                   std::shared_ptr<storage::SqlTable> pg_namespace)
        : oid_(oid), row_(row), map_(std::move(map)), pg_namespace_(std::move(pg_namespace)) {}

    /**
     * Get the value of an attribute by col_oid
     * @param col the col_oid of the attribute
     * @return a pointer to the attribute value
     * @throw std::out_of_range if the column doesn't exist.
     */
    byte *GetValue(col_oid_t col) { return row_->AccessWithNullCheck(map_.at(col)); }

    /**
     * Get the value of an attribute by name
     * @param name the name of the attribute
     * @return a pointer to the attribute value
     * @throw std::out_of_range if the column doesn't exist.
     */
    byte *GetValue(const std::string &name) { return GetValue(pg_namespace_->GetSchema().GetColumn(name).GetOid()); }

    /**
     * Return the namespace_oid of the underlying database
     * @return namespace_oid of the database
     */
    namespace_oid_t GetNamespaceOid() { return oid_; }

    /**
     * Destruct namespace entry. It frees the memory for storing the projected row.
     */
    ~NamespaceEntry() {
      TERRIER_ASSERT(row_ != nullptr, "namespace entry should always represent a valid row");
      delete[] reinterpret_cast<byte *>(row_);
    }

   private:
    namespace_oid_t oid_;
    storage::ProjectedRow *row_;
    storage::ProjectionMap map_;
    std::shared_ptr<storage::SqlTable> pg_namespace_;
  };

  /**
   * Construct a namespace handle. It keeps a pointer to the pg_namespace sql table.
   * @param catalog a pointer to the catalog
   * @param oid the db oid of the underlying database
   * @param pg_namespace a pointer to pg_namespace
   */
  explicit NamespaceHandle(Catalog *catalog, db_oid_t oid, std::shared_ptr<storage::SqlTable> pg_namespace)
      : catalog_(catalog), db_oid_(oid), pg_namespace_(std::move(pg_namespace)) {}

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
   * Get a table handle
   * @return a table handle
   */
  TableHandle GetTableHandle();

 private:
  Catalog *catalog_;
  db_oid_t db_oid_;
  std::shared_ptr<storage::SqlTable> pg_namespace_;
};

}  // namespace terrier::catalog
