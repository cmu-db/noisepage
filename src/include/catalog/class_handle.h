#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "catalog/catalog_entry.h"
#include "catalog/table_handle.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

class Catalog;
struct SchemaCol;

/**
 * An ClassEntry is a row in pg_class catalog
 */
class ClassCatalogEntry : public CatalogEntry<col_oid_t> {
 public:
  /**
   * Constructor
   * @param oid class def oid
   * @param sql_table associated with this entry
   * @param entry a row in pg_class that represents this table
   */
  ClassCatalogEntry(col_oid_t oid, catalog::SqlTableHelper *sql_table, std::vector<type::TransientValue> &&entry)
      : CatalogEntry(oid, sql_table, std::move(entry)) {}
};

/**
 * Class (equiv. of pg_class) stores much of the metadata for
 * anything that has columns and is like a table.
 */
class ClassCatalogTable {
 public:
  /**
   * Constructor
   * @param catalog the global catalog object
   * @param pg_class the pg_class sql table rw helper instance
   */
  explicit ClassCatalogTable(Catalog *catalog, SqlTableHelper *pg_class) : catalog_(catalog), pg_class_rw_(pg_class) {}

  /**
   * Get a specific Class entry.
   * @param txn the transaction that initiates the read
   * @param oid which entry to return
   * @return a shared pointer to Class entry;
   *         NULL if the entry doesn't exist.
   */
  std::shared_ptr<ClassCatalogEntry> GetClassEntry(transaction::TransactionContext *txn, col_oid_t oid);

  /**
   * Get a class entry by name
   * @param txn transaction
   * @param name to lookup
   * @return a shared ptr to a Class entry.
   */
  std::shared_ptr<ClassCatalogEntry> GetClassEntry(transaction::TransactionContext *txn, const char *name);

  /**
   * Get a class entry by name
   * @param txn transaction
   * @param ns_oid namespace oid
   * @param name to lookup
   * @return a shared ptr to a Class entry.
   */
  std::shared_ptr<ClassCatalogEntry> GetClassEntry(transaction::TransactionContext *txn, namespace_oid_t ns_oid,
                                                   const char *name);

  /**
   * Add row into the Class table.
   * @param txn transaction to run
   * @param tbl_ptr ptr to the table
   * @param entry_oid entry oid
   * @param name class name
   * @param ns_oid namespace oid
   * @param ts_oid tablespace oid
   */
  void AddEntry(transaction::TransactionContext *txn, int64_t tbl_ptr, int32_t entry_oid, const std::string &name,
                int32_t ns_oid, int32_t ts_oid);

  /**
   * Create the storage table
   * @param txn the txn that creates this table
   * @param catalog ptr to the catalog
   * @param db_oid db_oid of this handle
   * @param name catalog name
   * @return a shared pointer to the catalog table
   */
  static SqlTableHelper *Create(transaction::TransactionContext *txn, Catalog *catalog, db_oid_t db_oid,
                                const std::string &name);

  /**
   * Delete a entry from the class table
   * @param txn transaction
   * @param ns_oid namespace oid of entry to delete
   * @param col_oid column oid of entry to delete
   * @return true on success
   */
  bool DeleteEntry(transaction::TransactionContext *txn, namespace_oid_t ns_oid, col_oid_t col_oid);

  /**
   * Delete an entry in ClassHandle
   * @return true on success
   */
  bool DeleteEntry(transaction::TransactionContext *txn, const std::shared_ptr<ClassCatalogEntry> &entry);

  /**
   * Debug methods
   */
  void Dump(transaction::TransactionContext *txn) { pg_class_rw_->Dump(txn); }

  /** Used schema columns */
  static const std::vector<SchemaCol> schema_cols_;

 private:
  Catalog *catalog_;
  // storage for this table
  catalog::SqlTableHelper *pg_class_rw_;
};
}  // namespace terrier::catalog
