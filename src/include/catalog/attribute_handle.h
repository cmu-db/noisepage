#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "catalog/catalog_entry.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog {

class Catalog;
struct SchemaCol;

/**
 * A attribute entry represent a row in pg_attribute catalog.
 */
class AttributeCatalogEntry : public CatalogEntry<col_oid_t> {
 public:
  /**
   * Constructor
   * @param oid attribute oid
   * @param sql_table associated with this entry
   * @param entry a row in pg_attribute that represents this table
   */
  AttributeCatalogEntry(col_oid_t oid, catalog::SqlTableHelper *sql_table, std::vector<type::TransientValue> &&entry)
      : CatalogEntry(oid, sql_table, std::move(entry)) {}
};

/**
 * An attribute handle provides accessors to the pg_attribute catalog.
 * Each database has it's own pg_attribute catalog.
 *
 * Following description verbatim from the Postgres documentation:
 * The catalog pg_attribute stores information about table columns.
 * There will be exactly one pg_attribute row for every column in every
 * table in the database. (There will also be attribute entries for indexes,
 * and indeed all objects that have pg_class entries.)
 *
 * The term attribute is equivalent to column and is used for historical
 * reasons.
 */
class AttributeCatalogTable {
 public:
  /**
   * Construct an attribute handle
   * @param pg_attribute a pointer to pg_attribute sql table rw helper instance
   */
  explicit AttributeCatalogTable(SqlTableHelper *pg_attribute) : pg_attribute_hrw_(pg_attribute) {}

  /**
   * Convert a attribute string to its oid representation
   * @param name the attribute
   * @param txn the transaction context
   * @return the col oid
   */
  col_oid_t NameToOid(transaction::TransactionContext *txn, const std::string &name);

  /**
   * Get an attribute entry.
   * @param txn transaction (required)
   * @param table_oid an attribute for this table
   * @param col_oid attribute for col_oid of table_oid
   * @return a shared pointer to Attribute entry; NULL if the attribute doesn't exist
   */
  std::shared_ptr<AttributeCatalogEntry> GetAttributeEntry(transaction::TransactionContext *txn, table_oid_t table_oid,
                                                           col_oid_t col_oid);

  /**
   * Get an attribute entry.
   * @param txn transaction (required)
   * @param table_oid an attribute for this table
   * @param name attribute for column name of table_oid
   * @return a shared pointer to Attribute entry;
   */
  std::shared_ptr<AttributeCatalogEntry> GetAttributeEntry(transaction::TransactionContext *txn, table_oid_t table_oid,
                                                           const std::string &name);

  /**
   * Delete all entries matching table_oid
   * @param txn transaction
   * @param table_oid to match
   */
  void DeleteEntries(transaction::TransactionContext *txn, table_oid_t table_oid);

  /**
   * Create the storage table
   */
  static SqlTableHelper *Create(transaction::TransactionContext *txn, Catalog *catalog, db_oid_t db_oid,
                                const std::string &name);

  // start Debug methods
  /**
   * Dump the contents of the table
   * @param txn
   */
  void Dump(transaction::TransactionContext *txn) {
    auto limit = static_cast<int32_t>(AttributeCatalogTable::schema_cols_.size());
    pg_attribute_hrw_->Dump(txn, limit);
  }
  // end Debug methods

  /** Used schema columns */
  static const std::vector<SchemaCol> schema_cols_;

 private:
  catalog::SqlTableHelper *pg_attribute_hrw_;
};

}  // namespace terrier::catalog
