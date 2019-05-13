#pragma once

#include <type/transient_value.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "catalog/catalog_entry.h"
#include "catalog/catalog_sql_table.h"

namespace terrier::catalog {

class Catalog;

/**
 * An TypeEntry is a row in pg_class catalog
 */
class TypeCatalogEntry : public CatalogEntry<type_oid_t> {
 public:
  /**
   * Constructor
   * @param oid type def oid
   * @param sql_table associated with this entry
   * @param entry a row in pg_type that represents this table
   */
  TypeCatalogEntry(type_oid_t oid, catalog::SqlTableHelper *sql_table, std::vector<type::TransientValue> &&entry)
      : CatalogEntry(oid, sql_table, std::move(entry)) {}
};

/**
 * A type handle contains information about data types.
 *
 * pg_type:
 *      oid | typname | typlen | typtype | typcategory
 */
class TypeCatalogTable {
 public:
  /**
   * Construct a type handle. It keeps a pointer to the pg_type sql table.
   */
  TypeCatalogTable(Catalog *catalog, SqlTableHelper *pg_type);

  /**
   * Get the oid of a type given its name.
   */
  type_oid_t TypeToOid(transaction::TransactionContext *txn, const std::string &type);

  /**
   * Get a type entry from pg_type handle
   *
   * @param txn the transaction to run
   * @param oid type entry oid
   * @return a shared pointer to the type entry
   */
  std::shared_ptr<TypeCatalogEntry> GetTypeEntry(transaction::TransactionContext *txn, type_oid_t oid);

  /**
   * Add a type entry into pg_type handle.
   */
  void AddEntry(transaction::TransactionContext *txn, type_oid_t oid, const std::string &typname,
                namespace_oid_t typnamespace, int32_t typlen, const std::string &typtype);

  /**
   * Get a type entry from pg_type handle by name.
   */
  std::shared_ptr<TypeCatalogEntry> GetTypeEntry(transaction::TransactionContext *txn, const std::string &type);

  /**
   * Create storage table
   */
  static SqlTableHelper *Create(transaction::TransactionContext *txn, Catalog *catalog, db_oid_t db_oid,
                                const std::string &name);

  /**
   * Debug methods
   */
  void Dump(transaction::TransactionContext *txn) {
    auto limit = static_cast<int32_t>(TypeCatalogTable::schema_cols_.size());
    pg_type_rw_->Dump(txn, limit);
  }

  /** Used schema columns */
  static const std::vector<SchemaCol> schema_cols_;
  // TODO(yeshengm): we have to add support for UDF in the future
 private:
  Catalog *catalog_;
  catalog::SqlTableHelper *pg_type_rw_;
};

}  // namespace terrier::catalog
