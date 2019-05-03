#pragma once

#include <type/transient_value.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/catalog_entry.h"
#include "catalog/catalog_sql_table.h"
#include "catalog/namespace_handle.h"

namespace terrier::catalog {

/**
 * A namespace entry represent a row in pg_namespace catalog.
 */
class IndexEntry : public CatalogEntry<index_oid_t> {
 public:
  /**
   * Constructor
   * @param oid namespace def oid
   * @param sql_table associated with this entry
   * @param entry a row in pg_namespace that represents this table
   */
  IndexEntry(index_oid_t oid, catalog::SqlTableHelper *sql_table, std::vector<type::TransientValue> &&entry)
      : CatalogEntry(oid, sql_table, std::move(entry)) {}
};

class Catalog;
/**
 * An IndexHandle contains the information about indexes.
 *
 * pg_index:
 *  indexrelid | indrelid | indnatts | indnkeyatts | indisunique | indisprimary |  indisvalid | indisready | indislive
 */
class IndexHandle {
 public:
  /**
   * Construct a IndexHandle. It keeps a pointer to the pg_index sql table.
   * @param catalog: The pointer to the catalog.
   * @param pg_index: The pointer to the pg_index sql table.
   */
  explicit IndexHandle(catalog::SqlTableHelper *pg_index);

  /**
   * Get the IndexEntry by oid from IndexHandle
   * @param txn: the transaction context of current transaction.
   * @param oid: the oid of index wanted.
   * @return: a pointer to the index entry wanted.
   */
  std::shared_ptr<IndexEntry> GetIndexEntry(transaction::TransactionContext *txn, index_oid_t oid);

  /**
   * Add an IndexEntry to the IndexHandle
   */
  void AddEntry(transaction::TransactionContext *txn, index_oid_t indexrelid, table_oid_t indrelid, int32_t indnatts,
                int32_t indnkeyatts, bool indisunique, bool indisprimary, bool indisvalid, bool indisready,
                bool indislive);

  /**
   * Create storage table
   */
  static catalog::SqlTableHelper *Create(transaction::TransactionContext *txn, Catalog *catalog, db_oid_t db_oid,
                                         const std::string &name);
  /**
   * Debug methods
   */
  void Dump(transaction::TransactionContext *txn) {
    auto limit = static_cast<int32_t>(IndexHandle::schema_cols_.size());
    pg_index_rw_->Dump(txn, limit);
  }

  static const std::vector<SchemaCol> schema_cols_;

 private:
  catalog::SqlTableHelper *pg_index_rw_;
};
}  // namespace terrier::catalog
