#pragma once

#include <type/transient_value.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/catalog_sql_table.h"
#include "catalog/namespace_handle.h"

namespace terrier::catalog {

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
   * An IndexEntry represents a row in pg_index catalog.
   */
  class IndexEntry {
   public:
    /**
     * Contruct an IndexEntry.
     * @param oid: the oid of the give index, i.e. the indexrelid of the given pg_index row.
     * @param entry: the vector of all the columns of the pg_index row.
     */
    IndexEntry(index_oid_t oid, std::vector<type::TransientValue> &&entry) : oid_(oid), entry_(std::move(entry)) {}

    /**
     * Get the given column of the given pg_index row.
     * @param col_num: the index of the column.
     * @return the given row
     */
    const type::TransientValue &GetColumn(int32_t col_num) { return entry_[col_num]; }

    /**
     *  Get the index oid.
     * @return the index oid
     */
    index_oid_t GetIndexOid() { return oid_; }

   private:
    index_oid_t oid_;
    std::vector<type::TransientValue> entry_;
  };

  /**
   * Construct a IndexHandle. It keeps a pointer to the pg_index sql table.
   * @param catalog: The pointer to the catalog.
   * @param pg_index: The pointer to the pg_index sql table.
   */
  IndexHandle(Catalog *catalog, std::shared_ptr<catalog::SqlTableRW> pg_index);

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
  static std::shared_ptr<catalog::SqlTableRW> Create(transaction::TransactionContext *txn, Catalog *catalog,
                                                     db_oid_t db_oid, const std::string &name);
  /**
   * Debug methods
   */
  void Dump(transaction::TransactionContext *txn) {
    auto limit = static_cast<int32_t>(IndexHandle::schema_cols_.size());
    pg_index_rw_->Dump(txn, limit);
  }

  /** Used schema columns */
  static const std::vector<SchemaCol> schema_cols_;
  /** Unused schema columns */
  static const std::vector<SchemaCol> unused_schema_cols_;

 private:
  Catalog *catalog_;
  std::shared_ptr<catalog::SqlTableRW> pg_index_rw_;
};
}  // namespace terrier::catalog
