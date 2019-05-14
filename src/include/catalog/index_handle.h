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

/**
 * A index entry represent a row in pg_index catalog.
 */
class IndexCatalogEntry : public CatalogEntry<index_oid_t> {
 public:
  /**
   * Constructor
   * @param oid index oid
   * @param sql_table associated with this entry
   * @param entry a row in pg_index that represents this table
   */
  IndexCatalogEntry(index_oid_t oid, catalog::SqlTableHelper *sql_table, std::vector<type::TransientValue> &&entry)
      : CatalogEntry(oid, sql_table, std::move(entry)) {}
};

class Catalog;
struct SchemaCol;

/**
 * An IndexHandle contains the information about indexes.
 *
 * pg_index:
 *  indexrelid | indrelid | indnatts | indnkeyatts | indisunique | indisprimary |  indisvalid | indisready | indislive
 *
 * For more details, see PostgreSQL's documentation: https://www.postgresql.org/docs/current/catalog-pg-index.html
 */
class IndexCatalogTable {
 public:
  /**
   * Construct a IndexHandle. It keeps a pointer to the pg_index catalog.
   * @param catalog: The pointer to the catalog.
   * @param pg_index: The pointer to the pg_index sql table.
   */
  explicit IndexCatalogTable(Catalog *catalog, catalog::SqlTableHelper *pg_index);

  /**
   * Get the IndexEntry by oid from IndexHandle
   * @param txn: the transaction context of current transaction.
   * @param oid: the oid of index wanted.
   * @return: a pointer to the index entry wanted.
   */
  std::shared_ptr<IndexCatalogEntry> GetIndexEntry(transaction::TransactionContext *txn, index_oid_t oid);

  /**
   * Get the IndexEntry by oid from IndexHandle
   * @param txn the transaction context of current transaction.
   * @param index_name the name of index wanted.
   * @return a pointer to the index entry wanted.
   */
  std::shared_ptr<IndexCatalogEntry> GetIndexEntry(transaction::TransactionContext *txn, const std::string &index_name);

  /**
   * Add an entry into the pg_index catalog.
   * @param txn the transaction context
   * @param index_ptr the pointer to the index
   * @param indexrelid the id of index object
   * @param indrelid the id of indexed table
   * @param indnatts the number of index attributes
   * @param indnkeyatts the number of index key attributes
   * @param indisunique whether the index has unique constraint
   * @param indisprimary whether the index represents the primary key of the table
   * @param indisvalid whether the index is valid
   * @param indisready whether the index is ready
   * @param indislive If false, the index is being dropped
   * @param indisblocking If true, the index is being created in the blocking manner
   */
  void AddEntry(transaction::TransactionContext *txn, storage::index::Index *index_ptr, index_oid_t indexrelid,
                const std::string &indexname, table_oid_t indrelid, int32_t indnatts, int32_t indnkeyatts,
                bool indisunique, bool indisprimary, bool indisvalid, bool indisready, bool indislive,
                bool indisblocking);

  /**
   *
   * Current workaround so that columns can be set in this table
   * FIXME(yesheng): better have a unified approach.
   *
   * @param txn the transaction context
   * @param indexreloid the id of the index object
   * @param col the col name
   * @param value the value to be set
   */
  void SetEntryColumn(transaction::TransactionContext *txn, index_oid_t indexreloid, const std::string &col,
                      const type::TransientValue &value);

  /**
   * Delete the entry in the catalog
   * @param txn the transaction context
   * @param entry the target entry
   * @return true if successfull otherwise false
   */
  bool DeleteEntry(transaction::TransactionContext *txn, const std::shared_ptr<IndexCatalogEntry> &entry);

  /**
   * Create storage table
   * @param txn the transaction context
   * @param catalog the pointer to the system catalog
   * @param db_oid the object id of database
   * @param name the name of the index handle
   * @return a pointer to the sql table helper
   */
  static catalog::SqlTableHelper *Create(transaction::TransactionContext *txn, Catalog *catalog, db_oid_t db_oid,
                                         const std::string &name);
  /**
   * Debug methods
   * @param txn the transaction context
   */
  void Dump(transaction::TransactionContext *txn) {
    auto limit = static_cast<int32_t>(IndexCatalogTable::schema_cols_.size());
    pg_index_rw_->Dump(txn, limit);
  }

  /**
   * Schema columns
   */
  static const std::vector<SchemaCol> schema_cols_;

 private:
  Catalog *catalog_;
  catalog::SqlTableHelper *pg_index_rw_;
};
}  // namespace terrier::catalog
