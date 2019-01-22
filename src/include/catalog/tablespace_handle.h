#pragma once

#include <memory>
#include <string>
#include <utility>

#include "catalog/catalog_defs.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

/**
 * A tablespace handle contains information about all the tablespaces. It is equivalent to pg_tablespace
 * in postgres
 */
class TablespaceHandle {
 public:
  /**
   * A tablespace entry represent a row in pg_tablespace catalog.
   */
  class TablespaceEntry {
   public:
    /**
     * Constructs a tablespace entry.
     * @param oid the tablespace_oid of the underlying database
     * @param row a pointer points to the projection of the row
     * @param map a map that encodes how to access attributes of the row
     * @param pg_tablespace a pointer to the pg_tablespace sql table
     */
    TablespaceEntry(tablespace_oid_t oid, storage::ProjectedRow *row, storage::ProjectionMap map,
                    std::shared_ptr<storage::SqlTable> pg_tablespace)
        : oid_(oid), row_(row), map_(std::move(map)), pg_tablespace_(std::move(pg_tablespace)) {}

    /**
     * Get the value of an attribute by col_oid
     * @param col the col_oid of the attribute
     * @return a pointer to the attribute value
     */
    byte *GetValue(col_oid_t col) { return row_->AccessWithNullCheck(map_[col]); }

    /**
     * Get the value of an attribute by name
     * @param name the name of the attribute
     * @return a pointer to the attribute value
     */
    byte *GetValue(const std::string &name) { return GetValue(pg_tablespace_->GetSchema().GetColumn(name).GetOid()); }

    /**
     * Return the tablespace_oid
     * @return tablespace_oid the tablespace oid
     */
    tablespace_oid_t GetTablespaceOid() { return oid_; }

    /**
     * Destruct tablespace entry. It frees the memory for storing the projected row.
     */
    ~TablespaceEntry() {
      TERRIER_ASSERT(row_ != nullptr, "tablespace entry should always represent a valid row");
      delete[] reinterpret_cast<byte *>(row_);
    }

   private:
    tablespace_oid_t oid_;
    storage::ProjectedRow *row_;
    storage::ProjectionMap map_;
    std::shared_ptr<storage::SqlTable> pg_tablespace_;
  };

  /**
   * Construct a tablespace handle. It keeps a pointer to the pg_tablespace sql table.
   * @param pg_tablespace a pointer to pg_tablespace
   */
  explicit TablespaceHandle(std::shared_ptr<storage::SqlTable> pg_tablespace)
      : pg_tablespace_(std::move(pg_tablespace)) {}

  /**
   * Get a tablespace entry for a given tablespace_oid. It's essentially equivalent to reading a
   * row from pg_tablespace. It has to be executed in a transaction context.
   *
   * @param txn the transaction that initiates the read
   * @param oid the tablespace_oid of the database the transaction wants to read
   * @return a shared pointer to Tablespace entry; NULL if the tablespace doesn't exist in
   * the database
   */
  std::shared_ptr<TablespaceEntry> GetTablespaceEntry(transaction::TransactionContext *txn, tablespace_oid_t oid);

  /**
   * Get a tablespace entry for a given tablespace. It's essentially equivalent to reading a
   * row from pg_tablespace. It has to be executed in a transaction context.
   *
   * @param txn the transaction that initiates the read
   * @param name the tablespace of the database the transaction wants to read
   * @return a shared pointer to Tablespace entry; NULL if the tablespace doesn't exist in
   * the database
   */
  std::shared_ptr<TablespaceEntry> GetTablespaceEntry(transaction::TransactionContext *txn, const std::string &name);

 private:
  std::shared_ptr<storage::SqlTable> pg_tablespace_;
};

}  // namespace terrier::catalog
