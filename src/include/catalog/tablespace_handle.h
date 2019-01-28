#pragma once

#include <memory>
#include <string>
#include <utility>

#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

/**
 * A TablespaceHandle provides access to the (global) system pg_tablespace
 * catalog.
 *
 * This pg_tablespace is a subset of Postgres (v11)  pg_tablespace, and
 * contains the following fields:
 *
 * Name    SQL Type     Description
 * ----    --------     -----------
 * oid     integer
 * spcname varchar      Tablespace name
 *
 * TablespaceEntry instances provide accessors for individual rows of
 * pg_tablespace
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
    TablespaceEntry(std::shared_ptr<catalog::SqlTableRW> pg_tblspc_rw, tablespace_oid_t oid, storage::ProjectedRow *row,
                    storage::ProjectionMap map)
        : oid_(oid), row_(row), map_(std::move(map)), pg_tablespace_(pg_tblspc_rw) {}

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
    byte *GetValue(const std::string &name) {
      auto oid = pg_tablespace_->GetSqlTable()->GetSchema().GetColumn(name).GetOid();
      return GetValue(oid);
    }

    /**
     *From this entry, return col_num as an integer
     * @param col_num - column number in the schema
     * @return integer
     */
    uint32_t GetIntColInRow(int32_t col_num) { return pg_tablespace_->GetIntColInRow(col_num, row_); }

    /**
     * From this entry, return col_num as a C string.
     * @param col_num - column number in the schema
     * @return malloc'ed C string (with null terminator). Caller must
     *   free.
     */
    char *GetVarcharColInRow(int32_t col_num) { return pg_tablespace_->GetVarcharColInRow(col_num, row_); }

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
    std::shared_ptr<catalog::SqlTableRW> pg_tablespace_;
  };

  /**
   * Construct a tablespace handle. It keeps a pointer to the pg_tablespace sql table.
   * @param pg_tablespace a pointer to pg_tablespace
   */
  explicit TablespaceHandle(std::shared_ptr<catalog::SqlTableRW> pg_tablespace) : pg_tablespace_(pg_tablespace) {}

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
  std::shared_ptr<catalog::SqlTableRW> pg_tablespace_;
};

}  // namespace terrier::catalog
