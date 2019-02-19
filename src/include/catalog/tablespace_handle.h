#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

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
     * @param entry: the row as a vector of values
     */
    TablespaceEntry(tablespace_oid_t oid, std::vector<type::Value> entry) : oid_(oid), entry_(std::move(entry)) {}

    /**
     * Return the tablespace_oid
     * @return tablespace_oid the tablespace oid
     */
    tablespace_oid_t GetTablespaceOid() { return oid_; }

    /**
     * Get the value for a given column
     * @param col_num the column index
     * @return the value of the column
     */
    const type::Value &GetColumn(int32_t col_num) { return entry_[col_num]; }

   private:
    tablespace_oid_t oid_;
    std::vector<type::Value> entry_;
  };

  /**
   * Construct a tablespace handle. It keeps a pointer to the pg_tablespace sql table.
   * @param pg_tablespace a pointer to pg_tablespace
   */
  explicit TablespaceHandle(std::shared_ptr<catalog::SqlTableRW> pg_tablespace)
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
  std::shared_ptr<catalog::SqlTableRW> pg_tablespace_;
};

}  // namespace terrier::catalog
