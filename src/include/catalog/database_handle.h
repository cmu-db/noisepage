#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/namespace_handle.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

class Catalog;
class NamespaceHandle;

/**
 * A DatabaseHandle provides access to the (global) system pg_database
 * catalog.
 *
 * This pg_database is a subset of Postgres (v11)  pg_database, and
 * contains the following fields:
 *
 * Name    SQL Type     Description
 * ----    --------     -----------
 * oid     integer
 * datname varchar      Database name
 *
 * DatabaseEntry instances provide accessors for individual rows of
 * pg_database.
 */

class DatabaseHandle {
 public:
  /**
   * A database entry represent a row in pg_database catalog.
   */
  class DatabaseEntry {
   public:
    /**
     * Constructs a database entry.
     * @param oid the db_oid of the underlying database
     * @param row a pointer points to the projection of the row
     * @param map a map that encodes how to access attributes of the row
     * @param pg_database the pointer to the pg_database sql table
     */
    DatabaseEntry(db_oid_t oid, storage::ProjectedRow *row, storage::ProjectionMap map)
        : oid_(oid), row_(row), map_(std::move(map)) {}

    DatabaseEntry(std::shared_ptr<catalog::SqlTableRW> pg_db_sqltbl_rw, db_oid_t oid, storage::ProjectedRow *row,
                  storage::ProjectionMap map)
        : oid_(oid), row_(row), map_(std::move(map)), pg_db_sqltbl_rw_(pg_db_sqltbl_rw) {}
    /**
     * Get the value of an attribute by col_oid
     * @param col the col_oid of the attribute
     * @return a pointer to the attribute value
     * @throw std::out_of_range if the column doesn't exist.
     */
    byte *GetValue(col_oid_t col) { return row_->AccessWithNullCheck(map_.at(col)); }
    
    /**
     * Read an integer from a row
     * @param col_num column number in the schema
     * @return integer value
     */
    uint32_t GetIntColInRow(int32_t col_num) {
      auto col_oid = pg_db_sqltbl_rw_->ColNumToOid(col_num);
      byte *col_p = row_->AccessForceNotNull(map_.at(col_oid));
      auto ret_val = *(reinterpret_cast<uint32_t *>(col_p));
      return ret_val;
    }

    /**
     * Get the value of an attribute by attribute name
     * @param name the name of the attribute
     * @return a pointer to the attribute value
     * @throw std::out_of_range if the column doesn't exist.
     */
    byte *GetValue(const std::string &name) {
      auto oid = pg_db_sqltbl_rw_->GetSqlTable()->GetSchema().GetColumn(name).GetOid();
      return GetValue(oid);
    }

  /**
   * From this entry, return col_num as an integer
   * @param col_num - column number in the schema
   * @return integer
   */
    uint32_t GetIntColInRow(int32_t col_num) {
      return pg_db_sqltbl_rw_->GetIntColInRow(col_num, row_);
    }

  /**
   * From this entry, return col_num as a C string.
   * @param col_num - column number in the schema
   * @return malloc'ed C string (with null terminator). Caller must
   *   free.
   */
    char *GetVarcharColInRow(int32_t col_num) {
      return pg_db_sqltbl_rw_->GetVarcharColInRow(col_num, row_);
    }

    /**
     * Return the db_oid of the underlying database
     * @return db_oid of the database
     */
    db_oid_t GetDatabaseOid() { return oid_; }

    /**
     * Destruct database entry. It frees the memory for storing the projected row.
     */
    ~DatabaseEntry() {
      TERRIER_ASSERT(row_ != nullptr, "database entry should always represent a valid row");
      delete[] reinterpret_cast<byte *>(row_);
    }

   private:
    db_oid_t oid_;
    storage::ProjectedRow *row_;
    storage::ProjectionMap map_;

    std::shared_ptr<catalog::SqlTableRW> pg_db_sqltbl_rw_;
  };

  /**
   * Construct a database handle. It keeps a pointer to pg_database sql table.
   * @param catalog a pointer to the catalog object
   * @param oid the db_oid of the database
   * @param pg_database the pointer to pg_database
   */
  DatabaseHandle(Catalog *catalog, db_oid_t oid, std::shared_ptr<catalog::SqlTableRW> pg_database);

  /**
   * Get a namespace handle for the database.
   * @return A namespace handle
   */
  NamespaceHandle GetNamespaceHandle();

  /**
   * Get a database entry for a given db_oid. It's essentially equivalent to reading a
   * row from pg_database. It has to be executed in a transaction context.
   *
   * Since a transaction can only access to a single database, we should get only one
   * database entry. And it is only allowed to get the database entry of the database it
   * is correctly connected to.
   *
   * @param txn the transaction that initiates the read
   * @param oid the db_oid of the database the transaction wants to read
   * @return a shared pointer to database entry; NULL if the transaction is trying to
   * access another database.
   */
  std::shared_ptr<DatabaseEntry> GetDatabaseEntry(transaction::TransactionContext *txn, db_oid_t oid);

 private:
  Catalog *catalog_;
  db_oid_t oid_;
  std::shared_ptr<catalog::SqlTableRW> pg_database_rw_;
};

}  // namespace terrier::catalog
