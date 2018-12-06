#pragma once

#include <loggers/catalog_logger.h>
#include <memory>
#include <utility>
#include <vector>
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

/**
 * A database handle represents a database in the system. It's the entry point for access data
 * stored in this database.
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
     */
    DatabaseEntry(db_oid_t oid, storage::ProjectedRow *row, storage::ProjectionMap map)
        : oid_(oid), row_(row), map_(std::move(map)) {}
    /**
     * Get the value of an attribute
     * @param col the col_oid of the attribute
     * @return a pointer to the attribute value
     */
    byte *GetValue(col_oid_t col) { return row_->AccessWithNullCheck(map_[col]); }

    /**
     * Return the db_oid of the underlying database
     * @return db_oid of the database
     */
    db_oid_t GetDatabase() { return oid_; };

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
  };

  /**
   * Construct a database handle. It keeps a pointer to pg_databse sql table.
   * @param oid the db_oid of the database
   * @param pg_database the pointer to pg_databse
   */
  DatabaseHandle(db_oid_t oid, std::shared_ptr<storage::SqlTable> pg_database) : oid_(oid), pg_database_(pg_database) {}

  /**
   * Get a database entry for a given db_oid. It's essentially equivalent to reading a
   * row from pg_databse. It has to be executed in a transaction context.
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
  db_oid_t oid_;
  std::shared_ptr<storage::SqlTable> pg_database_;
};

}  // namespace terrier::catalog
