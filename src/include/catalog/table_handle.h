#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "catalog/catalog_sql_table.h"
#include "loggers/catalog_logger.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

class Catalog;
/**
 * A tablespace handle contains information about all the tables in a database.
 * It is equivalent to pg_tables in postgres, which is a view.
 * it has columns:
 *      schemaname | tablename | tablespace
 */
class TableHandle {
 public:
  /**
   * A table entry represent a row in pg_tables catalog.
   */
  class TableEntry {
   public:
    /**
     * Constructs a table entry in pg_tables.
     *
     * @param oid the table oid
     * @param row a row in pg_class that represents this table
     * @param txn the transaction which wants the entry
     * @param pg_class a pointer to the pg_class catalog
     * @param pg_namespace a pointer to pg_namespace
     * @param pg_tablespace a pointer to tablespace
     */
    TableEntry(table_oid_t oid, storage::ProjectedRow *row, transaction::TransactionContext *txn,
               std::shared_ptr<SqlTableRW> pg_class, std::shared_ptr<SqlTableRW> pg_namespace,
               std::shared_ptr<SqlTableRW> pg_tablespace)
        : oid_(oid),
          txn_(txn),
          pg_class_(std::move(pg_class)),
          pg_namespace_(std::move(pg_namespace)),
          pg_tablespace_(std::move(pg_tablespace)) {
      rows_.emplace_back(row);
    }

    /**
     *From this entry, return col_num as an integer
     * @param col_num - column number in the schema
     * @return integer
     */
    uint32_t GetIntColInRow(int32_t col_num) {
      TERRIER_ASSERT(false, "there is no IntegerRow in this table");
      return 0;
    }

    /**
     * From this entry, return col_num as a C string.
     * @param col_num - column number in the schema
     * @return malloc'ed C string (with null terminator). Caller must
     *   free.
     */
    char *GetVarcharColInRow(int32_t col_num) {
      // get the namespace_oid and tablespace_oid of the table
      namespace_oid_t nsp_oid(0);
      tablespace_oid_t tsp_oid(0);
      nsp_oid = namespace_oid_t(pg_class_->GetIntColInRow(2, rows_[0]));
      tsp_oid = tablespace_oid_t(pg_class_->GetIntColInRow(3, rows_[0]));

      // for different attribute we need to look up different sql tables
      if (col_num == 0) {
        // schemaname
        storage::ProjectedRow *nsp_row = pg_namespace_->FindRow(txn_, 0, !nsp_oid);
        rows_.emplace_back(nsp_row);
        return pg_namespace_->GetVarcharColInRow(1, nsp_row);
      }
      if (col_num == 1) {
        // tablename
        CATALOG_LOG_TRACE("retrieve information from pg_class ... ");
        return pg_class_->GetVarcharColInRow(1, rows_[0]);
      }

      if (col_num == 2) {
        CATALOG_LOG_TRACE("looking at tablespace attribute ...");
        storage::ProjectedRow *tsp_row = pg_tablespace_->FindRow(txn_, 0, !tsp_oid);
        rows_.emplace_back(tsp_row);
        return pg_tablespace_->GetVarcharColInRow(1, tsp_row);
      }
      throw std::out_of_range("Attribute name doesn't exist");
    }

    /**
     * Get the table oid
     * @return table oid
     */
    table_oid_t GetTableOid() { return oid_; }
    
    /**
     * Destruct tablespace entry. It frees the memory for storing allocated memory.
     */
    ~TableEntry() {
      for (auto &r : rows_) {
        delete[] reinterpret_cast<byte *>(r);
      }
    }

   private:
    table_oid_t oid_;
    transaction::TransactionContext *txn_;
    // keep track of the memory allocated to represent a row in pg_table
    std::vector<storage::ProjectedRow *> rows_;
    std::shared_ptr<SqlTableRW> pg_class_;
    std::shared_ptr<SqlTableRW> pg_namespace_;
    std::shared_ptr<SqlTableRW> pg_tablespace_;
  };

  /**
   * Construct a table handle. It keeps pointers to the pg_class, pg_namespace, pg_tablespace sql tables.
   * It uses use these three tables to provide the view of pg_tables.
   * @param name the namespace which the tables belong to
   * @param pg_class a pointer to pg_class
   * @param pg_namespace a pointer to pg_namespace
   * @param pg_tablespace a pointer to pg_tablespace
   */
  TableHandle(Catalog *catalog, namespace_oid_t nsp_oid, std::shared_ptr<SqlTableRW> pg_class,
              std::shared_ptr<SqlTableRW> pg_namespace, std::shared_ptr<SqlTableRW> pg_tablespace)
      : catalog_(catalog),
        nsp_oid_(nsp_oid),
        pg_class_(std::move(pg_class)),
        pg_namespace_(std::move(pg_namespace)),
        pg_tablespace_(std::move(pg_tablespace)) {}

  /**
   * Get the table oid for a given table name
   * @param name
   * @return table oid
   */
  table_oid_t NameToOid(transaction::TransactionContext *txn, const std::string &name);

  /**
   * Get a table entry for the given table name. It's essentially equivalent to reading a
   * row from pg_tables. It has to be executed in a transaction context.
   *
   * @param txn the transaction that initiates the read
   * @param name the name of the table
   * @return a shared pointer to table entry; NULL if the namespace doesn't exist in
   * the database
   */
  std::shared_ptr<TableEntry> GetTableEntry(transaction::TransactionContext *txn, table_oid_t oid);

  /**
   * Get a table entry for the given table name. It's essentially equivalent to reading a
   * row from pg_tables. It has to be executed in a transaction context.
   *
   * @param txn the transaction that initiates the read
   * @param name the name of the table
   * @return a shared pointer to table entry; NULL if the namespace doesn't exist in
   * the database
   */
  std::shared_ptr<TableEntry> GetTableEntry(transaction::TransactionContext *txn, const std::string &name);

  /**
   * Create a SqlTable. The namespace of the table is the same as the TableHandle.
   * @param txn
   * @param schema
   */
  void CreateTable(transaction::TransactionContext *txn, Schema &schema, const std::string &name);

 private:
  Catalog *catalog_;
  namespace_oid_t nsp_oid_;
  std::shared_ptr<SqlTableRW> pg_class_;
  std::shared_ptr<SqlTableRW> pg_namespace_;
  std::shared_ptr<SqlTableRW> pg_tablespace_;
};

}  // namespace terrier::catalog
