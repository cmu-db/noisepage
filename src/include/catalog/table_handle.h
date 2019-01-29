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
     * TODO(yangjuns): we need to change uint32_t to strings once we take varlen
     *
     * @param table_name the name of the table.
     * @param txn the transaction which wants the entry
     * @param pg_class a pointer to the pg_class catalog
     * @param pg_namespace a pointer to pg_namespace
     * @param pg_tablespace a pointer to tablespace
     */
    TableEntry(std::string table_name, transaction::TransactionContext *txn, std::shared_ptr<SqlTableRW> pg_class,
               std::shared_ptr<SqlTableRW> pg_namespace, std::shared_ptr<SqlTableRW> pg_tablespace)
        : table_name_(std::move(table_name)),
          txn_(txn),
          pg_class_(std::move(pg_class)),
          pg_namespace_(std::move(pg_namespace)),
          pg_tablespace_(std::move(pg_tablespace)) {}

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
      storage::ProjectedRow *row = pg_class_->FindRow(txn_, 1, table_name_.c_str());
      rows_.emplace_back(row);
      nsp_oid = namespace_oid_t(pg_class_->GetIntColInRow(2, row));
      tsp_oid = tablespace_oid_t(pg_class_->GetIntColInRow(3, row));

      CATALOG_LOG_TRACE("{} has namespace oid {}, tablespace_oid {}", table_name_, !nsp_oid, !tsp_oid);
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
        return pg_class_->GetVarcharColInRow(1, row);
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
     * Destruct namespace entry. It frees the memory for storing allocated memory.
     */
    ~TableEntry() {
      for (auto &r : rows_) {
        delete[] reinterpret_cast<byte *>(r);
      }
    }

   private:
    const std::string table_name_;
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
   * @param pg_class a pointer to pg_class
   * @param pg_namespace a pointer to pg_namespace
   * @param pg_tablespace a pointer to pg_tablespace
   */
  TableHandle(std::string name, std::shared_ptr<SqlTableRW> pg_class, std::shared_ptr<SqlTableRW> pg_namespace,
              std::shared_ptr<SqlTableRW> pg_tablespace)
      : nsp_name(std::move(name)),
        pg_class_(std::move(pg_class)),
        pg_namespace_(std::move(pg_namespace)),
        pg_tablespace_(std::move(pg_tablespace)) {}

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

 private:
  const std::string nsp_name;
  std::shared_ptr<SqlTableRW> pg_class_;
  std::shared_ptr<SqlTableRW> pg_namespace_;
  std::shared_ptr<SqlTableRW> pg_tablespace_;
};

}  // namespace terrier::catalog
