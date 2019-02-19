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
#include "type/value.h"
#include "type/value_factory.h"
namespace terrier::catalog {

class Catalog;
/**
 * A tablespace handle contains information about all the tables in a database.
 * It is equivalent to pg_tables in postgres, which is a view.
 * pg_tables (view):
 *      schemaname | tablename | tablespace
 *
 * pg_class:
 *      __ptr | oid | relname | relnamespace | reltablespace
 *
 * pg_namespace:
 *      oid | nspname
 *
 * pg_tablespace:
 *	    oid | spcname
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
    TableEntry(table_oid_t oid, const std::vector<type::Value> &row, transaction::TransactionContext *txn,
               std::shared_ptr<SqlTableRW> pg_class, std::shared_ptr<SqlTableRW> pg_namespace,
               std::shared_ptr<SqlTableRW> pg_tablespace)
        : oid_(oid),
          txn_(txn),
          pg_class_(std::move(pg_class)),
          pg_namespace_(std::move(pg_namespace)),
          pg_tablespace_(std::move(pg_tablespace)) {
      rows_.resize(3);
      rows_[1] = row;
    }

    /**
     * Get the value for a given column number
     * @param col_num the column number
     * @return the value
     */
    const type::Value GetColInRow(uint32_t col_num) {
      // TODO(yangjuns): error handling
      // get the namespace_oid and tablespace_oid of the table
      namespace_oid_t nsp_oid = namespace_oid_t(rows_[1][3].GetIntValue());
      tablespace_oid_t tsp_oid = tablespace_oid_t(rows_[1][4].GetIntValue());

      // for different attribute we need to look up different sql tables
      switch (col_num) {
        case 0: {
          // schemaname
          if (rows_[0].empty()) {
            std::vector<type::Value> search_vec;
            search_vec.emplace_back(type::ValueFactory::GetIntegerValue(!nsp_oid));
            std::vector<type::Value> nsp_row = pg_namespace_->FindRow(txn_, search_vec);
            rows_[0] = nsp_row;
          }
          return rows_[0][1];
        }
        case 1: {
          // tablename
          return rows_[1][2];
        }
        case 2: {
          if (rows_[2].empty()) {
            std::vector<type::Value> search_vec;
            search_vec.emplace_back(type::ValueFactory::GetIntegerValue(!tsp_oid));
            std::vector<type::Value> tsp_row = pg_tablespace_->FindRow(txn_, search_vec);
            rows_[2] = tsp_row;
          }
          return rows_[2][1];
        }
        default:
          throw std::out_of_range("Attribute name doesn't exist");
      }
    }

    /**
     * Get the table oid
     * @return table oid
     */
    table_oid_t GetTableOid() { return oid_; }

   private:
    table_oid_t oid_;
    transaction::TransactionContext *txn_;
    // it stores three rows in three tables
    std::vector<std::vector<type::Value>> rows_;
    std::shared_ptr<SqlTableRW> pg_class_;
    std::shared_ptr<SqlTableRW> pg_namespace_;
    std::shared_ptr<SqlTableRW> pg_tablespace_;
  };

  /**
   * Construct a table handle. It keeps pointers to the pg_class, pg_namespace, pg_tablespace sql tables.
   * It uses use these three tables to provide the view of pg_tables.
   * @param catalog a pointer to catalog
   * @param nsp_oid the namespace oid which the tables belong to
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
   * @param txn the transaction context
   * @param name the table name
   * @return table oid
   */
  table_oid_t NameToOid(transaction::TransactionContext *txn, const std::string &name);

  /**
   * Get a table entry for the given table oid. It's essentially equivalent to reading a
   * row from pg_tables. It has to be executed in a transaction context.
   *
   * @param txn the transaction that initiates the read
   * @param oid the table oid
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
   * @param txn the transaction context
   * @param name the table name
   * @param schema the table schema
   */
  SqlTableRW *CreateTable(transaction::TransactionContext *txn, const Schema &schema, const std::string &name);

  /**
   * Get a pointer to the table for read and write
   * @param txn the transaction context
   * @param oid the oid of the table
   * @return a pointer to the SqlTableRW
   */
  SqlTableRW *GetTable(transaction::TransactionContext *txn, table_oid_t oid);

  /**
   * Get a pointer to the table for read and write
   * @param txn the transaction context
   * @param name the name of the table
   * @return a pointer to the SqlTableRW
   */
  SqlTableRW *GetTable(transaction::TransactionContext *txn, const std::string &name);

 private:
  Catalog *catalog_;
  namespace_oid_t nsp_oid_;
  std::shared_ptr<SqlTableRW> pg_class_;
  std::shared_ptr<SqlTableRW> pg_namespace_;
  std::shared_ptr<SqlTableRW> pg_tablespace_;
};

}  // namespace terrier::catalog
