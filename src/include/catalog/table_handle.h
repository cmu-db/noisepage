#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/attribute_handle.h"
#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "catalog/catalog_sql_table.h"
#include "loggers/catalog_logger.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
#include "type/transient_value.h"
#include "type/transient_value_factory.h"

namespace terrier::catalog {

class Catalog;
class AttributeCatalogTable;

/**
 * A table entry represent a row in pg_tables catalog.
 *
 * A row in pg_tables needs information from pg_class, pg_namespace, pg_tablespace
 *    1) it needs schemaname from pg_namespace
 *    2) it needs tablename from pg_class
 *    3) it needs tablespace from pg_tablespace
 *
 * The class uses a vector of size 3 to stores 3 rows in these three tables.
 *    rows_[0] stores a row in pg_namespace which contains namespace information for this TableEntry
 *    rows_[1] stores a row in pg_class which contains table information for this TableEntry
 *    rows_[2] stores a row in pg_tablespace which contains table information for this TableEntry
 */
class TableCatalogEntry {
 public:
  /**
   * Constructs a table entry in pg_tables.
   * TODO(yeshengm): consider using universal reference here??? we are now forcing a rvalue ref.
   *
   * @param oid the table oid
   * @param row a row in pg_class that represents this table
   * @param txn the transaction which wants the entry
   * @param pg_namespace a pointer to pg_namespace
   * @param pg_tablespace a pointer to tablespace
   */
  TableCatalogEntry(table_oid_t oid, std::vector<type::TransientValue> &&row, transaction::TransactionContext *txn,
                    SqlTableHelper *pg_namespace, SqlTableHelper *pg_tablespace)
      : oid_(oid), txn_(txn), pg_namespace_(pg_namespace), pg_tablespace_(pg_tablespace) {
    rows_.resize(3);
    rows_[1] = std::move(row);
  }

  std::string_view GetSchemaname();
  std::string_view GetTablename();
  /* future
   * boolean GetHasindexes();
   * boolean GetHasrules();
   * boolean GetHastriggers();
   */

  /**
   * Get the value for a given column number
   * @param col_num the column number
   * @return the value
   */
  const type::TransientValue &GetColInRow(uint32_t col_num) {
    // TODO(yangjuns): error handling
    // get the namespace_oid and tablespace_oid of the table
    namespace_oid_t nsp_oid = namespace_oid_t(type::TransientValuePeeker::PeekInteger(rows_[1][3]));
    tablespace_oid_t tsp_oid = tablespace_oid_t(type::TransientValuePeeker::PeekInteger(rows_[1][4]));

    // for different attribute we need to look up different sql tables
    switch (col_num) {
      case 0: {
        // schemaname
        if (rows_[0].empty()) {
          std::vector<type::TransientValue> search_vec;
          search_vec.emplace_back(type::TransientValueFactory::GetInteger(!nsp_oid));
          rows_[0] = pg_namespace_->FindRow(txn_, search_vec);
          // TODO(pakhtar): error checking
        }
        return rows_[0][1];
      }
      case 1: {
        // tablename
        return rows_[1][2];
      }
      case 2: {
        if (rows_[2].empty()) {
          std::vector<type::TransientValue> search_vec;
          search_vec.emplace_back(type::TransientValueFactory::GetInteger(!tsp_oid));
          rows_[2] = pg_tablespace_->FindRow(txn_, search_vec);
          // TODO(pakhtar): error checking
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
  std::vector<std::vector<type::TransientValue>> rows_;
  // SqlTableHelper *pg_class_;
  SqlTableHelper *pg_namespace_;
  SqlTableHelper *pg_tablespace_;
};

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
class TableCatalogView {
 public:
  /**
   * Construct a table handle. It keeps pointers to the pg_class, pg_namespace, pg_tablespace sql tables.
   * It uses use these three tables to provide the view of pg_tables.
   * @param catalog a pointer to catalog
   * @param nsp_oid the namespace oid which the tables belong to
   * @param pg_class a pointer to pg_class
   * @param pg_namespace a pointer to pg_namespace
   * @param pg_tablespace a pointer to pg_tablespace
   */
  TableCatalogView(Catalog *catalog, namespace_oid_t nsp_oid, SqlTableHelper *pg_class, SqlTableHelper *pg_namespace,
                   SqlTableHelper *pg_tablespace)
      : catalog_(catalog),
        nsp_oid_(nsp_oid),
        pg_class_(pg_class),
        pg_namespace_(pg_namespace),
        pg_tablespace_(pg_tablespace) {}

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
  std::shared_ptr<TableCatalogEntry> GetTableEntry(transaction::TransactionContext *txn, table_oid_t oid);

  /**
   * Get a table entry for the given table name. It's essentially equivalent to reading a
   * row from pg_tables. It has to be executed in a transaction context.
   *
   * @param txn the transaction that initiates the read
   * @param name the name of the table
   * @return a shared pointer to table entry; NULL if the namespace doesn't exist in
   * the database
   */
  std::shared_ptr<TableCatalogEntry> GetTableEntry(transaction::TransactionContext *txn, const std::string &name);

  /**
   * Create a SqlTable. The namespace of the table is the same as the TableHandle.
   * @param txn the transaction context
   * @param name the table name
   * @param schema the table schema
   */
  SqlTableHelper *CreateTable(transaction::TransactionContext *txn, const Schema &schema, const std::string &name);

  /**
   * Get a pointer to the table for read and write
   * @param txn the transaction context
   * @param oid the oid of the table
   * @return a pointer to the SqlTableRW
   */
  SqlTableHelper *GetTable(transaction::TransactionContext *txn, table_oid_t oid);

  /**
   * Get a pointer to the table for read and write
   * @param txn the transaction context
   * @param name the name of the table
   * @return a pointer to the SqlTableRW
   */
  SqlTableHelper *GetTable(transaction::TransactionContext *txn, const std::string &name);

 private:
  Catalog *catalog_;
  namespace_oid_t nsp_oid_;
  SqlTableHelper *pg_class_;
  SqlTableHelper *pg_namespace_;
  SqlTableHelper *pg_tablespace_;
};

}  // namespace terrier::catalog
