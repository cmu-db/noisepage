#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "catalog/catalog_entry.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

class Catalog;
struct SchemaCol;

/**
 * A tablespace entry represent a row in pg_tablespace catalog.
 */
class TablespaceCatalogEntry : public CatalogEntry<tablespace_oid_t> {
 public:
  /**
   * Constructor
   * @param oid tablespace def oid
   * @param sql_table associated with this entry
   * @param entry a row in pg_tablespace that represents this table
   */
  TablespaceCatalogEntry(tablespace_oid_t oid, catalog::SqlTableHelper *sql_table,
                         std::vector<type::TransientValue> &&entry)
      : CatalogEntry(oid, sql_table, std::move(entry)) {}

  // tablespace isn't used, column access methods omitted.
};

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
class TablespaceCatalogTable {
 public:
  /**
   * Construct a tablespace handle. It keeps a pointer to the pg_tablespace sql table.
   * @param pg_tablespace a pointer to pg_tablespace
   * @param catalog pointer to the catalog class
   */
  explicit TablespaceCatalogTable(Catalog *catalog, SqlTableHelper *pg_tablespace)
      : catalog_(catalog), pg_tablespace_(pg_tablespace) {}

  /**
   * Get a tablespace entry for a given tablespace_oid. It's essentially equivalent to reading a
   * row from pg_tablespace. It has to be executed in a transaction context.
   *
   * @param txn the transaction that initiates the read
   * @param oid the tablespace_oid of the database the transaction wants to read
   * @return a shared pointer to Tablespace entry; NULL if the tablespace doesn't exist in
   * the database
   */
  std::shared_ptr<TablespaceCatalogEntry> GetTablespaceEntry(transaction::TransactionContext *txn,
                                                             tablespace_oid_t oid);

  /**
   * Get a tablespace entry for a given tablespace. It's essentially equivalent to reading a
   * row from pg_tablespace. It has to be executed in a transaction context.
   *
   * @param txn the transaction that initiates the read
   * @param name the tablespace of the database the transaction wants to read
   * @return a shared pointer to Tablespace entry; NULL if the tablespace doesn't exist in
   * the database
   */
  std::shared_ptr<TablespaceCatalogEntry> GetTablespaceEntry(transaction::TransactionContext *txn,
                                                             const std::string &name);

  /**
   * Add a tablespace
   * @param txn
   * @param name tablespace to add
   */
  void AddEntry(transaction::TransactionContext *txn, const std::string &name);

  /**
   * Create the storage table
   */
  static SqlTableHelper *Create(Catalog *catalog, db_oid_t db_oid, const std::string &name);

  /** Used schema columns */
  static const std::vector<SchemaCol> schema_cols_;

 private:
  Catalog *catalog_;
  catalog::SqlTableHelper *pg_tablespace_;
};

}  // namespace terrier::catalog
