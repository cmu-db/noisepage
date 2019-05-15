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
#include "type/transient_value.h"

namespace terrier::catalog {

class Catalog;
struct SchemaCol;

/**
 * An AttrDefEntry is a row in pg_attrdef catalog
 */
class AttrDefCatalogEntry : public CatalogEntry<col_oid_t> {
 public:
  /**
   * Constructor
   * @param oid attributed def oid
   * @param sql_table associated with this entry
   * @param entry a row in pg_attrdef that represents this table
   */
  AttrDefCatalogEntry(col_oid_t oid, catalog::SqlTableHelper *sql_table, std::vector<type::TransientValue> &&entry)
      : CatalogEntry(oid, sql_table, std::move(entry)) {}

  // col_oid_t GetOid();
  int32_t GetAdrelid();
  int32_t GetAdnum();
  std::string_view GetAdbin();
};

/**
 * AttrDef (attribute default) contains the default value for attributes
 * (i.e. a column), where such a default has been defined.
 */
class AttrDefCatalogTable {
 public:
  /**
   * Get a specific attrdef entry.
   * @param txn the transaction that initiates the read
   * @param oid which entry to return
   * @return a shared pointer to AttrDef entry;
   *         NULL if the entry doesn't exist.
   */
  std::shared_ptr<AttrDefCatalogEntry> GetAttrDefEntry(transaction::TransactionContext *txn, col_oid_t oid);

  /**
   * Constructor
   * @param pg_attrdef a pointer to pg_attrdef sql table rw helper instance
   */
  explicit AttrDefCatalogTable(SqlTableHelper *pg_attrdef) : pg_attrdef_rw_(pg_attrdef) {}

  /**
   * Delete all entries matching table_oid
   * @param txn transaction
   * @param table_oid to match
   */
  void DeleteEntries(transaction::TransactionContext *txn, table_oid_t table_oid);

  /**
   * Create the storage table
   * @param txn the txn that creates this table
   * @param catalog ptr to the catalog
   * @param db_oid db_oid of this handle
   * @param name catalog name
   * @return a shared pointer to the catalog table
   */
  static SqlTableHelper *Create(transaction::TransactionContext *txn, Catalog *catalog, db_oid_t db_oid,
                                const std::string &name);

  /**
   * Debug methods
   */
  void Dump(transaction::TransactionContext *txn) { pg_attrdef_rw_->Dump(txn); }

  /** Used schema columns */
  static const std::vector<SchemaCol> schema_cols_;

 private:
  // storage for this table
  catalog::SqlTableHelper *pg_attrdef_rw_;
};
}  // namespace terrier::catalog
