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
class AttrDefEntry : public CatalogEntry<col_oid_t> {
 public:
  /**
   * Constructor
   * @param oid attributed def oid
   * @param entry a row in pg_attrdef that represents this table
   */
  AttrDefEntry(col_oid_t oid, catalog::SqlTableRW *sql_table, std::vector<type::TransientValue> &&entry) :
  CatalogEntry(oid, sql_table, std::move(entry)) {}
};

/**
 * AttrDef (attribute default) contains the default value for attributes
 * (i.e. a column), where such a default has been defined.
 */
class AttrDefHandle {
 public:
  /**
   * Get a specific attrdef entry.
   * @param txn the transaction that initiates the read
   * @param oid which entry to return
   * @return a shared pointer to AttrDef entry;
   *         NULL if the entry doesn't exist.
   */
  std::shared_ptr<AttrDefEntry> GetAttrDefEntry(transaction::TransactionContext *txn, col_oid_t oid);

  /**
   * Constructor
   * @param pg_attrdef a pointer to pg_attrdef sql table rw helper instance
   */
  explicit AttrDefHandle(std::shared_ptr<catalog::SqlTableRW> pg_attrdef) : pg_attrdef_rw_(std::move(pg_attrdef)) {}

  /**
   * Create the storage table
   * @param txn the txn that creates this table
   * @param catalog ptr to the catalog
   * @param db_oid db_oid of this handle
   * @param name catalog name
   * @return a shared pointer to the catalog table
   */
  static std::shared_ptr<catalog::SqlTableRW> Create(transaction::TransactionContext *txn, Catalog *catalog,
                                                     db_oid_t db_oid, const std::string &name);

  /**
   * Debug methods
   */
  void Dump(transaction::TransactionContext *txn) { pg_attrdef_rw_->Dump(txn); }

  /** Used schema columns */
  static const std::vector<SchemaCol> schema_cols_;
  /** Unused schema columns */
  static const std::vector<SchemaCol> unused_schema_cols_;

 private:
  // not sure if needed..
  // Catalog *catalog_;
  // database containing this table
  // db_oid_t db_oid_;
  // storage for this table
  std::shared_ptr<catalog::SqlTableRW> pg_attrdef_rw_;
};
}  // namespace terrier::catalog
