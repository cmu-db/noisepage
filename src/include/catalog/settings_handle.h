#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "catalog/table_handle.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

class Catalog;

/**
 * An SettingsEntry is a row in pg_setting catalog
 */
class SettingsEntry : public CatalogEntry<settings_oid_t> {
 public:
  /**
   * Constructor
   * @param oid settings oid
   * @param sql_table associated with this entry
   * @param entry a row in pg_settings that represents this table
   */
  SettingsEntry(settings_oid_t oid, catalog::SqlTableHelper *sql_table, std::vector<type::TransientValue> &&entry)
      : CatalogEntry(oid, sql_table, std::move(entry)) {}
};

/**
 * The settings catalog contains, global settings.
 */
class SettingsHandle {
 public:
  /**
   * Get a specific settings entry.
   * @param txn the transaction that initiates the read
   * @param oid which entry to return
   * @return a shared pointer to Settings entry;
   *         NULL if the entry doesn't exist.
   */
  std::shared_ptr<SettingsEntry> GetSettingsEntry(transaction::TransactionContext *txn, settings_oid_t oid);

  /**
   * Constructor
   * @param pg_settings a pointer to pg_settings sql table helper instance
   */
  explicit SettingsHandle(SqlTableHelper *pg_settings) : pg_settings_(pg_settings) {}

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
   * Insert a row
   * @param txn transaction to insert a row
   * @param row row
   */
  void InsertRow(transaction::TransactionContext *txn, const std::vector<type::TransientValue> &row) {
    pg_settings_->InsertRow(txn, row);
  }

  /**
   * Get the settings entry
   * @param txn transaction to query
   * @param name settings name
   * @return settings entry
   */
  std::shared_ptr<SettingsEntry> GetSettingsEntry(transaction::TransactionContext *txn, const std::string &name) {
    std::vector<type::TransientValue> search_vec, ret_row;
    search_vec.push_back(type::TransientValueFactory::GetNull(type::TypeId::INTEGER));
    search_vec.push_back(type::TransientValueFactory::GetVarChar(name.c_str()));
    ret_row = pg_settings_->FindRow(txn, search_vec);
    if (ret_row.empty()) {
      return nullptr;
    }
    settings_oid_t oid(type::TransientValuePeeker::PeekInteger(ret_row[0]));
    return std::make_shared<SettingsEntry>(oid, pg_settings_, std::move(ret_row));
  }

  /**
   * Debug methods
   */
  void Dump(transaction::TransactionContext *txn) { pg_settings_->Dump(txn); }

  /** Used schema columns */
  static const std::vector<SchemaCol> schema_cols_;

 private:
  // storage for this table
  catalog::SqlTableHelper *pg_settings_;
};

enum class SettingsTableColumn {
  OID = 0,
  NAME = 1,
  SETTING = 2,
  UNIT = 3,
  CATEGORY = 4,
  SHORT_DESC = 5,
  EXTRA_DESC = 6,
  CONTEXT = 7,
  VARTYPE = 8,
  SOURCE = 9,
  MIN_VAL = 10,
  MAX_VAL = 11,
  ENUMVALS = 12,
  BOOT_VAL = 13,
  RESET_VAL = 14,
  SOURCEFILE = 15,
  SOURCELINE = 16,
  PENDING_RESTART = 17
};

}  // namespace terrier::catalog
