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
class SettingsCatalogEntry : public CatalogEntry<settings_oid_t> {
 public:
  /**
   * Constructor
   * @param oid settings oid
   * @param sql_table associated with this entry
   * @param entry a row in pg_settings that represents this table
   */
  SettingsCatalogEntry(settings_oid_t oid, catalog::SqlTableHelper *sql_table,
                       std::vector<type::TransientValue> &&entry)
      : CatalogEntry(oid, sql_table, std::move(entry)) {}

  // settings_oid_t GetOid();
  std::string_view GetName();
  std::string_view GetSetting();
  std::string_view GetUnit();
  std::string_view GetCategory();
  std::string_view GetShortDesc();
  std::string_view GetExtraDesc();
  std::string_view GetContext();
  std::string_view GetVartype();
  std::string_view GetSource();
  std::string_view GetMinval();
  std::string_view GetMaxval();
  std::string_view GetEnumvals();
  std::string_view GetBootval();
  std::string_view GetResetval();
  std::string_view GetSourcefile();
  int32_t GetSourceline();
  bool GetPendingrestart();
};

/**
 * The settings catalog contains, global settings.
 */
class SettingsCatalogTable {
 public:
  /**
   * Get a specific settings entry.
   * @param txn the transaction that initiates the read
   * @param oid which entry to return
   * @return a shared pointer to Settings entry;
   *         NULL if the entry doesn't exist.
   */
  std::shared_ptr<SettingsCatalogEntry> GetSettingsEntry(transaction::TransactionContext *txn, settings_oid_t oid);

  /**
   * Constructor
   * @param pg_settings a pointer to pg_settings sql table helper instance
   */
  explicit SettingsCatalogTable(SqlTableHelper *pg_settings) : pg_settings_(pg_settings) {}

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
  std::shared_ptr<SettingsCatalogEntry> GetSettingsEntry(transaction::TransactionContext *txn,
                                                         const std::string &name) {
    std::vector<type::TransientValue> search_vec, ret_row;
    search_vec.push_back(type::TransientValueFactory::GetNull(type::TypeId::INTEGER));
    search_vec.push_back(type::TransientValueFactory::GetVarChar(name.c_str()));
    ret_row = pg_settings_->FindRow(txn, search_vec);
    if (ret_row.empty()) {
      return nullptr;
    }
    settings_oid_t oid(type::TransientValuePeeker::PeekInteger(ret_row[0]));
    return std::make_shared<SettingsCatalogEntry>(oid, pg_settings_, std::move(ret_row));
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

}  // namespace terrier::catalog
