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
 * The settings catalog contains, global settings.
 */
class SettingsHandle {
 public:
  /**
   * An SettingsEntry is a row in pg_setting catalog
   */
  class SettingsEntry {
   public:
    /**
     * Constructs a Settings entry.
     * @param oid
     * @param entry: the row as a vector of values
     */
    SettingsEntry(settings_oid_t oid, std::vector<type::Value> entry) : oid_(oid), entry_(std::move(entry)) {}

    /**
     * Get the value for a given column
     * @param col_num the column index
     * @return the value of the column
     */
    const type::Value &GetColumn(int32_t col_num) { return entry_[col_num]; }

    /**
     * Set the value for a given column
     * @param col_num the column index
     * @param value the value of the column
     */
    void SetColumn(int32_t col_num, const type::Value &value) { entry_[col_num] = value; }

    /**
     * Return the settings_oid of the attribute
     * @return settings_oid of the attribute
     */
    settings_oid_t GetSettingsOid() { return oid_; }

   private:
    // the row
    settings_oid_t oid_;
    std::vector<type::Value> entry_;
  };

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
  explicit SettingsHandle(std::shared_ptr<catalog::SqlTableRW> pg_settings) : pg_settings_(std::move(pg_settings)) {}

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
   * Insert row into pg_settings table
   * @param txn the txn that inserts the row
   * @param row the row to be inserted
   */
  void InsertRow(transaction::TransactionContext *txn, const std::vector<type::Value> &row) {
    pg_settings_->InsertRow(txn, row);
  }

  /**
   * Get settings entry by its name attribute
   * @param txn the txn that gets the entry
   * @param name the name of the entry to get
   * @return a shared pointer to the settings entry
   */
  std::shared_ptr<SettingsHandle::SettingsEntry> GetSettingsEntry(transaction::TransactionContext *txn,
                                                                  const std::string &name) {
    std::vector<type::Value> search_vec, ret_row;
    search_vec.push_back(type::ValueFactory::GetNullValue(type::TypeId::INTEGER));
    search_vec.push_back(type::ValueFactory::GetVarcharValue(name.c_str()));
    ret_row = pg_settings_->FindRow(txn, search_vec);

    settings_oid_t oid(ret_row[0].GetIntValue());
    return std::make_shared<SettingsEntry>(oid, ret_row);
  }

  /**
   * Debug methods
   */
  void Dump(transaction::TransactionContext *txn) { pg_settings_->Dump(txn); }

  /** Used schema columns */
  static const std::vector<SchemaCol> schema_cols_;
  /** Unused schema columns */
  static const std::vector<SchemaCol> unused_schema_cols_;

 private:
  // storage for this table
  std::shared_ptr<catalog::SqlTableRW> pg_settings_;
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
