#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "catalog/settings_handle.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
namespace terrier::catalog {

/**
 * pg_settings as defined by Postgres, with the following differences:
 *
 * 0. First column is an oid (required for us).
 * 1. This is a real table, not a view (to an internal function)
 * 2. enumvals is a VARCHAR (i.e. text). Postgres defines as an array, text[]
 *
 * See postgres documentation for more description of the fields
 */
const std::vector<SchemaCol> SettingsCatalogTable::schema_cols_ = {
    {0, true, "oid", type::TypeId::INTEGER},         {1, true, "name", type::TypeId::VARCHAR},
    {2, true, "setting", type::TypeId::VARCHAR},     {3, true, "unit", type::TypeId::VARCHAR},
    {4, true, "category", type::TypeId::VARCHAR},    {5, true, "short_desc", type::TypeId::VARCHAR},
    {6, true, "extra_desc", type::TypeId::VARCHAR},  {7, true, "context", type::TypeId::VARCHAR},
    {8, true, "vartype", type::TypeId::VARCHAR},     {9, true, "source", type::TypeId::VARCHAR},
    {10, true, "min_val", type::TypeId::VARCHAR},    {11, true, "max_val", type::TypeId::VARCHAR},
    {12, true, "enumvals", type::TypeId::VARCHAR},   {13, true, "boot_val", type::TypeId::VARCHAR},
    {14, true, "reset_val", type::TypeId::VARCHAR},  {15, true, "sourcefile", type::TypeId::VARCHAR},
    {16, true, "sourceline", type::TypeId::INTEGER}, {17, true, "pending_restart", type::TypeId::BOOLEAN}};

// Find entry with (row) oid and return it
std::shared_ptr<SettingsCatalogEntry> SettingsCatalogTable::GetSettingsEntry(transaction::TransactionContext *txn,
                                                                             settings_oid_t oid) {
  std::vector<type::TransientValue> search_vec, ret_row;
  search_vec.push_back(type::TransientValueFactory::GetInteger(!oid));
  ret_row = pg_settings_->FindRow(txn, search_vec);
  if (ret_row.empty()) {
    return nullptr;
  }
  return std::make_shared<SettingsCatalogEntry>(oid, pg_settings_, std::move(ret_row));
}

SqlTableHelper *SettingsCatalogTable::Create(transaction::TransactionContext *txn, Catalog *catalog, db_oid_t db_oid,
                                             const std::string &name) {
  catalog::SqlTableHelper *pg_settings;

  // get an oid
  table_oid_t pg_settings_oid(catalog->GetNextOid());

  // uninitialized storage
  pg_settings = new catalog::SqlTableHelper(pg_settings_oid);

  // columns we use
  for (auto col : SettingsCatalogTable::schema_cols_) {
    pg_settings->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }

  // now actually create, with the provided schema
  pg_settings->Create();
  // catalog->AddToMaps(db_oid, pg_settings_oid, name, pg_settings);
  return pg_settings;
}

std::string_view SettingsCatalogEntry::GetName() { return GetVarcharColumn("name"); }
std::string_view SettingsCatalogEntry::GetSetting() { return GetVarcharColumn("setting"); }
std::string_view SettingsCatalogEntry::GetUnit() { return GetVarcharColumn("unit"); }
std::string_view SettingsCatalogEntry::GetCategory() { return GetVarcharColumn("category"); }
std::string_view SettingsCatalogEntry::GetShortDesc() { return GetVarcharColumn("short_desc"); }
std::string_view SettingsCatalogEntry::GetExtraDesc() { return GetVarcharColumn("extra_desc"); }
std::string_view SettingsCatalogEntry::GetContext() { return GetVarcharColumn("context"); }
std::string_view SettingsCatalogEntry::GetVartype() { return GetVarcharColumn("vartype"); }
std::string_view SettingsCatalogEntry::GetSource() { return GetVarcharColumn("source"); }
std::string_view SettingsCatalogEntry::GetMinval() { return GetVarcharColumn("min_val"); }
std::string_view SettingsCatalogEntry::GetMaxval() { return GetVarcharColumn("max_val"); }
std::string_view SettingsCatalogEntry::GetEnumvals() { return GetVarcharColumn("enumvals"); }
std::string_view SettingsCatalogEntry::GetBootval() { return GetVarcharColumn("boot_val"); }
std::string_view SettingsCatalogEntry::GetResetval() { return GetVarcharColumn("reset_val"); }
std::string_view SettingsCatalogEntry::GetSourcefile() { return GetVarcharColumn("sourcefile"); }
int32_t SettingsCatalogEntry::GetSourceline() { return GetIntegerColumn("sourceline"); }
bool SettingsCatalogEntry::GetPendingrestart() { return GetBooleanColumn("pending_restart"); }

}  // namespace terrier::catalog
