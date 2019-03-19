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

// Find entry with (row) oid and return it
std::shared_ptr<SettingsHandle::SettingsEntry> SettingsHandle::GetSettingsEntry(transaction::TransactionContext *txn,
                                                                                col_oid_t oid) {
  std::vector<type::Value> search_vec, ret_row;
  search_vec.push_back(type::ValueFactory::GetIntegerValue(!oid));
  ret_row = pg_settings_->FindRow(txn, search_vec);
  return std::make_shared<SettingsEntry>(oid, ret_row);
}

std::shared_ptr<catalog::SqlTableRW> SettingsHandle::Create(transaction::TransactionContext *txn, Catalog *catalog,
                                                            db_oid_t db_oid, const std::string &name) {
  std::shared_ptr<catalog::SqlTableRW> pg_settings;

  // get an oid
  table_oid_t pg_settings_oid(catalog->GetNextOid());

  // uninitialized storage
  pg_settings = std::make_shared<catalog::SqlTableRW>(pg_settings_oid);

  // columns we use
  for (auto col : SettingsHandle::schema_cols_) {
    pg_settings->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }

  // columns we don't use
  for (auto col : SettingsHandle::unused_schema_cols_) {
    pg_settings->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }
  // now actually create, with the provided schema
  pg_settings->Create();
  catalog->AddToMaps(db_oid, pg_settings_oid, name, pg_settings);
  return pg_settings;
}

/**
 * pg_settings as defined by Postgres, with the following differences:
 *
 * 0. First column is an oid (required for us).
 * 1. This is a real table, not a view (to an internal function)
 * 2. enumvals is a VARCHAR (i.e. text). Postgres defines as an array, text[]
 *
 * See postgres documentation for more description of the fields
 */
const std::vector<SchemaCol> SettingsHandle::schema_cols_ = {
    {0, "oid", type::TypeId::INTEGER},         {1, "name", type::TypeId::VARCHAR},
    {2, "setting", type::TypeId::VARCHAR},     {3, "unit", type::TypeId::VARCHAR},
    {4, "category", type::TypeId::VARCHAR},    {5, "short_desc", type::TypeId::VARCHAR},
    {6, "extra_desc", type::TypeId::VARCHAR},  {7, "context", type::TypeId::VARCHAR},
    {8, "vartype", type::TypeId::VARCHAR},     {9, "source", type::TypeId::VARCHAR},
    {10, "min_val", type::TypeId::VARCHAR},    {11, "max_val", type::TypeId::VARCHAR},
    {12, "enumvals", type::TypeId::VARCHAR},   {13, "unit", type::TypeId::VARCHAR},
    {13, "boot_val", type::TypeId::VARCHAR},   {14, "reset_val", type::TypeId::VARCHAR},
    {15, "sourcefile", type::TypeId::VARCHAR}, {16, "sourceline", type::TypeId::VARCHAR},
    {17, "sourceline", type::TypeId::INTEGER}, {18, "pending_restart", type::TypeId::BOOLEAN}};

const std::vector<SchemaCol> SettingsHandle::unused_schema_cols_ = {};

}  // namespace terrier::catalog
