#include "catalog/namespace_handle.h"
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "loggers/catalog_logger.h"
#include "storage/block_layout.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"
namespace terrier::catalog {

std::shared_ptr<NamespaceHandle::NamespaceEntry> NamespaceHandle::GetNamespaceEntry(
    transaction::TransactionContext *txn, namespace_oid_t oid) {
  std::vector<type::Value> search_vec, ret_row;
  search_vec.push_back(type::ValueFactory::GetIntegerValue(!oid));
  ret_row = pg_namespace_hrw_->FindRow(txn, search_vec);
  return std::make_shared<NamespaceEntry>(oid, ret_row);
}

std::shared_ptr<NamespaceHandle::NamespaceEntry> NamespaceHandle::GetNamespaceEntry(
    transaction::TransactionContext *txn, const std::string &name) {
  std::vector<type::Value> search_vec, ret_row;
  search_vec.push_back(type::ValueFactory::GetNullValue(type::TypeId::INTEGER));
  search_vec.push_back(type::ValueFactory::GetVarcharValue(name.c_str()));
  ret_row = pg_namespace_hrw_->FindRow(txn, search_vec);
  namespace_oid_t oid(ret_row[0].GetIntValue());
  return std::make_shared<NamespaceEntry>(oid, ret_row);
}

namespace_oid_t NamespaceHandle::NameToOid(transaction::TransactionContext *txn, const std::string &name) {
  auto nse = GetNamespaceEntry(txn, name);
  return namespace_oid_t(nse->GetColumn(0).GetIntValue());
}

TableHandle NamespaceHandle::GetTableHandle(transaction::TransactionContext *txn, const std::string &nsp_name) {
  CATALOG_LOG_TRACE("Getting the table handle ...");
  std::string pg_class("pg_class");
  std::string pg_namespace("pg_namespace");
  std::string pg_tablespace("pg_tablespace");
  return TableHandle(catalog_, db_oid_, NameToOid(txn, nsp_name), catalog_->GetDatabaseCatalog(db_oid_, pg_class),
                     catalog_->GetDatabaseCatalog(db_oid_, pg_namespace),
                     catalog_->GetDatabaseCatalog(db_oid_, pg_tablespace));
}

void NamespaceHandle::AddEntry(transaction::TransactionContext *txn, const std::string &name) {
  std::vector<type::Value> row;

  row.emplace_back(type::ValueFactory::GetIntegerValue(catalog_->GetNextOid()));
  row.emplace_back(type::ValueFactory::GetVarcharValue(name.c_str()));
  catalog_->SetUnusedColumns(&row, NamespaceHandle::unused_schema_cols_);
  pg_namespace_hrw_->InsertRow(txn, row);
}

std::shared_ptr<catalog::SqlTableRW> NamespaceHandle::Create(transaction::TransactionContext *txn, Catalog *catalog,
                                                             db_oid_t db_oid, const std::string &name) {
  std::shared_ptr<catalog::SqlTableRW> storage_table;

  // get an oid
  table_oid_t storage_table_oid(catalog->GetNextOid());

  // uninitialized storage
  storage_table = std::make_shared<catalog::SqlTableRW>(storage_table_oid);

  // columns we use
  for (auto col : NamespaceHandle::schema_cols_) {
    storage_table->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }

  // columns we don't use
  for (auto col : NamespaceHandle::unused_schema_cols_) {
    storage_table->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }
  // now actually create, with the provided schema
  storage_table->Create();
  catalog->AddToMaps(db_oid, storage_table_oid, name, storage_table);
  // catalog->AddColumnsToPGAttribute(txn, db_oid, storage_table->GetSqlTable());
  return storage_table;
}

const std::vector<SchemaCol> NamespaceHandle::schema_cols_ = {{0, "oid", type::TypeId::INTEGER},
                                                              {1, "nspname", type::TypeId::VARCHAR}};

const std::vector<SchemaCol> NamespaceHandle::unused_schema_cols_ = {
    {2, "nspowner", type::TypeId::INTEGER},
    {3, "nspacl", type::TypeId::VARCHAR},
};

}  // namespace terrier::catalog
