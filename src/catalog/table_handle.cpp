#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/namespace_handle.h"
#include "catalog/schema.h"
#include "loggers/catalog_logger.h"
#include "storage/block_layout.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"

namespace terrier::catalog {

std::shared_ptr<TableCatalogEntry> TableCatalogView::GetTableEntry(transaction::TransactionContext *txn,
                                                                   table_oid_t oid) {
  // get the namespace_oid of the table to check if it's a table under current namespace
  namespace_oid_t nsp_oid(0);
  std::vector<type::TransientValue> search_vec;
  search_vec.emplace_back(type::TransientValueFactory::GetNull(type::TypeId::BIGINT));
  search_vec.emplace_back(type::TransientValueFactory::GetInteger(!oid));

  std::vector<type::TransientValue> row = pg_class_->FindRow(txn, search_vec);
  if (row.empty()) {
    return nullptr;
  }
  nsp_oid = namespace_oid_t(type::TransientValuePeeker::PeekInteger(row[3]));
  if (nsp_oid != nsp_oid_) return nullptr;
  return std::make_shared<TableCatalogEntry>(oid, std::move(row), txn, pg_namespace_, pg_tablespace_);
}

std::shared_ptr<TableCatalogEntry> TableCatalogView::GetTableEntry(transaction::TransactionContext *txn,
                                                                   const std::string &name) {
  return GetTableEntry(txn, NameToOid(txn, name));
}

table_oid_t TableCatalogView::NameToOid(transaction::TransactionContext *txn, const std::string &name) {
  // TODO(yangjuns): repeated work if the row can be used later. Maybe cache can solve it.
  std::vector<type::TransientValue> search_vec;
  search_vec.emplace_back(type::TransientValueFactory::GetNull(type::TypeId::BIGINT));
  search_vec.emplace_back(type::TransientValueFactory::GetNull(type::TypeId::INTEGER));
  search_vec.emplace_back(type::TransientValueFactory::GetVarChar(name));

  std::vector<type::TransientValue> row = pg_class_->FindRow(txn, search_vec);
  if (row.empty()) {
    // table does not exist
    return table_oid_t(NULL_OID);
  }
  auto result = table_oid_t(type::TransientValuePeeker::PeekInteger(row[1]));
  return result;
}

SqlTableHelper *TableCatalogView::CreateTable(transaction::TransactionContext *txn, const Schema &schema,
                                              const std::string &name) {
  std::vector<type::TransientValue> row;
  // TODO(yangjuns): error handling
  // Create SqlTable
  auto table = new SqlTableHelper(table_oid_t(catalog_->GetNextOid()));
  auto cols = schema.GetColumns();
  for (auto &col : cols) {
    table->DefineColumn(col.GetName(), col.GetType(), col.GetNullable(), col.GetOid());
  }
  table->Create();
  // Add to pg_class
  row.emplace_back(type::TransientValueFactory::GetBigInt(reinterpret_cast<int64_t>(table)));
  row.emplace_back(type::TransientValueFactory::GetInteger(!table->Oid()));
  row.emplace_back(type::TransientValueFactory::GetVarChar(name));
  row.emplace_back(type::TransientValueFactory::GetInteger(!nsp_oid_));
  row.emplace_back(type::TransientValueFactory::GetInteger(
      !catalog_->GetTablespaceHandle().GetTablespaceEntry(txn, "pg_default")->GetOid()));
  pg_class_->InsertRow(txn, row);
  return table;
}

SqlTableHelper *TableCatalogView::GetTable(transaction::TransactionContext *txn, table_oid_t oid) {
  // TODO(yangjuns): error handling
  // get the namespace_oid of the table to check if it's a table under current namespace
  std::vector<type::TransientValue> search_vec;
  search_vec.emplace_back(type::TransientValueFactory::GetNull(type::TypeId::BIGINT));
  search_vec.emplace_back(type::TransientValueFactory::GetInteger(!oid));

  std::vector<type::TransientValue> row = pg_class_->FindRow(txn, search_vec);
  if (row.empty()) {
    return nullptr;
  }
  namespace_oid_t nsp_oid = namespace_oid_t(type::TransientValuePeeker::PeekInteger(row[3]));
  if (nsp_oid != nsp_oid_) return nullptr;
  auto ptr = reinterpret_cast<SqlTableHelper *>(type::TransientValuePeeker::PeekBigInt(row[0]));
  return ptr;
}

SqlTableHelper *TableCatalogView::GetTable(transaction::TransactionContext *txn, const std::string &name) {
  return GetTable(txn, NameToOid(txn, name));
}

std::string_view TableCatalogEntry::GetSchemaname() {
  return type::TransientValuePeeker::PeekVarChar(this->GetColInRow(0));
}

std::string_view TableCatalogEntry::GetTablename() {
  return type::TransientValuePeeker::PeekVarChar(this->GetColInRow(1));
}

}  // namespace terrier::catalog
