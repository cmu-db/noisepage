#include <iostream>
#include <memory>
#include <string>
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

std::shared_ptr<TableHandle::TableEntry> TableHandle::GetTableEntry(transaction::TransactionContext *txn,
                                                                    table_oid_t oid) {
  // get the namespace_oid of the table to check if it's a table under current namespace
  namespace_oid_t nsp_oid(0);
  std::vector<type::Value> search_vec;
  search_vec.emplace_back(type::ValueFactory::GetNullValue(type::TypeId::BIGINT));
  search_vec.emplace_back(type::ValueFactory::GetIntegerValue(!oid));

  std::vector<type::Value> row = pg_class_->FindRow(txn, search_vec);
  nsp_oid = namespace_oid_t(row[3].GetIntValue());
  if (nsp_oid != nsp_oid_) return nullptr;
  return std::make_shared<TableEntry>(oid, row, txn, pg_class_, pg_namespace_, pg_tablespace_);
}

std::shared_ptr<TableHandle::TableEntry> TableHandle::GetTableEntry(transaction::TransactionContext *txn,
                                                                    const std::string &name) {
  return GetTableEntry(txn, NameToOid(txn, name));
}

table_oid_t TableHandle::NameToOid(transaction::TransactionContext *txn, const std::string &name) {
  // TODO(yangjuns): repeated work if the row can be used later. Maybe cache can solve it.
  std::vector<type::Value> search_vec;
  search_vec.emplace_back(type::ValueFactory::GetNullValue(type::TypeId::BIGINT));
  search_vec.emplace_back(type::ValueFactory::GetNullValue(type::TypeId::INTEGER));
  search_vec.emplace_back(type::ValueFactory::GetVarcharValue(name.c_str()));

  std::vector<type::Value> row = pg_class_->FindRow(txn, search_vec);
  auto result = table_oid_t(row[1].GetIntValue());
  return result;
}

AttributeHandle TableHandle::GetAttributeHandle(transaction::TransactionContext *txn, const std::string &table_name) {
  // get the table pointer
  SqlTableRW *table_ptr = GetTable(txn, table_name);
  return AttributeHandle(catalog_, table_ptr, catalog_->GetDatabaseCatalog(db_oid_, "pg_attribute"));
}

SqlTableRW *TableHandle::CreateTable(transaction::TransactionContext *txn, const Schema &schema,
                                     const std::string &name) {
  // TODO(yangjuns): error handling
  // Create SqlTable
  auto table = new SqlTableRW(table_oid_t(catalog_->GetNextOid()));
  auto cols = schema.GetColumns();
  for (auto &col : cols) {
    table->DefineColumn(col.GetName(), col.GetType(), col.GetNullable(), col.GetOid());
  }
  table->Create();
  // Add to pg_class
  pg_class_->StartRow();
  pg_class_->SetColInRow(0, type::ValueFactory::GetBigIntValue(reinterpret_cast<int64_t>(table)));
  pg_class_->SetColInRow(1, type::ValueFactory::GetIntegerValue(!table->Oid()));
  pg_class_->SetColInRow(2, type::ValueFactory::GetVarcharValue(name.c_str()));
  pg_class_->SetColInRow(3, type::ValueFactory::GetIntegerValue(!nsp_oid_));
  pg_class_->SetColInRow(
      4, type::ValueFactory::GetIntegerValue(
             !catalog_->GetTablespaceHandle().GetTablespaceEntry(txn, "pg_default")->GetTablespaceOid()));
  pg_class_->EndRowAndInsert(txn);
  return table;
}

SqlTableRW *TableHandle::GetTable(transaction::TransactionContext *txn, table_oid_t oid) {
  // TODO(yangjuns): error handling
  // get the namespace_oid of the table to check if it's a table under current namespace
  std::vector<type::Value> search_vec;
  search_vec.emplace_back(type::ValueFactory::GetNullValue(type::TypeId::BIGINT));
  search_vec.emplace_back(type::ValueFactory::GetIntegerValue(!oid));

  std::vector<type::Value> row = pg_class_->FindRow(txn, search_vec);
  namespace_oid_t nsp_oid = namespace_oid_t(row[3].GetIntValue());
  if (nsp_oid != nsp_oid_) return nullptr;
  auto ptr = reinterpret_cast<SqlTableRW *>(row[0].GetBigIntValue());
  return ptr;
}

SqlTableRW *TableHandle::GetTable(transaction::TransactionContext *txn, const std::string &name) {
  return GetTable(txn, NameToOid(txn, name));
}

}  // namespace terrier::catalog
