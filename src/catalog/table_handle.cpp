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
  // TODO(yangjuns): error handling
  // get the namespace_oid of the table to check if it's a table under current namespace
  namespace_oid_t nsp_oid(0);
  storage::ProjectedRow *row = pg_class_->FindRow(txn, 0, !oid);
  nsp_oid = namespace_oid_t(pg_class_->GetIntColInRow(2, row));
  if (nsp_oid != nsp_oid_) return nullptr;
  return std::make_shared<TableEntry>(oid, row, txn, pg_class_, pg_namespace_, pg_tablespace_);
}

std::shared_ptr<TableHandle::TableEntry> TableHandle::GetTableEntry(transaction::TransactionContext *txn,
                                                                    const std::string &name) {
  return GetTableEntry(txn, NameToOid(txn, name));
}

table_oid_t TableHandle::NameToOid(transaction::TransactionContext *txn, const std::string &name) {
  // TODO(yangjuns): error handling
  // TODO(yangjuns): repeated work if the user wants an entry later. Maybe cache can solve it.
  auto row = pg_class_->FindRow(txn, 1, name.c_str());
  auto result = table_oid_t(pg_class_->GetIntColInRow(0, row));
  delete[] reinterpret_cast<byte *>(row);
  return result;
}

void TableHandle::CreateTable(transaction::TransactionContext *txn, Schema &schema, const std::string &name) {
  // TODO(yangjuns): error handling
  // Create SqlTable
  auto table = std::make_shared<catalog::SqlTableRW>(table_oid_t(catalog_->GetNextOid()));
  auto cols = schema.GetColumns();
  for (auto &col : cols) {
    table->DefineColumn(col.GetName(), col.GetType(), col.GetNullable(), col.GetOid());
  }
  table->Create();
  // Add to pg_class
  pg_class_->StartRow();
  pg_class_->SetIntColInRow(0, !table->Oid());
  pg_class_->SetVarcharColInRow(1, name.c_str());
  pg_class_->SetIntColInRow(2, !nsp_oid_);
  pg_class_->SetIntColInRow(3,
                            !catalog_->GetTablespaceHandle().GetTablespaceEntry(txn, "pg_default")->GetTablespaceOid());
  pg_class_->EndRowAndInsert(txn);
}

}  // namespace terrier::catalog
