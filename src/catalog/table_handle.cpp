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
                                                                    const std::string &name) {
  // TODO(yangjuns): if the table is not under the namespace then we should not provide the table
  return std::make_shared<TableEntry>(name, txn, pg_class_, pg_namespace_, pg_tablespace_);
}

void TableHandle::CreateTable(transaction::TransactionContext *txn, Schema &schema, const std::string &name) {
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
}

}  // namespace terrier::catalog
