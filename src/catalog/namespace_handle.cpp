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

void NamespaceHandle::CreateNamespace(terrier::transaction::TransactionContext *txn, const std::string &name) {
  pg_namespace_hrw_->StartRow();
  pg_namespace_hrw_->SetColInRow(0, type::ValueFactory::GetIntegerValue(catalog_->GetNextOid()));
  pg_namespace_hrw_->SetColInRow(1, type::ValueFactory::GetVarcharValue(name.c_str()));
  pg_namespace_hrw_->EndRowAndInsert(txn);
}
}  // namespace terrier::catalog
