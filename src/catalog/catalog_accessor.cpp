#include "catalog/catalog_accessor.h"
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog.h"

namespace terrier::catalog {
db_oid_t CatalogAccessor::GetDatabaseOid(const std::string &name) { return catalog_->GetDatabaseOid(txn_, name); }

db_oid_t CatalogAccessor::CreateDatabase(const std::string &name) { return catalog_->CreateDatabase(txn_, name, true); }

bool CatalogAccessor::DropDatabase(db_oid_t db) { return catalog_->DeleteDatabase(txn_, db); }

void CatalogAccessor::SetSearchPath(std::vector<namespace_oid_t> namespaces) { search_path_ = std::move(namespaces); }

namespace_oid_t CatalogAccessor::GetNamespaceOid(const std::string &name) { return dbc_->GetNamespaceOid(txn_, name); }

namespace_oid_t CatalogAccessor::CreateNamespace(const std::string &name) { return dbc_->CreateNamespace(txn_, name); }

bool CatalogAccessor::DropNamespace(namespace_oid_t ns) { return dbc_->DeleteNamespace(txn_, ns); }

table_oid_t CatalogAccessor::GetTableOid(const std::string &name) {
  for (auto &path : search_path_) {
    table_oid_t search_result = dbc_->GetTableOid(txn_, path, name);
    if (search_result != INVALID_TABLE_OID) return search_result;
  }
  return INVALID_TABLE_OID;
}

table_oid_t CatalogAccessor::GetTableOid(namespace_oid_t ns, const std::string &name) {
  return dbc_->GetTableOid(txn_, ns, name);
}

table_oid_t CatalogAccessor::CreateTable(namespace_oid_t ns, const std::string &name, const Schema &schema) {
  return dbc_->CreateTable(txn_, ns, name, schema);
}

bool CatalogAccessor::RenameTable(table_oid_t table, const std::string &new_table_name) {
  return dbc_->RenameTable(txn_, table, new_table_name);
}

bool CatalogAccessor::DropTable(table_oid_t table) { return dbc_->DeleteTable(txn_, table); }

bool CatalogAccessor::SetTablePointer(table_oid_t table, storage::SqlTable *table_ptr) {
  return dbc_->SetTablePointer(txn_, table, table_ptr);
}

common::ManagedPointer<storage::SqlTable> CatalogAccessor::GetTable(table_oid_t table) {
  return dbc_->GetTable(txn_, table);
}

bool CatalogAccessor::UpdateSchema(table_oid_t table, Schema *new_schema) {
  return dbc_->UpdateSchema(txn_, table, new_schema);
}

const Schema &CatalogAccessor::GetSchema(table_oid_t table) { return dbc_->GetSchema(txn_, table); }

std::vector<constraint_oid_t> CatalogAccessor::GetConstraints(table_oid_t table) {
  return dbc_->GetConstraints(txn_, table);
}

std::vector<index_oid_t> CatalogAccessor::GetIndexes(table_oid_t table) { return dbc_->GetIndexes(txn_, table); }

index_oid_t CatalogAccessor::GetIndexOid(const std::string &name) {
  for (auto &path : search_path_) {
    index_oid_t search_result = dbc_->GetIndexOid(txn_, path, name);
    if (search_result != INVALID_INDEX_OID) return search_result;
  }
  return INVALID_INDEX_OID;
}

index_oid_t CatalogAccessor::GetIndexOid(namespace_oid_t ns, const std::string &name) {
  return dbc_->GetIndexOid(txn_, ns, name);
}

std::vector<index_oid_t> CatalogAccessor::GetIndexOids(table_oid_t table) { return dbc_->GetIndexes(txn_, table); }

index_oid_t CatalogAccessor::CreateIndex(namespace_oid_t ns, table_oid_t table, const std::string &name,
                                         const IndexSchema &schema) {
  return dbc_->CreateIndex(txn_, ns, name, table, schema);
}

const IndexSchema &CatalogAccessor::GetIndexSchema(index_oid_t index) { return dbc_->GetIndexSchema(txn_, index); }

bool CatalogAccessor::DropIndex(index_oid_t index) { return dbc_->DeleteIndex(txn_, index); }

bool CatalogAccessor::SetIndexPointer(index_oid_t index, storage::index::Index *index_ptr) {
  return dbc_->SetIndexPointer(txn_, index, index_ptr);
}

common::ManagedPointer<storage::index::Index> CatalogAccessor::GetIndex(index_oid_t index) {
  return dbc_->GetIndex(txn_, index);
}

}  // namespace terrier::catalog
