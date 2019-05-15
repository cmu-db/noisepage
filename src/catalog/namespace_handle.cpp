#include "catalog/namespace_handle.h"
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "loggers/catalog_logger.h"
#include "storage/block_layout.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"

namespace terrier::catalog {

const std::vector<SchemaCol> NamespaceCatalogTable::schema_cols_ = {{0, true, "oid", type::TypeId::INTEGER},
                                                                    {1, true, "nspname", type::TypeId::VARCHAR},
                                                                    {2, false, "nspowner", type::TypeId::INTEGER},
                                                                    {3, false, "nspacl", type::TypeId::VARCHAR}};

std::shared_ptr<NamespaceCatalogEntry> NamespaceCatalogTable::GetNamespaceEntry(transaction::TransactionContext *txn,
                                                                                namespace_oid_t oid) {
  std::vector<type::TransientValue> search_vec, ret_row;
  search_vec.push_back(type::TransientValueFactory::GetInteger(!oid));
  ret_row = pg_namespace_hrw_->FindRow(txn, search_vec);
  if (ret_row.empty()) {
    return nullptr;
  }
  return std::make_shared<NamespaceCatalogEntry>(oid, pg_namespace_hrw_, std::move(ret_row));
}

std::shared_ptr<NamespaceCatalogEntry> NamespaceCatalogTable::GetNamespaceEntry(transaction::TransactionContext *txn,
                                                                                const std::string &name) {
  std::vector<type::TransientValue> search_vec, ret_row;
  search_vec.push_back(type::TransientValueFactory::GetNull(type::TypeId::INTEGER));
  search_vec.push_back(type::TransientValueFactory::GetVarChar(name));
  ret_row = pg_namespace_hrw_->FindRow(txn, search_vec);
  if (ret_row.empty()) {
    return nullptr;
  }
  namespace_oid_t oid(type::TransientValuePeeker::PeekInteger(ret_row[0]));
  return std::make_shared<NamespaceCatalogEntry>(oid, pg_namespace_hrw_, std::move(ret_row));
}

namespace_oid_t NamespaceCatalogTable::NameToOid(transaction::TransactionContext *txn, const std::string &name) {
  auto nse = GetNamespaceEntry(txn, name);
  if (nse == nullptr) {
    // no such namespace
    return namespace_oid_t(NULL_OID);
  }
  return namespace_oid_t(type::TransientValuePeeker::PeekInteger(nse->GetColumn(0)));
}

TableCatalogView NamespaceCatalogTable::GetTableHandle(transaction::TransactionContext *txn,
                                                       const std::string &nsp_name) {
  auto ns_oid = NameToOid(txn, nsp_name);
  return GetTableHandle(txn, ns_oid);
}

TableCatalogView NamespaceCatalogTable::GetTableHandle(transaction::TransactionContext *txn, namespace_oid_t ns_oid) {
  std::string pg_class("pg_class");
  std::string pg_namespace("pg_namespace");
  std::string pg_tablespace("pg_tablespace");
  return TableCatalogView(catalog_, ns_oid, catalog_->GetCatalogTable(db_oid_, CatalogTableType::CLASS),
                          catalog_->GetCatalogTable(db_oid_, CatalogTableType::NAMESPACE),
                          catalog_->GetCatalogTable(db_oid_, CatalogTableType::TABLESPACE));
}

void NamespaceCatalogTable::AddEntry(transaction::TransactionContext *txn, const std::string &name) {
  std::vector<type::TransientValue> row;

  row.emplace_back(type::TransientValueFactory::GetInteger(catalog_->GetNextOid()));
  row.emplace_back(type::TransientValueFactory::GetVarChar(name));
  catalog_->SetUnusedColumns(&row, NamespaceCatalogTable::schema_cols_);
  pg_namespace_hrw_->InsertRow(txn, row);
}

bool NamespaceCatalogTable::DeleteEntry(transaction::TransactionContext *txn,
                                        const std::shared_ptr<NamespaceCatalogEntry> &entry) {
  std::vector<type::TransientValue> search_vec;
  auto ns_oid_int = entry->GetIntegerColumn("oid");
  // get the oid of this row
  search_vec.emplace_back(type::TransientValueFactory::GetInteger(ns_oid_int));

  // lookup and get back the projected column. Recover the tuple_slot
  auto proj_col_p = pg_namespace_hrw_->FindRowProjCol(txn, search_vec);
  auto tuple_slot_p = proj_col_p->TupleSlots();
  // delete
  bool status = pg_namespace_hrw_->GetSqlTable()->Delete(txn, *tuple_slot_p);
  delete[] reinterpret_cast<byte *>(proj_col_p);
  return status;
}

SqlTableHelper *NamespaceCatalogTable::Create(transaction::TransactionContext *txn, Catalog *catalog, db_oid_t db_oid,
                                              const std::string &name) {
  catalog::SqlTableHelper *storage_table;

  // get an oid
  table_oid_t storage_table_oid(catalog->GetNextOid());

  // uninitialized storage
  storage_table = new catalog::SqlTableHelper(storage_table_oid);

  // columns we use
  for (auto col : NamespaceCatalogTable::schema_cols_) {
    storage_table->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }

  // now actually create, with the provided schema
  storage_table->Create();
  catalog->AddToMap(db_oid, CatalogTableType::NAMESPACE, storage_table);
  return storage_table;
}

// col_oid_t NamespaceCatalogEntry::GetOid();
std::string_view NamespaceCatalogEntry::GetNspname() { return GetVarcharColumn("nspname"); }

}  // namespace terrier::catalog
