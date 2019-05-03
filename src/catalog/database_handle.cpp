#include "catalog/database_handle.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "loggers/main_logger.h"
#include "storage/block_layout.h"
#include "storage/projected_columns.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"

namespace terrier::catalog {

const std::vector<SchemaCol> DatabaseHandle::schema_cols_ = {{0, true, "oid", type::TypeId::INTEGER},
                                                             {1, true, "datname", type::TypeId::VARCHAR},
                                                             {2, false, "datdba", type::TypeId::INTEGER},
                                                             {3, false, "encoding", type::TypeId::INTEGER},
                                                             {4, false, "datcollate", type::TypeId::VARCHAR},
                                                             {5, false, "datctype", type::TypeId::VARCHAR},
                                                             {6, false, "datistemplate", type::TypeId::BOOLEAN},
                                                             {7, false, "datallowconn", type::TypeId::BOOLEAN},
                                                             {8, false, "datconnlimit", type::TypeId::INTEGER},
                                                             {9, false, "datlasysiod", type::TypeId::INTEGER},
                                                             {10, false, "datfrozenxid", type::TypeId::INTEGER},
                                                             {11, false, "datminmxid", type::TypeId::INTEGER},
                                                             {12, false, "dattablespace", type::TypeId::INTEGER},
                                                             {13, false, "datacl", type::TypeId::INTEGER}};

/**
 * Handle methods
 */

DatabaseHandle::DatabaseHandle(Catalog *catalog, SqlTableHelper *pg_database)
    : catalog_(catalog), pg_database_rw_(pg_database) {}

ClassHandle DatabaseHandle::GetClassHandle(transaction::TransactionContext *txn, db_oid_t oid) {
  std::string pg_class("pg_class");
  return ClassHandle(catalog_, catalog_->GetCatalogTable(oid, pg_class));
}

NamespaceHandle DatabaseHandle::GetNamespaceHandle(transaction::TransactionContext *txn, db_oid_t oid) {
  std::string pg_namespace("pg_namespace");
  return NamespaceHandle(catalog_, oid, catalog_->GetCatalogTable(oid, pg_namespace));
}

TypeHandle DatabaseHandle::GetTypeHandle(transaction::TransactionContext *txn, db_oid_t oid) {
  return TypeHandle(catalog_, catalog_->GetCatalogTable(oid, "pg_type"));
}

AttributeHandle DatabaseHandle::GetAttributeHandle(transaction::TransactionContext *txn, db_oid_t oid) {
  return AttributeHandle(catalog_->GetCatalogTable(oid, "pg_attribute"));
}

AttrDefHandle DatabaseHandle::GetAttrDefHandle(transaction::TransactionContext *txn, db_oid_t oid) {
  return AttrDefHandle(catalog_->GetCatalogTable(oid, "pg_attrdef"));
}

IndexHandle DatabaseHandle::GetIndexHandle(terrier::transaction::TransactionContext *txn,
                                           terrier::catalog::db_oid_t oid) {
  return IndexHandle(catalog_, catalog_->GetCatalogTable(oid, "pg_index"));
}

std::shared_ptr<DatabaseEntry> DatabaseHandle::GetDatabaseEntry(transaction::TransactionContext *txn, db_oid_t oid) {
  auto pg_database_rw = catalog_->GetCatalogTable(oid, "pg_database");

  std::vector<type::TransientValue> search_vec;
  search_vec.push_back(type::TransientValueFactory::GetInteger(!oid));
  auto row_vec = pg_database_rw->FindRow(txn, search_vec);
  if (row_vec.empty()) {
    return nullptr;
  }
  return std::make_shared<DatabaseEntry>(oid, pg_database_rw, std::move(row_vec));
}

std::shared_ptr<DatabaseEntry> DatabaseHandle::GetDatabaseEntry(transaction::TransactionContext *txn,
                                                                const std::string &db_name) {
  std::vector<type::TransientValue> search_vec;
  search_vec.push_back(type::TransientValueFactory::GetNull(type::TypeId::INTEGER));
  search_vec.push_back(type::TransientValueFactory::GetVarChar(db_name));
  auto row_vec = pg_database_rw_->FindRow(txn, search_vec);
  if (row_vec.empty()) {
    return nullptr;
  }
  // specifying the oid is redundant. Eliminate?
  db_oid_t oid(type::TransientValuePeeker::PeekInteger(row_vec[0]));
  return std::make_shared<DatabaseEntry>(oid, pg_database_rw_, std::move(row_vec));
}

bool DatabaseHandle::DeleteEntry(transaction::TransactionContext *txn, const std::shared_ptr<DatabaseEntry> &entry) {
  std::vector<type::TransientValue> search_vec;
  // get the oid of this row
  search_vec.emplace_back(type::TransientValueFactory::GetCopy(entry->GetColumn(0)));

  // lookup and get back the projected column. Recover the tuple_slot
  auto proj_col_p = pg_database_rw_->FindRowProjCol(txn, search_vec);
  auto tuple_slot_p = proj_col_p->TupleSlots();
  // delete
  bool status = pg_database_rw_->GetSqlTable()->Delete(txn, *tuple_slot_p);
  delete[] reinterpret_cast<byte *>(proj_col_p);
  return status;
}

}  // namespace terrier::catalog
