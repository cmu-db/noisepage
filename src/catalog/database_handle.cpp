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
DatabaseHandle::DatabaseHandle(Catalog *catalog, std::shared_ptr<catalog::SqlTableRW> pg_database)
    : catalog_(catalog), pg_database_rw_(std::move(pg_database)) {}

NamespaceHandle DatabaseHandle::GetNamespaceHandle(transaction::TransactionContext *txn, db_oid_t oid) {
  std::string pg_namespace("pg_namespace");
  return NamespaceHandle(catalog_, oid, catalog_->GetDatabaseCatalog(oid, pg_namespace));
}

TypeHandle DatabaseHandle::GetTypeHandle(transaction::TransactionContext *txn, db_oid_t oid) {
  return TypeHandle(catalog_, catalog_->GetDatabaseCatalog(oid, "pg_type"));
}

std::shared_ptr<DatabaseHandle::DatabaseEntry> DatabaseHandle::GetDatabaseEntry(transaction::TransactionContext *txn,
                                                                                db_oid_t oid) {
  auto pg_database_rw = catalog_->GetDatabaseCatalog(oid, "pg_database");

  std::vector<type::Value> search_vec;
  search_vec.push_back(type::ValueFactory::GetIntegerValue(!oid));
  auto row_vec = pg_database_rw->FindRow(txn, search_vec);
  return std::make_shared<DatabaseEntry>(oid, row_vec);
}

std::shared_ptr<DatabaseHandle::DatabaseEntry> DatabaseHandle::GetOldDatabaseEntry(transaction::TransactionContext *txn,
                                                                                   const char *db_name) {
  // we don't need to do this lookup. pg_database is global
  // auto pg_database_rw = catalog_->GetDatabaseCatalog(DEFAULT_DATABASE_OID, "pg_database");

  // just use pg_database_

  std::vector<type::Value> search_vec;
  search_vec.push_back(type::ValueFactory::GetNullValue(type::TypeId::INTEGER));
  search_vec.push_back(type::ValueFactory::GetVarcharValue(db_name));
  auto row_vec = pg_database_rw_->FindRow(txn, search_vec);
  if (row_vec.empty()) {
    return nullptr;
  }
  // specifying the oid is redundant. Eliminate?
  db_oid_t oid(row_vec[0].GetIntValue());
  return std::make_shared<DatabaseEntry>(oid, row_vec);
}

std::shared_ptr<DatabaseHandle::DatabaseEntry> DatabaseHandle::GetDatabaseEntry(transaction::TransactionContext *txn,
                                                                                const char *db_name) {
  std::vector<type::Value> search_vec;
  search_vec.push_back(type::ValueFactory::GetNullValue(type::TypeId::INTEGER));
  search_vec.push_back(type::ValueFactory::GetVarcharValue(db_name));
  auto proj_col_p = pg_database_rw_->FindRowProjCol(txn, search_vec);
  if (proj_col_p == nullptr) {
    return nullptr;
  }

  return std::make_shared<DatabaseEntry>(std::make_shared<DatabaseHandle>(*this), proj_col_p);
}

DatabaseHandle::DatabaseEntry::DatabaseEntry(const std::shared_ptr<DatabaseHandle> &handle_p,
                                             terrier::storage::ProjectedColumns *proj_col_p)
    : proj_col_p_(proj_col_p), handle_p_(handle_p) {
  auto layout = handle_p_->pg_database_rw_->GetLayout();
  storage::ProjectedColumns::RowView row_view = proj_col_p->InterpretAsRow(layout, 0);
  entry_ = handle_p->pg_database_rw_->ColToValueVec(row_view);
  // recover the oid
  oid_ = db_oid_t(entry_[0].GetIntValue());
}

bool DatabaseHandle::DatabaseEntry::Delete(transaction::TransactionContext *txn) {
  // get the tuple slot.
  auto tuple_slot_p = proj_col_p_->TupleSlots();
  // call delete
  bool status = handle_p_->pg_database_rw_->GetSqlTable()->Delete(txn, *tuple_slot_p);
  return status;
}

}  // namespace terrier::catalog
