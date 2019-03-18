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

/**
 * Handle methods
 */

DatabaseHandle::DatabaseHandle(Catalog *catalog, std::shared_ptr<catalog::SqlTableRW> pg_database)
    : catalog_(catalog), pg_database_rw_(std::move(pg_database)) {}

ClassHandle DatabaseHandle::GetClassHandle(transaction::TransactionContext *txn, db_oid_t oid) {
  std::string pg_class("pg_class");
  return ClassHandle(catalog_, catalog_->GetDatabaseCatalog(oid, pg_class));
}

NamespaceHandle DatabaseHandle::GetNamespaceHandle(transaction::TransactionContext *txn, db_oid_t oid) {
  std::string pg_namespace("pg_namespace");
  return NamespaceHandle(catalog_, oid, catalog_->GetDatabaseCatalog(oid, pg_namespace));
}

TypeHandle DatabaseHandle::GetTypeHandle(transaction::TransactionContext *txn, db_oid_t oid) {
  return TypeHandle(catalog_, catalog_->GetDatabaseCatalog(oid, "pg_type"));
}

AttributeHandle DatabaseHandle::GetAttributeHandle(transaction::TransactionContext *txn, db_oid_t oid) {
  return AttributeHandle(catalog_, catalog_->GetDatabaseCatalog(oid, "pg_attribute"));
}

std::shared_ptr<DatabaseHandle::DatabaseEntry> DatabaseHandle::GetDatabaseEntry(transaction::TransactionContext *txn,
                                                                                db_oid_t oid) {
  auto pg_database_rw = catalog_->GetDatabaseCatalog(oid, "pg_database");

  std::vector<type::Value> search_vec;
  search_vec.push_back(type::ValueFactory::GetIntegerValue(!oid));
  auto row_vec = pg_database_rw->FindRow(txn, search_vec);
  return std::make_shared<DatabaseEntry>(oid, row_vec);
}

std::shared_ptr<DatabaseHandle::DatabaseEntry> DatabaseHandle::GetDatabaseEntry(transaction::TransactionContext *txn,
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

bool DatabaseHandle::DeleteEntry(transaction::TransactionContext *txn, const std::shared_ptr<DatabaseEntry> &entry) {
  std::vector<type::Value> search_vec;
  // get the oid of this row
  search_vec.emplace_back(entry->GetColumn(0));

  // lookup and get back the projected column. Recover the tuple_slot
  auto proj_col_p = pg_database_rw_->FindRowProjCol(txn, search_vec);
  auto tuple_slot_p = proj_col_p->TupleSlots();
  // delete
  bool status = pg_database_rw_->GetSqlTable()->Delete(txn, *tuple_slot_p);
  delete[] reinterpret_cast<byte *>(proj_col_p);
  return status;
}

const std::vector<SchemaCol> DatabaseHandle::schema_cols_ = {{0, "oid", type::TypeId::INTEGER},
                                                             {1, "datname", type::TypeId::VARCHAR}};

const std::vector<SchemaCol> DatabaseHandle::unused_schema_cols_ = {
    {2, "datdba", type::TypeId::INTEGER},        {3, "encoding", type::TypeId::INTEGER},
    {4, "datcollate", type::TypeId::VARCHAR},    {5, "datctype", type::TypeId::VARCHAR},
    {6, "datistemplate", type::TypeId::BOOLEAN}, {7, "datallowconn", type::TypeId::BOOLEAN},
    {8, "datconnlimit", type::TypeId::INTEGER}};

}  // namespace terrier::catalog
