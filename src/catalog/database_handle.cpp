#include "catalog/database_handle.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "loggers/main_logger.h"
#include "storage/block_layout.h"
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

std::shared_ptr<DatabaseHandle::DatabaseEntry> DatabaseHandle::GetDatabaseEntry(transaction::TransactionContext *txn,
                                                                                db_oid_t oid) {
  auto pg_database_rw = catalog_->GetDatabaseCatalog(oid, "pg_database");

  std::vector<type::Value> search_vec;
  search_vec.push_back(type::ValueFactory::GetIntegerValue(!oid));
  auto row_vec = pg_database_rw->FindRow(txn, search_vec);
  return std::make_shared<DatabaseEntry>(oid, row_vec);

#ifdef notdef
  storage::ProjectedRow *row = pg_database_rw->FindRow(txn, 0, !oid);
  if (row == nullptr) {
    return nullptr;
  }

  return std::make_shared<DatabaseEntry>(pg_database_rw, oid, row, *pg_database_rw->GetPRMap());
#endif /* notdef */
}

}  // namespace terrier::catalog
