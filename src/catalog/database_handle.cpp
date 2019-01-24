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
DatabaseHandle::DatabaseHandle(Catalog *catalog, db_oid_t oid, std::shared_ptr<storage::SqlTable> pg_database)
    : catalog_(catalog), oid_(oid), pg_database_(pg_database) {}

NamespaceHandle DatabaseHandle::GetNamespaceHandle() {
  std::string pg_namespace("pg_namespace");
  return NamespaceHandle(catalog_, oid_, catalog_->GetDatabaseCatalog(oid_, pg_namespace));
}

std::shared_ptr<DatabaseHandle::DatabaseEntry> DatabaseHandle::GetDatabaseEntry(transaction::TransactionContext *txn,
                                                                                db_oid_t oid) {
  // Each database handle can only see entry with the same oid
  if (oid_ != oid) return nullptr;

  auto pg_database_rw = catalog_->GetPGDatabase();
  storage::ProjectedRow *row = pg_database_rw->FindRow(txn, 0, !oid);
  if (row == nullptr) {
    return nullptr;
  }

  return std::make_shared<DatabaseEntry>(catalog_->GetPGDatabase(), oid_, row, *pg_database_rw->GetPRMap());
}

}  // namespace terrier::catalog
