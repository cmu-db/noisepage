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
//DatabaseHandle::DatabaseHandle(Catalog *catalog, db_oid_t oid, std::shared_ptr<storage::SqlTable> pg_database)
//    : catalog_(catalog), oid_(oid), pg_database_(std::move(pg_database)) {}
DatabaseHandle::DatabaseHandle(Catalog *catalog, db_oid_t oid, std::shared_ptr<storage::SqlTable> pg_database)
      : catalog_(catalog), oid_(oid), pg_database_(pg_database) {}

NamespaceHandle DatabaseHandle::GetNamespaceHandle() {
  std::string pg_namespace("pg_namespace");
  return NamespaceHandle(catalog_, oid_, catalog_->GetDatabaseCatalog(oid_, pg_namespace));
}

std::shared_ptr<DatabaseHandle::DatabaseEntry> DatabaseHandle::GetDatabaseEntry(transaction::TransactionContext *txn,
                                                                                db_oid_t oid) {

  std::shared_ptr<storage::SqlTable> pg_db_sqltable = catalog_->GetPGDatabase()->GetSqlTable();
  // Each database handle can only see entry with the same oid
  if (oid_ != oid) return nullptr;

  // TODO(yangjun): we can cache this
  std::vector<col_oid_t> cols;
  for (const auto &c : pg_db_sqltable->GetSchema().GetColumns()) {
    cols.emplace_back(c.GetOid());
  }
  auto row_pair = pg_db_sqltable->InitializerForProjectedRow(cols);
  auto read_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *read = row_pair.first.InitializeRow(read_buffer);
  // Find the row using sequential scan
  auto tuple_iter = pg_db_sqltable->begin();
  for (; tuple_iter != pg_db_sqltable->end(); tuple_iter++) {
    pg_db_sqltable->Select(txn, *tuple_iter, read);
    if ((*reinterpret_cast<db_oid_t *>(read->AccessForceNotNull(row_pair.second[cols[0]]))) == oid_) {
      return std::make_shared<DatabaseEntry>(catalog_->GetPGDatabase(), oid_, read, row_pair.second);
    }
  }
  delete[] read_buffer;
  return nullptr;
}

}  // namespace terrier::catalog
