#include <memory>
#include <unordered_map>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/database_handle.h"
#include "loggers/catalog_logger.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_manager.h"

namespace terrier::catalog {

std::shared_ptr<Catalog> terrier_catalog;

Catalog::Catalog(transaction::TransactionManager *txn_manager) : txn_manager_(txn_manager) {
  CATALOG_LOG_INFO("Creating catalog ...");
  Bootstrap();
}

DatabaseHandle Catalog::GetDatabaseHandle(db_oid_t db_oid) { return DatabaseHandle(this, db_oid, pg_database_); }

std::shared_ptr<storage::SqlTable> Catalog::GetDatabaseCatalog(db_oid_t db_oid, table_oid_t table_oid) {
  return map_[db_oid][table_oid];
}

void Catalog::Bootstrap() {
  CATALOG_LOG_INFO("Bootstrapping global catalogs ...");
  table_oid_t table_oid(0);
  col_oid_t col_oid(0);
  transaction::TransactionContext *txn = txn_manager_->BeginTransaction();
  CATALOG_LOG_INFO("Creating pg_database table ...");
  CreatePGDatabase(txn, table_oid++, &col_oid);
  CATALOG_LOG_INFO("Creating pg_tablespace table ...");
  CreatePGTablespace(txn, table_oid++, &col_oid);

  BootstrapDatabase(txn, DEFAULT_DATABASE_OID);
  txn_manager_->Commit(txn, BootstrapCallback, nullptr);
  delete txn;
}

void Catalog::BootstrapDatabase(transaction::TransactionContext *txn, db_oid_t db_oid) {
  table_oid_t table_oid(DATABASE_CATALOG_TABLE_START_OID);
  col_oid_t col_oid(DATABASE_CATALOG_COL_START_OID);
  nsp_oid_t nsp_oid(0);

  // create pg_namespace
  table_oid_t pg_namespace_oid(table_oid++);
  std::vector<Schema::Column> cols;
  cols.emplace_back("oid", type::TypeId::INTEGER, false, col_oid++);
  // TODO(yangjun): we don't support VARCHAR at the moment, use INTEGER for now
  cols.emplace_back("nspname", type::TypeId::INTEGER, false, col_oid++);

  Schema schema(cols);
  std::shared_ptr<storage::SqlTable> pg_namespace =
      std::make_shared<storage::SqlTable>(&block_store_, schema, pg_namespace_oid);
  map_[db_oid][pg_namespace_oid] = pg_namespace;

  // create a catalog namespace
  // insert rows to pg_namespace
  std::vector<col_oid_t> col_ids;
  for (const auto &c : pg_namespace->GetSchema().GetColumns()) {
    col_ids.emplace_back(c.GetOid());
  }
  auto row_pair = pg_namespace->InitializerForProjectedRow(col_ids);
  nsp_oid_t catalog_nsp_oid(nsp_oid++);

  byte *row_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *insert = row_pair.first.InitializeRow(row_buffer);
  byte *first = insert->AccessForceNotNull(row_pair.second[col_ids[0]]);
  (*reinterpret_cast<uint32_t *>(first)) = !catalog_nsp_oid;
  byte *second = insert->AccessForceNotNull(row_pair.second[col_ids[1]]);
  // TODO(yangjun): we don't support VARCHAR at the moment, just use random number
  (*reinterpret_cast<uint32_t *>(second)) = 22222;

  pg_namespace->Insert(txn, *insert);
  delete[] row_buffer;
}

void Catalog::CreatePGDatabase(transaction::TransactionContext *txn, table_oid_t table_oid, col_oid_t *start_col_oid) {
  // create pg_database catalog
  table_oid_t pg_database_oid(table_oid);
  std::vector<Schema::Column> cols;
  cols.emplace_back("oid", type::TypeId::INTEGER, false, (*start_col_oid)++);
  // TODO(yangjun): we don't support VARCHAR at the moment, use INTEGER for now
  cols.emplace_back("datname", type::TypeId::INTEGER, false, (*start_col_oid)++);

  Schema schema(cols);
  pg_database_ = std::make_shared<storage::SqlTable>(&block_store_, schema, pg_database_oid);

  CATALOG_LOG_INFO("Creating terrier database ...");
  // insert rows to pg_database
  std::vector<col_oid_t> col_ids;
  for (const auto &c : pg_database_->GetSchema().GetColumns()) {
    col_ids.emplace_back(c.GetOid());
  }
  auto row_pair = pg_database_->InitializerForProjectedRow(col_ids);

  // create default database terrier
  db_oid_t terrier_oid = DEFAULT_DATABASE_OID;

  byte *row_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *insert = row_pair.first.InitializeRow(row_buffer);
  // hard code the first column's value (terrier's db_oid_t) to be 0
  byte *first = insert->AccessForceNotNull(row_pair.second[col_ids[0]]);
  (*reinterpret_cast<uint32_t *>(first)) = !terrier_oid;
  // hard code datname column's value to be "terrier"
  byte *second = insert->AccessForceNotNull(row_pair.second[col_ids[1]]);
  // TODO(yangjun): we don't support VARCHAR at the moment, just use random number
  (*reinterpret_cast<uint32_t *>(second)) = 12345;
  pg_database_->Insert(txn, *insert);
  delete[] row_buffer;

  map_[terrier_oid] = std::unordered_map<table_oid_t, std::shared_ptr<storage::SqlTable>>();
}

void Catalog::CreatePGTablespace(transaction::TransactionContext *txn, table_oid_t table_oid,
                                 col_oid_t *start_col_oid) {
  // create pg_tablespace catalog
  std::vector<Schema::Column> cols;
  // TODO(yangjun): we don't support VARCHAR at the moment, use INTEGER for now
  cols.emplace_back("spcname", type::TypeId::INTEGER, false, (*start_col_oid)++);

  Schema schema(cols);
  pg_tablespace_ = std::make_shared<storage::SqlTable>(&block_store_, schema, table_oid);

  // insert rows to pg_tablespace
  std::vector<col_oid_t> col_ids;
  for (const auto &c : pg_tablespace_->GetSchema().GetColumns()) {
    col_ids.emplace_back(c.GetOid());
  }
  auto row_pair = pg_tablespace_->InitializerForProjectedRow(col_ids);

  byte *row_buffer, *first;
  storage::ProjectedRow *insert;

  CATALOG_LOG_INFO("Inserting pg_global to pg_tablespace...");
  // insert pg_global
  row_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  insert = row_pair.first.InitializeRow(row_buffer);
  // hard code the first column's value
  first = insert->AccessForceNotNull(row_pair.second[col_ids[0]]);
  // TODO(yangjun): we don't support VARCHAR at the moment, the value should be "pg_global", just use random number
  (*reinterpret_cast<uint32_t *>(first)) = 11111;
  pg_tablespace_->Insert(txn, *insert);
  delete[] row_buffer;

  CATALOG_LOG_INFO("Inserting pg_default to pg_tablespace...");
  // insert pg_default
  row_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  insert = row_pair.first.InitializeRow(row_buffer);
  // hard code the first column's value
  first = insert->AccessForceNotNull(row_pair.second[col_ids[0]]);
  // TODO(yangjun): we don't support VARCHAR at the moment, the value should be "pg_default", just use random number
  (*reinterpret_cast<uint32_t *>(first)) = 22222;
  pg_tablespace_->Insert(txn, *insert);
  delete[] row_buffer;
}

}  // namespace terrier::catalog
