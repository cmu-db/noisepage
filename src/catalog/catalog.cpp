#include <memory>
#include <vector>
#include <unordered_map>

#include "catalog/catalog.h"
#include "catalog/database_handle.h"
#include "loggers/catalog_logger.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_manager.h"

namespace terrier::catalog {

std::shared_ptr<Catalog> terrier_catalog;
std::atomic<uint32_t> oid_counter(1);
std::atomic<uint32_t> col_oid_counter(5001);

Catalog::Catalog(transaction::TransactionManager *txn_manager) : txn_manager_(txn_manager) {
  CATALOG_LOG_INFO("Initializing catalog ...");
  CATALOG_LOG_INFO("Creating pg_database table ...");
  // create pg_database catalog
  table_oid_t pg_database_oid(oid_counter++);
  std::vector<Schema::Column> cols;
  cols.emplace_back("oid", type::TypeId::INTEGER, false, col_oid_t(col_oid_counter++));
  // TODO(yangjun): we don't support VARCHAR at the moment, use INTEGER for now
  cols.emplace_back("datname", type::TypeId::INTEGER, false, col_oid_t(col_oid_counter++));

  Schema schema(cols);
  pg_database_ = std::make_shared<storage::SqlTable>(&block_store_, schema, pg_database_oid);

  Bootstrap();
}

void Catalog::Bootstrap() {
  // insert rows to pg_database
  std::vector<col_oid_t> cols;
  for (const auto &c : pg_database_->GetSchema().GetColumns()) {
    cols.emplace_back(c.GetOid());
  }
  auto row_pair = pg_database_->InitializerForProjectedRow(cols);

  // create default database terrier
  db_oid_t terrier_oid = db_oid_t(828);
  auto txn_context = txn_manager_->BeginTransaction();

  byte *row_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *insert = row_pair.first.InitializeRow(row_buffer);
  // hard code the first column's value (terrier's db_oid_t) to be 0
  byte *first = insert->AccessForceNotNull(row_pair.second[cols[0]]);
  (*reinterpret_cast<uint32_t *>(first)) = !terrier_oid;
  // hard code datname column's value to be "terrier"
  byte *second = insert->AccessForceNotNull(row_pair.second[cols[1]]);
  // TODO(yangjun): we don't support VARCHAR at the moment, just use random number
  (*reinterpret_cast<uint32_t *>(second)) = 12345;
  pg_database_->Insert(txn_context, *insert);
  delete[] row_buffer;

  map_[terrier_oid] = std::unordered_map<table_oid_t, std::shared_ptr<storage::SqlTable>>();
  BootstrapDatabase(txn_context, terrier_oid);
  txn_manager_->Commit(txn_context, BootstrapCallback, nullptr);
  delete txn_context;

  // done with bootstrapping, set oid_counter ready for user usage.
  oid_counter = 10000;
}

void Catalog::BootstrapDatabase(transaction::TransactionContext *txn, db_oid_t db_oid) {
  // create pg_namespace
  table_oid_t pg_namespace_oid(oid_counter++);
  std::vector<Schema::Column> cols;
  cols.emplace_back("oid", type::TypeId::INTEGER, false, col_oid_t(col_oid_counter++));
  // TODO(yangjun): we don't support VARCHAR at the moment, use INTEGER for now
  cols.emplace_back("nspname", type::TypeId::INTEGER, false, col_oid_t(col_oid_counter++));

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
  nsp_oid_t catalog_nsp_oid = nsp_oid_t(oid_counter++);

  byte *row_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *insert = row_pair.first.InitializeRow(row_buffer);
  byte *first = insert->AccessForceNotNull(row_pair.second[col_ids[0]]);
  (*reinterpret_cast<uint32_t *>(first)) = !catalog_nsp_oid;
  byte *second = insert->AccessForceNotNull(row_pair.second[col_ids[1]]);
  // TODO(yangjun): we don't support VARCHAR at the moment, just use random number
  (*reinterpret_cast<uint32_t *>(second)) = 12345;

  pg_namespace->Insert(txn, *insert);
  delete[] row_buffer;
}
}  // namespace terrier::catalog
