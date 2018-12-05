#include <memory>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/database_handle.h"
#include "loggers/catalog_logger.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_manager.h"

namespace terrier::catalog {

std::shared_ptr<Catalog> terrier_catalog;
std::atomic<uint32_t> oid_counter(0);

/* Initialization of catalog, including:
 * 1) create database_catalog.
 * create terrier database, create catalog tables, add them into
 * terrier database, insert columns into pg_attribute
 * 2) create necessary indexes, insert into pg_index
 * 3) insert terrier into pg_database, catalog tables into pg_table
 */
Catalog::Catalog(transaction::TransactionManager *txn_manager) : txn_manager_(txn_manager) {
  CATALOG_LOG_INFO("Initializing catalog ...");
  CATALOG_LOG_INFO("Creating pg_database table ..,");
  // need to create schema
  // pg_database has {oid, datname}
  table_oid_t pg_database_oid(oid_counter);
  CATALOG_LOG_INFO("pg_database_oid = {}", oid_counter);
  oid_counter++;

  // Columns
  std::vector<Schema::Column> cols;
  // oid
  cols.emplace_back("oid", type::TypeId::INTEGER, false, col_oid_t(oid_counter));
  CATALOG_LOG_INFO("first col_oid = {}", oid_counter);
  oid_counter++;
  // datname
  // TODO(yangjun): we don't support VARCHAR at the moment
  cols.emplace_back("datname", type::TypeId::INTEGER, false, col_oid_t(oid_counter));
  CATALOG_LOG_INFO("second col_oid = {}", oid_counter);
  oid_counter++;

  // TODO(yangjun): need to put this columns into pg_attribute for each database
  Schema schema(cols);
  pg_database_ = std::make_shared<storage::SqlTable>(&block_store_, schema, pg_database_oid);

  Bootstrap();
}

void EmptyCallback(void * /*unused*/) {}

void Catalog::Bootstrap() {
  std::vector<col_oid_t> cols;
  printf("insert oid: ");
  for (const auto &c : pg_database_->GetSchema().GetColumns()) {
    cols.emplace_back(c.GetOid());
    printf("%d ", int(!c.GetOid()));
  }
  auto row_pair = pg_database_->InitializerForProjectedRow(cols);

  // need to create default database terrier

  // need to create a transaction

  // need to create record buffer segmentpool
  auto txn_context = txn_manager_->BeginTransaction();

  byte *row_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *insert = row_pair.first.InitializeRow(row_buffer);

  // fill in the first attribute
  byte *first = insert->AccessForceNotNull(row_pair.second[cols[0]]);
  (*reinterpret_cast<uint32_t *>(first)) = 0;

  // fill in the second attribute
  byte *second = insert->AccessForceNotNull(row_pair.second[cols[1]]);
  // TODO(yangjun): we don't support VARCHAR at the moment
  (*reinterpret_cast<uint32_t *>(second)) = 15721;

  auto tp_slot = pg_database_->Insert(txn_context, *insert);
  printf("insert tuple slot: offset %d, block %p\n", tp_slot.GetOffset(), tp_slot.GetBlock());

  CATALOG_LOG_INFO("inserted a row in pg_database");
  txn_manager_->Commit(txn_context, EmptyCallback, nullptr);
  delete txn_context;
  delete[] row_buffer;
}
}  // namespace terrier::catalog
