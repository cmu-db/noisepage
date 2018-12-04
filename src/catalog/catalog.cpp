#include "catalog/catalog.h"
#include "catalog/database_handle.h"
#include "loggers/main_logger.h"
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
Catalog::Catalog() {
  LOG_INFO("Initializing catalog");
  LOG_INFO("Creating pg_database ..");
  // need to create schema
  // pg_database has {oid, datname}
  table_oid_t pg_database_oid(oid_counter);
  LOG_INFO("pg_database_oid = {}", oid_counter);
  oid_counter++;
  // oid
  cols_.emplace_back("oid", type::TypeId::INTEGER, false, col_oid_t(oid_counter));
  LOG_INFO("first col_oid = {}", oid_counter);
  oid_counter++;
  // datname
  cols_.emplace_back("datname", type::TypeId::VARCHAR, false, col_oid_t(oid_counter));
  LOG_INFO("second col_oid = {}", oid_counter);
  oid_counter++;

  // TODO: need to put this columns into pg_attribute for each database
  Schema schema(cols_);
  pg_database_ = new storage::SqlTable(&block_store_, schema, pg_database_oid);

  Bootstrap();
}

void Catalog::Bootstrap() {
  // need to create default database terrier

  // need to create a transaction

  // need to create record buffer segmentpool
  storage::RecordBufferSegmentPool buffer_pool{100, 100};

  // disable logging
  transaction::TransactionManager txn_manager(&buffer_pool, false, nullptr);
  auto txn_context = txn_manager.BeginTransaction();

  // insert a a row

  // create a projected row

  // need a block layout
  auto layout_pair = storage::StorageUtil::BlockLayoutFromSchema(Schema(cols_));

  // get col_id_t for the first column
  col_oid_t first_col_oid = cols_[0].GetOid();
  storage::col_id_t first_col_id_t = layout_pair.second[first_col_oid];
  // get col_id_t for the second column
  col_oid_t second_col_oid = cols_[1].GetOid();
  storage::col_id_t second_col_id_t = layout_pair.second[second_col_oid];
  LOG_INFO("First: col_oid_t = {} col_id_t = {}", int(!first_col_oid), int(!first_col_id_t))

  std::vector<storage::col_id_t> col_ids = {first_col_id_t, second_col_id_t};
  storage::ProjectedRowInitializer initializer(layout_pair.first, col_ids);

  byte *row_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  storage::ProjectedRow *insert = initializer.InitializeRow(row_buffer);

  //fill in the first attribute
  byte *first = insert->AccessForceNotNull(0);
  (*reinterpret_cast<uint32_t *>(first)) = oid_counter++;

  // fill in the second attribute
  byte *second = insert->AccessForceNotNull(1);
  strcpy(reinterpret_cast<char *>(second), "terrier");

  pg_database_->Insert(txn_context, *insert);

  LOG_INFO("inserted a row in pg_database");
}
}  // namespace terrier::catalog