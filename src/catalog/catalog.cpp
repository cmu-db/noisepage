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
  oid_counter++;
  // oid
  cols_.emplace_back("oid", type::TypeId::INTEGER, false, col_oid_t(oid_counter));
  oid_counter++;
  // datname
  cols_.emplace_back("datname", type::TypeId::VARCHAR, false, col_oid_t(oid_counter));
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
  //  storage::RecordBufferSegmentPool buffer_pool{100, 100};

  // disable logging
  //  transaction::TransactionManager txn_manager(&buffer_pool, false, nullptr);
  //  auto txn_context = txn_manager.BeginTransaction();

  // insert a a row

  // create a projected row

  // need a block layout
  //  storage::BlockLayout layout({4, 20});
  //  std::vector<storage::col_id_t> col_ids = {storage::col_id_t(0),storage::col_id_t(1)};
  //  storage::ProjectedRowInitializer initializer(layout, col_ids);
  //
  //  byte * row_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  //  static_cast<uint32_t*>(row_buffer)

  // pg_database_->Insert(txn_context, )
}
}  // namespace terrier::catalog