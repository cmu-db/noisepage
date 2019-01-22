#include "catalog/catalog.h"
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "catalog/database_handle.h"
#include "catalog/tablespace_handle.h"
#include "loggers/catalog_logger.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_manager.h"

namespace terrier::catalog {

std::shared_ptr<Catalog> terrier_catalog;

Catalog::Catalog(transaction::TransactionManager *txn_manager) : txn_manager_(txn_manager), oid_(START_OID) {
  CATALOG_LOG_TRACE("Creating catalog ...");
  Bootstrap();
}

DatabaseHandle Catalog::GetDatabaseHandle(db_oid_t db_oid) { return DatabaseHandle(this, db_oid, pg_database_); }

TablespaceHandle Catalog::GetTablespaceHandle() { return TablespaceHandle(pg_tablespace_); }

std::shared_ptr<storage::SqlTable> Catalog::GetDatabaseCatalog(db_oid_t db_oid, table_oid_t table_oid) {
  return map_[db_oid][table_oid];
}

std::shared_ptr<storage::SqlTable> Catalog::GetDatabaseCatalog(db_oid_t db_oid, const std::string &table_name) {
  return GetDatabaseCatalog(db_oid, name_map_[db_oid][table_name]);
}

db_oid_t Catalog::GetNextDBOid() { return db_oid_t(oid_++); }

namespace_oid_t Catalog::GetNextNamepsaceOid() { return namespace_oid_t(oid_++); }

table_oid_t Catalog::GetNextTableOid() { return table_oid_t(oid_++); }

col_oid_t Catalog::GetNextColOid() { return col_oid_t(oid_++); }

uint32_t Catalog::GetNextOid() { return oid_++; }

void Catalog::Bootstrap() {
  CATALOG_LOG_TRACE("Bootstrapping global catalogs ...");
  transaction::TransactionContext *txn = txn_manager_->BeginTransaction();
  CATALOG_LOG_TRACE("Creating pg_database table ...");
  CreatePGDatabase(txn, GetNextTableOid());
  CATALOG_LOG_TRACE("Creating pg_tablespace table ...");
  CreatePGTablespace(txn, GetNextTableOid());

  BootstrapDatabase(txn, DEFAULT_DATABASE_OID);
  txn_manager_->Commit(txn, BootstrapCallback, nullptr);
  delete txn;
  CATALOG_LOG_TRACE("Finished bootstraping ...");
}

void Catalog::CreatePGDatabase(transaction::TransactionContext *txn, table_oid_t table_oid) {
  CATALOG_LOG_TRACE("pg_database (table) oid {}", !table_oid);
  // create pg_database catalog
  table_oid_t pg_database_oid(table_oid);
  std::vector<Schema::Column> cols;
  cols.emplace_back("oid", type::TypeId::INTEGER, false, GetNextColOid());
  // TODO(yangjun): we don't support VARCHAR at the moment, use INTEGER for now
  cols.emplace_back("datname", type::TypeId::INTEGER, false, GetNextColOid());

  Schema schema(cols);
  pg_database_ = std::make_shared<storage::SqlTable>(&block_store_, schema, pg_database_oid);

  CATALOG_LOG_TRACE("Creating terrier database ...");
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
  // hard code the first column's value (terrier's db_oid_t) to be DEFAULT_DATABASE_OID
  auto *pg_database_col_oid = reinterpret_cast<uint32_t *>(insert->AccessForceNotNull(row_pair.second[col_ids[0]]));
  *pg_database_col_oid = !terrier_oid;
  // hard code datname column's value to be "terrier"
  auto *pg_database_col_datname = reinterpret_cast<uint32_t *>(insert->AccessForceNotNull(row_pair.second[col_ids[1]]));
  // TODO(yangjun): we don't support VARCHAR at the moment, just use random number
  *pg_database_col_datname = 12345;
  pg_database_->Insert(txn, *insert);
  delete[] row_buffer;

  map_[terrier_oid] = std::unordered_map<table_oid_t, std::shared_ptr<storage::SqlTable>>();
}

void Catalog::CreatePGTablespace(transaction::TransactionContext *txn, table_oid_t table_oid) {
  CATALOG_LOG_TRACE("pg_tablespace (table) oid {}", !table_oid);
  // create pg_tablespace catalog
  std::vector<Schema::Column> cols;
  cols.emplace_back("oid", type::TypeId::INTEGER, false, GetNextColOid());
  // TODO(yangjun): we don't support VARCHAR at the moment, use INTEGER for now
  cols.emplace_back("spcname", type::TypeId::INTEGER, false, GetNextColOid());

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

  //  CATALOG_LOG_TRACE("Inserting pg_global to pg_tablespace...");
  // insert pg_global
  row_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  insert = row_pair.first.InitializeRow(row_buffer);
  // hard code the first column's value
  first = insert->AccessForceNotNull(row_pair.second[col_ids[0]]);
  tablespace_oid_t pg_global_oid = tablespace_oid_t(GetNextOid());
  (*reinterpret_cast<uint32_t *>(first)) = !pg_global_oid;
  CATALOG_LOG_TRACE("pg_global oid (tablespace_oid) {}", *reinterpret_cast<uint32_t *>(first));
  // TODO(yangjun): we don't support VARCHAR at the moment, the value should be "pg_global", just use random number
  first = insert->AccessForceNotNull(row_pair.second[col_ids[1]]);
  (*reinterpret_cast<uint32_t *>(first)) = 20001;
  pg_tablespace_->Insert(txn, *insert);
  delete[] row_buffer;

  //  CATALOG_LOG_TRACE("Inserting pg_default to pg_tablespace...");
  // insert pg_default
  row_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  insert = row_pair.first.InitializeRow(row_buffer);
  first = insert->AccessForceNotNull(row_pair.second[col_ids[0]]);
  tablespace_oid_t pg_default_oid = tablespace_oid_t(GetNextOid());
  (*reinterpret_cast<uint32_t *>(first)) = !pg_default_oid;
  CATALOG_LOG_TRACE("pg_default oid (tablespace_oid) {}", !pg_default_oid);
  // TODO(yangjun): we don't support VARCHAR at the moment, the value should be "pg_global", just use random number
  first = insert->AccessForceNotNull(row_pair.second[col_ids[1]]);
  (*reinterpret_cast<uint32_t *>(first)) = 20002;
  pg_tablespace_->Insert(txn, *insert);
  delete[] row_buffer;
}

void Catalog::BootstrapDatabase(transaction::TransactionContext *txn, db_oid_t db_oid) {
  map_[db_oid][pg_database_->Oid()] = pg_database_;
  map_[db_oid][pg_tablespace_->Oid()] = pg_tablespace_;
  name_map_[db_oid]["pg_database"] = pg_database_->Oid();
  name_map_[db_oid]["pg_tablespace"] = pg_tablespace_->Oid();
  CreatePGNameSpace(txn, db_oid);
  CreatePGClass(txn, db_oid);
}

void Catalog::CreatePGNameSpace(transaction::TransactionContext *txn, db_oid_t db_oid) {
  // create pg_namespace (table)
  table_oid_t pg_namespace_oid(GetNextTableOid());
  CATALOG_LOG_TRACE("pg_namespace oid (table_oid) {}", !pg_namespace_oid);
  std::vector<Schema::Column> cols;
  cols.emplace_back("oid", type::TypeId::INTEGER, false, GetNextColOid());
  // TODO(yangjun): we don't support VARCHAR at the moment, use INTEGER for now
  cols.emplace_back("nspname", type::TypeId::INTEGER, false, GetNextColOid());

  Schema schema(cols);
  std::shared_ptr<storage::SqlTable> pg_namespace =
      std::make_shared<storage::SqlTable>(&block_store_, schema, pg_namespace_oid);
  map_[db_oid][pg_namespace_oid] = pg_namespace;
  name_map_[db_oid]["pg_namespace"] = pg_namespace_oid;

  // insert pg_catalog (namespace) into pg_namespace (table)
  std::vector<col_oid_t> col_ids;
  for (const auto &c : pg_namespace->GetSchema().GetColumns()) {
    col_ids.emplace_back(c.GetOid());
  }
  auto row_pair = pg_namespace->InitializerForProjectedRow(col_ids);

  byte *row_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *insert = row_pair.first.InitializeRow(row_buffer);
  auto *pg_namespace_col_oid = reinterpret_cast<uint32_t *>(insert->AccessForceNotNull(row_pair.second[col_ids[0]]));
  *pg_namespace_col_oid = !GetNextNamepsaceOid();
  CATALOG_LOG_TRACE("pg_catalog oid (namespace_oid) {}", *pg_namespace_col_oid);
  // TODO(yangjun): we don't support VARCHAR at the moment, just use random number
  auto *pg_namespace_col_nspname =
      reinterpret_cast<uint32_t *>(insert->AccessForceNotNull(row_pair.second[col_ids[1]]));
  *pg_namespace_col_nspname = 30001;

  pg_namespace->Insert(txn, *insert);
  delete[] row_buffer;
}

void Catalog::CreatePGClass(transaction::TransactionContext *txn, db_oid_t db_oid) {
  // create pg_class (table)
  table_oid_t pg_class_oid(GetNextTableOid());
  CATALOG_LOG_TRACE("pg_class oid (table_oid) {}", !pg_class_oid);
  std::vector<Schema::Column> cols;
  std::vector<Schema::Column> pg_class_cols;
  cols.emplace_back("oid", type::TypeId::INTEGER, false, GetNextColOid());
  // TODO(yangjun): we don't support VARCHAR at the moment, use INTEGER for now
  cols.emplace_back("relname", type::TypeId::INTEGER, false, GetNextColOid());
  cols.emplace_back("relnamespace", type::TypeId::INTEGER, false, GetNextColOid());
  cols.emplace_back("reltablespace", type::TypeId::INTEGER, false, GetNextColOid());

  Schema pg_class_schema(cols);
  std::shared_ptr<storage::SqlTable> pg_class =
      std::make_shared<storage::SqlTable>(&block_store_, pg_class_schema, pg_class_oid);
  map_[db_oid][pg_class_oid] = pg_class;
  name_map_[db_oid]["pg_class"] = pg_class_oid;
  // populate pg_class (table)
  CATALOG_LOG_TRACE("populating pg_class ...");
  // insert pg_database, pg_tablespace, pg_namespace, pg_class into pg_class
  std::vector<col_oid_t> col_ids;
  for (const auto &c : pg_class->GetSchema().GetColumns()) {
    col_ids.emplace_back(c.GetOid());
  }
  auto row_pair = pg_class->InitializerForProjectedRow(col_ids);

  // Insert pg_database
  byte *row_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *insert = row_pair.first.InitializeRow(row_buffer);
  CATALOG_LOG_TRACE("entrying the first entry ...");
  auto *col_ptr = reinterpret_cast<uint32_t *>(insert->AccessForceNotNull(row_pair.second[col_ids[0]]));
  *col_ptr = !GetDatabaseCatalog(db_oid, "pg_database")->Oid();
  // TODO(yangjun): we don't support VARCHAR at the moment, just use random number
  CATALOG_LOG_TRACE("entrying the second entry ...");
  col_ptr = reinterpret_cast<uint32_t *>(insert->AccessForceNotNull(row_pair.second[col_ids[1]]));
  *col_ptr = 10001;
  CATALOG_LOG_TRACE("entrying the third entry ...");
  col_ptr = reinterpret_cast<uint32_t *>(insert->AccessForceNotNull(row_pair.second[col_ids[2]]));
  *col_ptr = !GetDatabaseHandle(db_oid).GetNamespaceHandle().GetNamespaceEntry(txn, "pg_catalog")->GetNamespaceOid();
  CATALOG_LOG_TRACE("entrying the fourth entry ...");
  col_ptr = reinterpret_cast<uint32_t *>(insert->AccessForceNotNull(row_pair.second[col_ids[3]]));
  *col_ptr = !GetTablespaceHandle().GetTablespaceEntry(txn, "pg_global")->GetTablespaceOid();
  pg_class->Insert(txn, *insert);
  delete[] row_buffer;

  // Insert pg_tablespace
  row_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  insert = row_pair.first.InitializeRow(row_buffer);
  col_ptr = reinterpret_cast<uint32_t *>(insert->AccessForceNotNull(row_pair.second[col_ids[0]]));
  *col_ptr = !GetDatabaseCatalog(db_oid, "pg_tablespace")->Oid();
  // TODO(yangjun): we don't support VARCHAR at the moment, just use random number
  col_ptr = reinterpret_cast<uint32_t *>(insert->AccessForceNotNull(row_pair.second[col_ids[1]]));
  *col_ptr = 10002;
  col_ptr = reinterpret_cast<uint32_t *>(insert->AccessForceNotNull(row_pair.second[col_ids[2]]));
  *col_ptr = !GetDatabaseHandle(db_oid).GetNamespaceHandle().GetNamespaceEntry(txn, "pg_catalog")->GetNamespaceOid();
  col_ptr = reinterpret_cast<uint32_t *>(insert->AccessForceNotNull(row_pair.second[col_ids[3]]));
  *col_ptr = !GetTablespaceHandle().GetTablespaceEntry(txn, "pg_global")->GetTablespaceOid();
  pg_class->Insert(txn, *insert);
  delete[] row_buffer;

  // Insert pg_namespace
  row_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  insert = row_pair.first.InitializeRow(row_buffer);
  col_ptr = reinterpret_cast<uint32_t *>(insert->AccessForceNotNull(row_pair.second[col_ids[0]]));
  *col_ptr = !GetDatabaseCatalog(db_oid, "pg_namespace")->Oid();
  // TODO(yangjun): we don't support VARCHAR at the moment, just use random number
  col_ptr = reinterpret_cast<uint32_t *>(insert->AccessForceNotNull(row_pair.second[col_ids[1]]));
  *col_ptr = 10003;
  col_ptr = reinterpret_cast<uint32_t *>(insert->AccessForceNotNull(row_pair.second[col_ids[2]]));
  *col_ptr = !GetDatabaseHandle(db_oid).GetNamespaceHandle().GetNamespaceEntry(txn, "pg_catalog")->GetNamespaceOid();
  col_ptr = reinterpret_cast<uint32_t *>(insert->AccessForceNotNull(row_pair.second[col_ids[3]]));
  *col_ptr = !GetTablespaceHandle().GetTablespaceEntry(txn, "pg_default")->GetTablespaceOid();
  pg_class->Insert(txn, *insert);
  delete[] row_buffer;

  // Insert pg_class
  row_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  insert = row_pair.first.InitializeRow(row_buffer);
  col_ptr = reinterpret_cast<uint32_t *>(insert->AccessForceNotNull(row_pair.second[col_ids[0]]));
  *col_ptr = !GetDatabaseCatalog(db_oid, "pg_class")->Oid();
  // TODO(yangjun): we don't support VARCHAR at the moment, just use random number
  col_ptr = reinterpret_cast<uint32_t *>(insert->AccessForceNotNull(row_pair.second[col_ids[1]]));
  *col_ptr = 10004;
  col_ptr = reinterpret_cast<uint32_t *>(insert->AccessForceNotNull(row_pair.second[col_ids[2]]));
  *col_ptr = !GetDatabaseHandle(db_oid).GetNamespaceHandle().GetNamespaceEntry(txn, "pg_catalog")->GetNamespaceOid();
  col_ptr = reinterpret_cast<uint32_t *>(insert->AccessForceNotNull(row_pair.second[col_ids[3]]));
  *col_ptr = !GetTablespaceHandle().GetTablespaceEntry(txn, "pg_default")->GetTablespaceOid();
  pg_class->Insert(txn, *insert);
  delete[] row_buffer;
}

}  // namespace terrier::catalog
