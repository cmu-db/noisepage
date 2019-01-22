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

Catalog::Catalog(transaction::TransactionManager *txn_manager)
    : txn_manager_(txn_manager),
      db_oid_(DEFAULT_DATABASE_OID),
      namespace_oid_(PG_CATALOG_OID),
      table_oid_(DEFAULT_TABLE_OID),
      col_oid_(DEFAULT_COL_OID) {
  CATALOG_LOG_TRACE("Creating catalog ...");
  Bootstrap();
}

DatabaseHandle Catalog::GetDatabaseHandle(db_oid_t db_oid) {
  return DatabaseHandle(this, db_oid, pg_database_->GetSqlTable());
}

std::shared_ptr<storage::SqlTable> Catalog::GetDatabaseCatalog(db_oid_t db_oid, table_oid_t table_oid) {
  return map_[db_oid][table_oid];
}

db_oid_t Catalog::GetNextDBOid() { return db_oid_++; }

namespace_oid_t Catalog::GetNextNamespaceOid() { return namespace_oid_++; }

table_oid_t Catalog::GetNextTableOid() { return table_oid_++; }

col_oid_t Catalog::GetNextColOid() { return col_oid_++; }

void Catalog::Bootstrap() {
  CATALOG_LOG_TRACE("Bootstrapping global catalogs ...");
  transaction::TransactionContext *txn = txn_manager_->BeginTransaction();

  CreatePGDatabase(GetNextTableOid());
  PopulatePGDatabase(txn);

  CreatePGTablespace(GetNextTableOid());
  PopulatePGTablespace(txn);

  BootstrapDatabase(txn, DEFAULT_DATABASE_OID);
  txn_manager_->Commit(txn, BootstrapCallback, nullptr);
  delete txn;
}

void Catalog::BootstrapDatabase(transaction::TransactionContext *txn, db_oid_t db_oid) {
  std::shared_ptr<catalog::SqlTableRW> pg_namespace;

  // refer to OID assignment scheme in catalog.h
  table_oid_.store(DATABASE_CATALOG_TABLE_START_OID);
  col_oid_.store(DATABASE_CATALOG_COL_START_OID);

  // TODO(pakhtar): replace nspname type with VARCHAR
  /*
   * Create pg_namespace.
   * Postgres has 4 columns in pg_namespace. We currently implement:
   * - oid
   * - nspname - will be type varlen - the namespace name.
   */
  table_oid_t pg_namespace_oid(GetNextTableOid());
  pg_namespace = std::make_shared<catalog::SqlTableRW>(pg_namespace_oid);
  pg_namespace->DefineColumn("oid", type::TypeId::INTEGER, false, GetNextColOid());
  pg_namespace->DefineColumn("nspname", type::TypeId::INTEGER, false, GetNextColOid());
  pg_namespace->Create();
  // TODO(pakhtar) : fix
  // map_[db_oid][pg_namespace_oid] = pg_namespace.

  // to this database's namespace, add a pg_catalog namespace
  pg_namespace->StartRow();
  pg_namespace->SetIntColInRow(0, !PG_CATALOG_OID);
  pg_namespace->SetIntColInRow(1, 22222);
  pg_namespace->EndRowAndInsert(txn);
}

void Catalog::CreatePGDatabase(table_oid_t table_oid) {
  CATALOG_LOG_TRACE("Creating pg_database table");
  // set the oid
  pg_database_ = std::make_shared<catalog::SqlTableRW>(table_oid);

  // add the schema
  // TODO(pakhtar): we don't support VARCHAR at the moment, use INTEGER for now
  pg_database_->DefineColumn("oid", type::TypeId::INTEGER, false, GetNextColOid());
  pg_database_->DefineColumn("datname", type::TypeId::INTEGER, false, GetNextColOid());
  // create the table
  pg_database_->Create();
}

void Catalog::PopulatePGDatabase(transaction::TransactionContext *txn) {
  db_oid_t terrier_oid = DEFAULT_DATABASE_OID;

  CATALOG_LOG_TRACE("Populate pg_database table");
  pg_database_->StartRow();
  // ! => underlying value. Replace with OID column type?
  pg_database_->SetIntColInRow(0, !terrier_oid);
  pg_database_->SetIntColInRow(1, 12345);
  pg_database_->EndRowAndInsert(txn);

  // add it to the map
  map_[terrier_oid] = std::unordered_map<table_oid_t, std::shared_ptr<storage::SqlTable>>();
}

void Catalog::CreatePGTablespace(table_oid_t table_oid) {
  CATALOG_LOG_TRACE("Creating pg_tablespace table");
  // set the oid
  pg_tablespace_ = std::make_shared<catalog::SqlTableRW>(table_oid);

  // add the schema
  // TODO(pakhtar): we don't support VARCHAR at the moment, use INTEGER for now
  pg_tablespace_->DefineColumn("spcname", type::TypeId::INTEGER, false, GetNextColOid());
  pg_tablespace_->Create();
}

void Catalog::PopulatePGTablespace(transaction::TransactionContext *txn) {
  CATALOG_LOG_TRACE("Populate pg_tablespace table");

  pg_tablespace_->StartRow();
  // insert pg_global. Placeholder value now, "pg_global" VARLEN soon.
  pg_tablespace_->SetIntColInRow(0, 11111);
  pg_tablespace_->EndRowAndInsert(txn);

  pg_tablespace_->StartRow();
  // insert pg_default. Placeholder value now, "pg_default" VARLEN soon.
  pg_tablespace_->SetIntColInRow(0, 22222);
  pg_tablespace_->EndRowAndInsert(txn);
}

}  // namespace terrier::catalog
