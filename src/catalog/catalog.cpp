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

std::shared_ptr<catalog::SqlTableRW> Catalog::GetDatabaseCatalog(db_oid_t db_oid, table_oid_t table_oid) {
  return map_.at(db_oid).at(table_oid);
}

std::shared_ptr<catalog::SqlTableRW> Catalog::GetDatabaseCatalog(db_oid_t db_oid, const std::string &table_name) {
  return GetDatabaseCatalog(db_oid, name_map_.at(db_oid).at(table_name));
}

uint32_t Catalog::GetNextOid() { return oid_++; }

void Catalog::Bootstrap() {
  CATALOG_LOG_TRACE("Bootstrapping global catalogs ...");
  transaction::TransactionContext *txn = txn_manager_->BeginTransaction();

  CreatePGDatabase(table_oid_t(GetNextOid()));
  PopulatePGDatabase(txn);

  CreatePGTablespace(table_oid_t(GetNextOid()));
  PopulatePGTablespace(txn);

  BootstrapDatabase(txn, DEFAULT_DATABASE_OID);
  txn_manager_->Commit(txn, BootstrapCallback, nullptr);
  delete txn;
}

#ifdef notdef
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
  map_[db_oid][pg_namespace_oid] = pg_namespace
                                       .

                                   // to this database's namespace, add a pg_catalog namespace
                                   pg_namespace->StartRow();
  pg_namespace->SetIntColInRow(0, !PG_CATALOG_OID);
  pg_namespace->SetIntColInRow(1, 22222);
  pg_namespace->EndRowAndInsert(txn);
}
#endif /* notdef */

void Catalog::CreatePGDatabase(table_oid_t table_oid) {
  CATALOG_LOG_TRACE("Creating pg_database table");
  // set the oid
  pg_database_ = std::make_shared<catalog::SqlTableRW>(table_oid);

  // add the schema
  // TODO(pakhtar): we don't support VARCHAR at the moment, use INTEGER for now
  pg_database_->DefineColumn("oid", type::TypeId::INTEGER, false, col_oid_t(GetNextOid()));
  pg_database_->DefineColumn("datname", type::TypeId::VARCHAR, false, col_oid_t(GetNextOid()));
  // create the table
  pg_database_->Create();
}

void Catalog::PopulatePGDatabase(transaction::TransactionContext *txn) {
  db_oid_t terrier_oid = DEFAULT_DATABASE_OID;

  CATALOG_LOG_TRACE("Populate pg_database table");
  pg_database_->StartRow();
  pg_database_->SetIntColInRow(0, !terrier_oid);
  pg_database_->SetVarcharColInRow(1, "terrier");
  pg_database_->EndRowAndInsert(txn);

  // add it to the map
  map_[terrier_oid] = std::unordered_map<table_oid_t, std::shared_ptr<catalog::SqlTableRW>>();
}

void Catalog::CreatePGTablespace(table_oid_t table_oid) {
  CATALOG_LOG_TRACE("Creating pg_tablespace table");
  // set the oid
  pg_tablespace_ = std::make_shared<catalog::SqlTableRW>(table_oid);

  // add the schema
  pg_tablespace_->DefineColumn("oid", type::TypeId::INTEGER, false, col_oid_t(GetNextOid()));
  pg_tablespace_->DefineColumn("spcname", type::TypeId::VARCHAR, false, col_oid_t(GetNextOid()));
  pg_tablespace_->Create();
}

void Catalog::PopulatePGTablespace(transaction::TransactionContext *txn) {
  CATALOG_LOG_TRACE("Populate pg_tablespace table");

  tablespace_oid_t pg_global_oid = tablespace_oid_t(GetNextOid());
  tablespace_oid_t pg_default_oid = tablespace_oid_t(GetNextOid());

  pg_tablespace_->StartRow();
  pg_tablespace_->SetIntColInRow(0, !pg_global_oid);
  pg_tablespace_->SetVarcharColInRow(1, "pg_global");
  pg_tablespace_->EndRowAndInsert(txn);

  pg_tablespace_->StartRow();
  pg_tablespace_->SetIntColInRow(0, !pg_default_oid);
  pg_tablespace_->SetVarcharColInRow(1, "pg_default");
  pg_tablespace_->EndRowAndInsert(txn);
}

void Catalog::BootstrapDatabase(transaction::TransactionContext *txn, db_oid_t db_oid) {
  CATALOG_LOG_TRACE("Bootstrapping database oid (db_oid) {}", !db_oid);
  map_[db_oid][pg_database_->Oid()] = pg_database_;
  map_[db_oid][pg_tablespace_->Oid()] = pg_tablespace_;
  name_map_[db_oid]["pg_database"] = pg_database_->Oid();
  name_map_[db_oid]["pg_tablespace"] = pg_tablespace_->Oid();

  CreatePGNameSpace(txn, db_oid);
  CreatePGClass(txn, db_oid);
}

void Catalog::CreatePGNameSpace(transaction::TransactionContext *txn, db_oid_t db_oid) {
  std::shared_ptr<catalog::SqlTableRW> pg_namespace;
  /*
   * Create pg_namespace.
   * Postgres has 4 columns in pg_namespace. We currently implement:
   * - oid
   * - nspname - will be type varlen - the namespace name.
   */
  table_oid_t pg_namespace_oid(GetNextOid());
  pg_namespace = std::make_shared<catalog::SqlTableRW>(pg_namespace_oid);
  pg_namespace->DefineColumn("oid", type::TypeId::INTEGER, false, col_oid_t(GetNextOid()));
  pg_namespace->DefineColumn("nspname", type::TypeId::VARCHAR, false, col_oid_t(GetNextOid()));
  pg_namespace->Create();

  map_[db_oid][pg_namespace_oid] = pg_namespace;
  name_map_[db_oid]["pg_namespace"] = pg_namespace_oid;

  // insert pg_catalog
  uint32_t pg_namespace_col_oid = !namespace_oid_t(GetNextOid());
  pg_namespace->StartRow();
  pg_namespace->SetIntColInRow(0, pg_namespace_col_oid);
  pg_namespace->SetVarcharColInRow(1, "pg_catalog");
  pg_namespace->EndRowAndInsert(txn);
}

void Catalog::CreatePGClass(transaction::TransactionContext *txn, db_oid_t db_oid) {
  // oid for pg_class table
  table_oid_t pg_class_oid(GetNextOid());
  std::shared_ptr<catalog::SqlTableRW> pg_class;
  CATALOG_LOG_TRACE("pg_class oid (table_oid) {}", !pg_class_oid);
  pg_class = std::make_shared<catalog::SqlTableRW>(pg_class_oid);

  // add the schema
  pg_class->DefineColumn("oid", type::TypeId::INTEGER, false, col_oid_t(GetNextOid()));
  pg_class->DefineColumn("relname", type::TypeId::VARCHAR, false, col_oid_t(GetNextOid()));
  pg_class->DefineColumn("relnamespace", type::TypeId::INTEGER, false, col_oid_t(GetNextOid()));
  pg_class->DefineColumn("reltablespace", type::TypeId::INTEGER, false, col_oid_t(GetNextOid()));
  pg_class->Create();

  map_[db_oid][pg_class_oid] = pg_class;
  name_map_[db_oid]["pg_class"] = pg_class_oid;

  // Insert pg_database
  CATALOG_LOG_TRACE("Inserting pg_database into pg_class ...");
  auto entry_db_oid = !GetDatabaseCatalog(db_oid, "pg_database")->Oid();
  CATALOG_LOG_TRACE("before 0.1...");
  auto namespace_oid =
      !GetDatabaseHandle(db_oid).GetNamespaceHandle().GetNamespaceEntry(txn, "pg_catalog")->GetNamespaceOid();
  CATALOG_LOG_TRACE("before 0.2...");
  auto tablespace_oid = !GetTablespaceHandle().GetTablespaceEntry(txn, "pg_global")->GetTablespaceOid();
  CATALOG_LOG_TRACE("before 1...");
  pg_class->StartRow();
  CATALOG_LOG_TRACE("before 2...");
  pg_class->SetIntColInRow(0, entry_db_oid);
  CATALOG_LOG_TRACE("before 3 ...");
  pg_class->SetVarcharColInRow(1, "pg_database");
  CATALOG_LOG_TRACE("before 4 ...");
  pg_class->SetIntColInRow(2, namespace_oid);
  pg_class->SetIntColInRow(3, tablespace_oid);
  pg_class->EndRowAndInsert(txn);

  // Insert pg_tablespace
  CATALOG_LOG_TRACE("Inserting pg_tablespace into pg_class ...");
  entry_db_oid = !GetDatabaseCatalog(db_oid, "pg_tablespace")->Oid();
  namespace_oid =
      !GetDatabaseHandle(db_oid).GetNamespaceHandle().GetNamespaceEntry(txn, "pg_catalog")->GetNamespaceOid();
  tablespace_oid = !GetTablespaceHandle().GetTablespaceEntry(txn, "pg_global")->GetTablespaceOid();

  pg_class->StartRow();
  pg_class->SetIntColInRow(0, entry_db_oid);
  pg_class->SetVarcharColInRow(1, "pg_tablespace");
  pg_class->SetIntColInRow(2, namespace_oid);
  pg_class->SetIntColInRow(3, tablespace_oid);
  pg_class->EndRowAndInsert(txn);

  // Insert pg_namespace
  // TODO: fix failure when spelled pg_namepace
  CATALOG_LOG_TRACE("Inserting pg_namespace into pg_class ...");
  entry_db_oid = !GetDatabaseCatalog(db_oid, "pg_namespace")->Oid();
  namespace_oid =
      !GetDatabaseHandle(db_oid).GetNamespaceHandle().GetNamespaceEntry(txn, "pg_catalog")->GetNamespaceOid();
  tablespace_oid = !GetTablespaceHandle().GetTablespaceEntry(txn, "pg_default")->GetTablespaceOid();

  pg_class->StartRow();
  pg_class->SetIntColInRow(0, entry_db_oid);
  pg_class->SetVarcharColInRow(1, "pg_namespace");
  pg_class->SetIntColInRow(2, namespace_oid);
  pg_class->SetIntColInRow(3, tablespace_oid);
  pg_class->EndRowAndInsert(txn);

  // Insert pg_class
  CATALOG_LOG_TRACE("Inserting pg_class into pg_class ...");
  entry_db_oid = !GetDatabaseCatalog(db_oid, "pg_class")->Oid();
  namespace_oid =
      !GetDatabaseHandle(db_oid).GetNamespaceHandle().GetNamespaceEntry(txn, "pg_catalog")->GetNamespaceOid();
  tablespace_oid = !GetTablespaceHandle().GetTablespaceEntry(txn, "pg_default")->GetTablespaceOid();

  pg_class->StartRow();
  pg_class->SetIntColInRow(0, entry_db_oid);
  pg_class->SetVarcharColInRow(1, "pg_class");
  pg_class->SetIntColInRow(2, namespace_oid);
  pg_class->SetIntColInRow(3, tablespace_oid);
  pg_class->EndRowAndInsert(txn);
}

}  // namespace terrier::catalog
