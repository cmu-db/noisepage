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
  CATALOG_LOG_TRACE("=======Finished Bootstrapping ======");
}

void Catalog::CreateDatabase(transaction::TransactionContext *txn, const char *name) {
  db_oid_t new_db_oid = db_oid_t(GetNextOid());
 Catalog::AddEntryToPGDatabase(txn, new_db_oid, name);
}

void Catalog::DeleteDatabase(transaction::TransactionContext *txn, const char *db_name) {
  // get database handle
  auto db_handle = GetDatabaseHandle();
  auto db_entry = db_handle.GetDatabaseEntry(txn, db_name);
  // TODO(pakhtar): complete
  // delete the entry
  // delete from the maps
}

DatabaseHandle Catalog::GetDatabaseHandle() { return DatabaseHandle(this, pg_database_); }

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

void Catalog::AddUnusedSchemaColumns(const std::shared_ptr<catalog::SqlTableRW> &db_p,
                                     const std::vector<UnusedSchemaCols> &cols) {
  for (const auto &col : cols) {
    db_p->DefineColumn(col.col_name, col.type_id, false, col_oid_t(GetNextOid()));
  }
}

void Catalog::AddColumnsToPGAttribute(transaction::TransactionContext *txn, db_oid_t db_oid,
                                      const std::shared_ptr<storage::SqlTable> &table) {
  Schema schema = table->GetSchema();
  std::vector<Schema::Column> cols = schema.GetColumns();
  std::shared_ptr<catalog::SqlTableRW> pg_attribute = map_[db_oid][name_map_[db_oid]["pg_attribute"]];
  for (auto &c : cols) {
    std::vector<type::Value> row;
    row.emplace_back(type::ValueFactory::GetIntegerValue(!c.GetOid()));
    row.emplace_back(type::ValueFactory::GetIntegerValue(!table->Oid()));
    row.emplace_back(type::ValueFactory::GetVarcharValue(c.GetName().c_str()));
    // the following 3 attributes are just placeholders, so I just use 0.
    row.emplace_back(type::ValueFactory::GetIntegerValue(0));
    row.emplace_back(type::ValueFactory::GetIntegerValue(0));
    row.emplace_back(type::ValueFactory::GetIntegerValue(0));
    pg_attribute->InsertRow(txn, row);
  }
}

void Catalog::CreatePGDatabase(table_oid_t table_oid) {
  CATALOG_LOG_TRACE("Creating pg_database table");
  // set the oid
  pg_database_ = std::make_shared<catalog::SqlTableRW>(table_oid);

  // add the schema
  pg_database_->DefineColumn("oid", type::TypeId::INTEGER, false, col_oid_t(GetNextOid()));
  pg_database_->DefineColumn("datname", type::TypeId::VARCHAR, false, col_oid_t(GetNextOid()));
  AddUnusedSchemaColumns(pg_database_, pg_database_unused_cols_);
  // create the table
  pg_database_->Create();
}

void Catalog::PopulatePGDatabase(transaction::TransactionContext *txn) {
  std::vector<type::Value> row;
  db_oid_t terrier_oid = DEFAULT_DATABASE_OID;
  CATALOG_LOG_TRACE("Populate pg_database table");

  row.emplace_back(type::ValueFactory::GetIntegerValue(!terrier_oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue("terrier"));
  SetUnusedColumns(&row, pg_database_unused_cols_);
  pg_database_->InsertRow(txn, row);

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
  AddUnusedSchemaColumns(pg_tablespace_, pg_tablespace_unused_cols_);
  // create the table
  pg_tablespace_->Create();
}

void Catalog::PopulatePGTablespace(transaction::TransactionContext *txn) {
  std::vector<type::Value> row;
  CATALOG_LOG_TRACE("Populate pg_tablespace table");

  tablespace_oid_t pg_global_oid = tablespace_oid_t(GetNextOid());
  tablespace_oid_t pg_default_oid = tablespace_oid_t(GetNextOid());

  row.emplace_back(type::ValueFactory::GetIntegerValue(!pg_global_oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue("pg_global"));
  SetUnusedColumns(&row, pg_tablespace_unused_cols_);
  pg_tablespace_->InsertRow(txn, row);

  row.clear();
  row.emplace_back(type::ValueFactory::GetIntegerValue(!pg_default_oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue("pg_default"));
  SetUnusedColumns(&row, pg_tablespace_unused_cols_);
  pg_tablespace_->InsertRow(txn, row);

  // TODO(yeshengm): do we have to add it to the global map?
}

void Catalog::BootstrapDatabase(transaction::TransactionContext *txn, db_oid_t db_oid) {
  CATALOG_LOG_TRACE("Bootstrapping database oid (db_oid) {}", !db_oid);
  map_[db_oid][pg_database_->Oid()] = pg_database_;
  map_[db_oid][pg_tablespace_->Oid()] = pg_tablespace_;
  name_map_[db_oid]["pg_database"] = pg_database_->Oid();
  name_map_[db_oid]["pg_tablespace"] = pg_tablespace_->Oid();

  // Order: pg_attribute -> pg_namespace -> pg_class
  CreatePGAttribute(txn, db_oid);
  CreatePGNameSpace(txn, db_oid);
  CreatePGClass(txn, db_oid);

  CreatePGAttrDef(txn, db_oid);
}

void Catalog::CreatePGAttribute(terrier::transaction::TransactionContext *txn, terrier::catalog::db_oid_t db_oid) {
  // oid for pg_attribute table
  table_oid_t pg_attribute_oid(GetNextOid());
  std::shared_ptr<catalog::SqlTableRW> pg_attribute;
  CATALOG_LOG_TRACE("pg_attribute oid (table_oid) {}", !pg_attribute_oid);
  pg_attribute = std::make_shared<catalog::SqlTableRW>(pg_attribute_oid);

  // add the schema
  std::vector<col_oid_t> next_col_oids(6);
  for (auto &oid : next_col_oids) {
    oid = col_oid_t(GetNextOid());
  }
  pg_attribute->DefineColumn("oid", type::TypeId::INTEGER, false, next_col_oids[0]);
  pg_attribute->DefineColumn("attrelid", type::TypeId::INTEGER, false, next_col_oids[1]);
  pg_attribute->DefineColumn("attname", type::TypeId::VARCHAR, false, next_col_oids[2]);
  pg_attribute->DefineColumn("atttypid", type::TypeId::INTEGER, true, next_col_oids[3]);
  pg_attribute->DefineColumn("attlen", type::TypeId::INTEGER, true, next_col_oids[4]);
  pg_attribute->DefineColumn("attnum", type::TypeId::INTEGER, true, next_col_oids[5]);
  pg_attribute->Create();

  map_[db_oid][pg_attribute_oid] = pg_attribute;
  name_map_[db_oid]["pg_attribute"] = pg_attribute_oid;

  // Insert columns of pg_attribute
  CATALOG_LOG_TRACE("Inserting columns of pg_attribute into pg_attribute ...");
  AddColumnsToPGAttribute(txn, db_oid, pg_attribute->GetSqlTable());

  // Insert columns of global catalogs
  AddColumnsToPGAttribute(txn, db_oid, map_[db_oid][name_map_[db_oid]["pg_database"]]->GetSqlTable());
  AddColumnsToPGAttribute(txn, db_oid, map_[db_oid][name_map_[db_oid]["pg_tablespace"]]->GetSqlTable());
}

void Catalog::CreatePGAttrDef(transaction::TransactionContext *txn, db_oid_t db_oid) {
  table_oid_t pg_attrdef_oid(GetNextOid());
  std::shared_ptr<catalog::SqlTableRW> pg_attrdef;

  pg_attrdef = std::make_shared<catalog::SqlTableRW>(pg_attrdef_oid);
  pg_attrdef->DefineColumn("oid", type::TypeId::INTEGER, false, col_oid_t(GetNextOid()));
  // table to which this column belongs. References pg_class.oid
  pg_attrdef->DefineColumn("adrelid", type::TypeId::INTEGER, false, col_oid_t(GetNextOid()));
  // the number of the column. References pg_attribute.attnum
  pg_attrdef->DefineColumn("adnum", type::TypeId::INTEGER, false, col_oid_t(GetNextOid()));
  pg_attrdef->DefineColumn("adbin", type::TypeId::VARCHAR, false, col_oid_t(GetNextOid()));

  // unused, but not defined via AddUnusedSchemaColumns for simplicity
  pg_attrdef->DefineColumn("adsrc", type::TypeId::VARCHAR, false, col_oid_t(GetNextOid()));
  pg_attrdef->Create();

  map_[db_oid][pg_attrdef_oid] = pg_attrdef;
  name_map_[db_oid]["pg_attrdef"] = pg_attrdef_oid;

  CATALOG_LOG_TRACE("Inserting columns of pg_attrdef into pg_attribute ...");
  AddColumnsToPGAttribute(txn, db_oid, pg_attrdef->GetSqlTable());
}

void Catalog::CreatePGNameSpace(transaction::TransactionContext *txn, db_oid_t db_oid) {
  std::vector<type::Value> row;
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
  AddUnusedSchemaColumns(pg_namespace, pg_namespace_unused_cols_);
  pg_namespace->Create();

  // Add columns to pg_attribute
  AddColumnsToPGAttribute(txn, db_oid, pg_namespace->GetSqlTable());

  // insert to catalog map
  map_[db_oid][pg_namespace_oid] = pg_namespace;
  name_map_[db_oid]["pg_namespace"] = pg_namespace_oid;

  // insert columns to pg_attribute
  pg_namespace->GetSqlTable()->GetSchema();

  // insert pg_catalog
  uint32_t pg_namespace_col_oid = !namespace_oid_t(GetNextOid());
  row.emplace_back(type::ValueFactory::GetIntegerValue(pg_namespace_col_oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue("pg_catalog"));
  SetUnusedColumns(&row, pg_namespace_unused_cols_);
  pg_namespace->InsertRow(txn, row);

  // insert public
  row.clear();
  pg_namespace_col_oid = !namespace_oid_t(GetNextOid());
  row.emplace_back(type::ValueFactory::GetIntegerValue(pg_namespace_col_oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue("public"));
  SetUnusedColumns(&row, pg_namespace_unused_cols_);
  pg_namespace->InsertRow(txn, row);
}

void Catalog::CreatePGClass(transaction::TransactionContext *txn, db_oid_t db_oid) {
  std::vector<type::Value> row;
  // oid for pg_class table
  table_oid_t pg_class_oid(GetNextOid());
  std::shared_ptr<catalog::SqlTableRW> pg_class;
  CATALOG_LOG_TRACE("pg_class oid (table_oid) {}", !pg_class_oid);
  pg_class = std::make_shared<catalog::SqlTableRW>(pg_class_oid);

  // add the schema
  // TODO(yangjuns): __ptr column stores the pointers to SqlTableRWs. It should be hidden from the user
  pg_class->DefineColumn("__ptr", type::TypeId::BIGINT, false, col_oid_t(GetNextOid()));
  pg_class->DefineColumn("oid", type::TypeId::INTEGER, false, col_oid_t(GetNextOid()));
  pg_class->DefineColumn("relname", type::TypeId::VARCHAR, false, col_oid_t(GetNextOid()));
  pg_class->DefineColumn("relnamespace", type::TypeId::INTEGER, false, col_oid_t(GetNextOid()));
  pg_class->DefineColumn("reltablespace", type::TypeId::INTEGER, false, col_oid_t(GetNextOid()));
  pg_class->Create();

  // Add columns to pg_attribute
  AddColumnsToPGAttribute(txn, db_oid, pg_class->GetSqlTable());

  map_[db_oid][pg_class_oid] = pg_class;
  name_map_[db_oid]["pg_class"] = pg_class_oid;

  // Insert pg_database
  CATALOG_LOG_TRACE("Inserting pg_database into pg_class ...");
  auto entry_db_oid = !GetDatabaseCatalog(db_oid, "pg_database")->Oid();
  auto namespace_oid =
      !GetDatabaseHandle().GetNamespaceHandle(txn, db_oid).GetNamespaceEntry(txn, "pg_catalog")->GetNamespaceOid();
  auto tablespace_oid = !GetTablespaceHandle().GetTablespaceEntry(txn, "pg_global")->GetTablespaceOid();
  row.emplace_back(
      type::ValueFactory::GetBigIntValue(reinterpret_cast<uint64_t>(GetDatabaseCatalog(db_oid, "pg_database").get())));
  row.emplace_back(type::ValueFactory::GetIntegerValue(entry_db_oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue("pg_database"));
  row.emplace_back(type::ValueFactory::GetIntegerValue(namespace_oid));
  row.emplace_back(type::ValueFactory::GetIntegerValue(tablespace_oid));
  pg_class->InsertRow(txn, row);

  // Insert pg_tablespace
  CATALOG_LOG_TRACE("Inserting pg_tablespace into pg_class ...");
  entry_db_oid = !GetDatabaseCatalog(db_oid, "pg_tablespace")->Oid();
  namespace_oid =
      !GetDatabaseHandle().GetNamespaceHandle(txn, db_oid).GetNamespaceEntry(txn, "pg_catalog")->GetNamespaceOid();
  tablespace_oid = !GetTablespaceHandle().GetTablespaceEntry(txn, "pg_global")->GetTablespaceOid();

  row.clear();
  row.emplace_back(type::ValueFactory::GetBigIntValue(
      reinterpret_cast<uint64_t>(GetDatabaseCatalog(db_oid, "pg_tablespace").get())));
  row.emplace_back(type::ValueFactory::GetIntegerValue(entry_db_oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue("pg_tablespace"));
  row.emplace_back(type::ValueFactory::GetIntegerValue(namespace_oid));
  row.emplace_back(type::ValueFactory::GetIntegerValue(tablespace_oid));
  pg_class->InsertRow(txn, row);

  // Insert pg_namespace
  CATALOG_LOG_TRACE("Inserting pg_namespace into pg_class ...");
  entry_db_oid = !GetDatabaseCatalog(db_oid, "pg_namespace")->Oid();
  namespace_oid =
      !GetDatabaseHandle().GetNamespaceHandle(txn, db_oid).GetNamespaceEntry(txn, "pg_catalog")->GetNamespaceOid();
  tablespace_oid = !GetTablespaceHandle().GetTablespaceEntry(txn, "pg_default")->GetTablespaceOid();

  row.clear();
  row.emplace_back(
      type::ValueFactory::GetBigIntValue(reinterpret_cast<uint64_t>(GetDatabaseCatalog(db_oid, "pg_namespace").get())));
  row.emplace_back(type::ValueFactory::GetIntegerValue(entry_db_oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue("pg_namespace"));
  row.emplace_back(type::ValueFactory::GetIntegerValue(namespace_oid));
  row.emplace_back(type::ValueFactory::GetIntegerValue(tablespace_oid));
  pg_class->InsertRow(txn, row);

  // Insert pg_class
  CATALOG_LOG_TRACE("Inserting pg_class into pg_class ...");
  entry_db_oid = !GetDatabaseCatalog(db_oid, "pg_class")->Oid();
  namespace_oid =
      !GetDatabaseHandle().GetNamespaceHandle(txn, db_oid).GetNamespaceEntry(txn, "pg_catalog")->GetNamespaceOid();
  tablespace_oid = !GetTablespaceHandle().GetTablespaceEntry(txn, "pg_default")->GetTablespaceOid();

  row.clear();
  row.emplace_back(
      type::ValueFactory::GetBigIntValue(reinterpret_cast<uint64_t>(GetDatabaseCatalog(db_oid, "pg_class").get())));
  row.emplace_back(type::ValueFactory::GetIntegerValue(entry_db_oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue("pg_class"));
  row.emplace_back(type::ValueFactory::GetIntegerValue(namespace_oid));
  row.emplace_back(type::ValueFactory::GetIntegerValue(tablespace_oid));
  pg_class->InsertRow(txn, row);

  // Insert pg_attribute
  CATALOG_LOG_TRACE("Inserting pg_attribute into pg_class ...");
  entry_db_oid = !GetDatabaseCatalog(db_oid, "pg_attribute")->Oid();
  namespace_oid =
      !GetDatabaseHandle().GetNamespaceHandle(txn, db_oid).GetNamespaceEntry(txn, "pg_catalog")->GetNamespaceOid();
  tablespace_oid = !GetTablespaceHandle().GetTablespaceEntry(txn, "pg_default")->GetTablespaceOid();

  row.clear();
  row.emplace_back(
      type::ValueFactory::GetBigIntValue(reinterpret_cast<uint64_t>(GetDatabaseCatalog(db_oid, "pg_attribute").get())));
  row.emplace_back(type::ValueFactory::GetIntegerValue(entry_db_oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue("pg_attribute"));
  row.emplace_back(type::ValueFactory::GetIntegerValue(namespace_oid));
  row.emplace_back(type::ValueFactory::GetIntegerValue(tablespace_oid));
  pg_class->InsertRow(txn, row);
}

void Catalog::DestroyDB(db_oid_t oid) {
  // Note that we are using shared pointers for SqlTableRW. Catalog class have references to all the catalog tables,
  // (i.e, tables that have namespace "pg_catalog") but not user created tables. We cannot use a shared pointer for a
  // user table because it will be automatically freed if no one holds it.
  // Since we don't automatically free these tables, we need to free tables when we destroy the database
  auto txn = txn_manager_->BeginTransaction();

  auto pg_class = GetDatabaseCatalog(oid, "pg_class");
  auto pg_class_ptr = pg_class->GetSqlTable();

  // save information needed for (later) reading and writing
  std::vector<col_oid_t> col_oids;
  for (const auto &c : pg_class_ptr->GetSchema().GetColumns()) {
    col_oids.emplace_back(c.GetOid());
  }
  auto col_pair = pg_class_ptr->InitializerForProjectedColumns(col_oids, 100);
  auto *buffer = common::AllocationUtil::AllocateAligned(col_pair.first.ProjectedColumnsSize());
  storage::ProjectedColumns *columns = col_pair.first.Initialize(buffer);
  storage::ProjectionMap col_map = col_pair.second;
  auto it = pg_class_ptr->begin();
  pg_class_ptr->Scan(txn, &it, columns);

  auto num_rows = columns->NumTuples();
  CATALOG_LOG_TRACE("We found {} rows in pg_class", num_rows);

  // Get the block layout
  auto layout = storage::StorageUtil::BlockLayoutFromSchema(pg_class_ptr->GetSchema()).first;
  // get the pg_catalog oid
  auto pg_catalog_oid = GetDatabaseHandle().GetNamespaceHandle(txn, oid).NameToOid(txn, "pg_catalog");
  for (uint32_t i = 0; i < num_rows; i++) {
    auto row = columns->InterpretAsRow(layout, i);
    byte *col_p = row.AccessForceNotNull(col_map.at(col_oids[3]));
    auto nsp_oid = *reinterpret_cast<uint32_t *>(col_p);
    if (nsp_oid != !pg_catalog_oid) {
      // user created tables, need to free them
      byte *addr_col = row.AccessForceNotNull(col_map.at(col_oids[0]));
      int64_t ptr = *reinterpret_cast<int64_t *>(addr_col);
      delete reinterpret_cast<SqlTableRW *>(ptr);
    }
  }
  delete[] buffer;
  delete txn;
}

// private methods

void Catalog::AddEntryToPGDatabase(transaction::TransactionContext *txn, db_oid_t oid, const char *name) {
  std::vector<type::Value> entry;
  entry.emplace_back(type::ValueFactory::GetIntegerValue(!oid));
  entry.emplace_back(type::ValueFactory::GetVarcharValue(name));
  SetUnusedColumns(&entry, pg_database_unused_cols_);
  pg_database_->InsertRow(txn, entry);

  // oid -> empty map (for tables)
  map_[oid] = std::unordered_map<table_oid_t, std::shared_ptr<catalog::SqlTableRW>>();
}

void Catalog::SetUnusedColumns(std::vector<type::Value> *vec, const std::vector<UnusedSchemaCols> &cols) {
  for (const auto col : cols) {
    switch (col.type_id) {
      case type::TypeId::BOOLEAN:
        vec->emplace_back(type::ValueFactory::GetBooleanValue(false));
        break;

      case type::TypeId::INTEGER:
        vec->emplace_back(type::ValueFactory::GetIntegerValue(0));
        break;

      case type::TypeId::VARCHAR:
        vec->emplace_back(type::ValueFactory::GetNullValue(type::TypeId::VARCHAR));
        break;

      default:
        throw NOT_IMPLEMENTED_EXCEPTION("unsupported type in SetUnusedSchemaColumns (by vec)");
    }
  }
}

}  // namespace terrier::catalog
