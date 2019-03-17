#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/attr_def_handle.h"
#include "catalog/catalog.h"
#include "catalog/class_handle.h"
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
  auto oid = db_entry->GetDatabaseOid();
  // remove entry from pg_database
  db_handle.DeleteEntry(txn, db_entry);

  // TODO(pakhtar):
  // - delete all the tables
  // - remove references from other catalog tables (pg_class)

  map_.erase(oid);
  name_map_.erase(oid);
}

void Catalog::CreateTable(transaction::TransactionContext *txn, db_oid_t db_oid, const std::string &table_name, catalog::Schema schema) {
  auto db_handle = GetDatabaseHandle();
  auto table_handle = db_handle.GetNamespaceHandle(txn, db_oid).GetTableHandle(txn, "public");

  // creates the storage table and adds to pg_class
  auto tbl_rw = std::shared_ptr<catalog::SqlTableRW>(table_handle.CreateTable(txn, schema, table_name));
  // auto tbl_rw = std::shared_ptr<catalog::SqlTableRW>(raw_tbl_rw);

  // add to maps
  AddToMaps(db_oid, tbl_rw->Oid(), table_name, tbl_rw);

  // enter attribute information
  // AddColumnsToPGAttribute(txn, db_oid, tbl_rw->GetSqlTable());
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
                                     const std::vector<SchemaCol> &cols) {
  for (const auto &col : cols) {
    db_p->DefineColumn(col.col_name, col.type_id, false, col_oid_t(GetNextOid()));
  }
}

void Catalog::AddColumnsToPGAttribute(transaction::TransactionContext *txn, db_oid_t db_oid,
                                      const std::shared_ptr<storage::SqlTable> &table) {
  Schema schema = table->GetSchema();
  std::vector<Schema::Column> cols = schema.GetColumns();
  std::shared_ptr<catalog::SqlTableRW> pg_attribute = map_[db_oid][name_map_[db_oid]["pg_attribute"]];
  int32_t col_num = 0;
  for (auto &c : cols) {
    std::vector<type::Value> row;
    row.emplace_back(type::ValueFactory::GetIntegerValue(!c.GetOid()));
    row.emplace_back(type::ValueFactory::GetIntegerValue(!table->Oid()));
    row.emplace_back(type::ValueFactory::GetVarcharValue(c.GetName().c_str()));

    // pg_type.oid
    // get a type handle
      auto type_handle = GetDatabaseHandle().GetTypeHandle(txn, db_oid);
      auto s_type = ValueTypeIdToSchemaType(c.GetType());
      auto type_entry = type_handle.GetTypeEntry(txn, s_type);

      row.emplace_back(type::ValueFactory::GetIntegerValue(!type_entry->GetTypeOid()));
//     row.emplace_back(type::ValueFactory::GetIntegerValue(0));

    // length of column type. Varlen columns have the sign bit set.
    // TODO(pakhtar): resolve what to store for varlens.
    auto attr_size = c.GetAttrSize();
    row.emplace_back(type::ValueFactory::GetIntegerValue(attr_size));

    // column number, starting from 1 for "real" columns.
    // The first column, 0, is terrier's row oid.
    row.emplace_back(type::ValueFactory::GetIntegerValue(col_num++));
    pg_attribute->InsertRow(txn, row);
  }
}

void Catalog::CreatePGDatabase(table_oid_t table_oid) {
  CATALOG_LOG_TRACE("Creating pg_database table");
  // set the oid
  pg_database_ = std::make_shared<catalog::SqlTableRW>(table_oid);

  // columns we use
  for (auto col : DatabaseHandle::schema_cols_) {
    pg_database_->DefineColumn(col.col_name, col.type_id, false, col_oid_t(GetNextOid()));
  }

  // columns we don't use
  for (auto col : DatabaseHandle::unused_schema_cols_) {
    pg_database_->DefineColumn(col.col_name, col.type_id, false, col_oid_t(GetNextOid()));
  }
  // create the table
  pg_database_->Create();
  db_oid_t terrier_oid = DEFAULT_DATABASE_OID;

  // add it to the map
  map_[terrier_oid] = std::unordered_map<table_oid_t, std::shared_ptr<catalog::SqlTableRW>>();
  // what about the name map?
}

void Catalog::PopulatePGDatabase(transaction::TransactionContext *txn) {
  std::vector<type::Value> row;
  db_oid_t terrier_oid = DEFAULT_DATABASE_OID;
  CATALOG_LOG_TRACE("Populate pg_database table");

  row.emplace_back(type::ValueFactory::GetIntegerValue(!terrier_oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue("terrier"));
  SetUnusedColumns(&row, DatabaseHandle::unused_schema_cols_);
  pg_database_->InsertRow(txn, row);
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
  CreatePGType(txn, db_oid);

  AttrDefHandle::Create(txn, this, db_oid, "pg_attrdef");

  // add columnn information into pg_attribute, for the catalog tables just created
  AddColumnsToPGAttribute(txn, db_oid, map_[db_oid][name_map_[db_oid]["pg_database"]]->GetSqlTable());
  AddColumnsToPGAttribute(txn, db_oid, map_[db_oid][name_map_[db_oid]["pg_tablespace"]]->GetSqlTable());
  AddColumnsToPGAttribute(txn, db_oid, map_[db_oid][name_map_[db_oid]["pg_attribute"]]->GetSqlTable());
  AddColumnsToPGAttribute(txn, db_oid, map_[db_oid][name_map_[db_oid]["pg_namespace"]]->GetSqlTable());
  AddColumnsToPGAttribute(txn, db_oid, map_[db_oid][name_map_[db_oid]["pg_class"]]->GetSqlTable());
  AddColumnsToPGAttribute(txn, db_oid, map_[db_oid][name_map_[db_oid]["pg_type"]]->GetSqlTable());
  AddColumnsToPGAttribute(txn, db_oid, map_[db_oid][name_map_[db_oid]["pg_attrdef"]]->GetSqlTable());
}

void Catalog::CreatePGAttribute(terrier::transaction::TransactionContext *txn, terrier::catalog::db_oid_t db_oid) {

  std::shared_ptr<catalog::SqlTableRW> pg_attribute = AttributeHandle::Create(txn, this, db_oid, "pg_attribute");

  // Insert columns of global catalogs
  // PA: this is probably the wrong place. If we want to use this function for any database,
  // we want to add the global table columns only once.
  // AddColumnsToPGAttribute(txn, db_oid, map_[db_oid][name_map_[db_oid]["pg_database"]]->GetSqlTable());
  // AddColumnsToPGAttribute(txn, db_oid, map_[db_oid][name_map_[db_oid]["pg_tablespace"]]->GetSqlTable());
}

void Catalog::CreatePGNameSpace(transaction::TransactionContext *txn, db_oid_t db_oid) {
  std::vector<type::Value> row;
  std::shared_ptr<catalog::SqlTableRW> pg_namespace;

  // create the namespace table
  pg_namespace = NamespaceHandle::Create(txn, this, db_oid, "pg_namespace");

  auto ns_handle = GetDatabaseHandle().GetNamespaceHandle(txn, db_oid);

  // populate it
  ns_handle.AddEntry(txn, "pg_catalog");
  ns_handle.AddEntry(txn, "public");
}

void Catalog::CreatePGClass(transaction::TransactionContext *txn, db_oid_t db_oid) {
  std::vector<type::Value> row;

  // create pg_class storage
  std::shared_ptr<catalog::SqlTableRW> pg_class = ClassHandle::Create(txn, this, db_oid, "pg_class");

  auto class_handle = GetDatabaseHandle().GetClassHandle(txn, db_oid);

  // lookup oids inserted in multiple entries
  auto pg_catalog_namespace_oid =
      !GetDatabaseHandle().GetNamespaceHandle(txn, db_oid).GetNamespaceEntry(txn, "pg_catalog")->GetNamespaceOid();

  auto pg_global_ts_oid = !GetTablespaceHandle().GetTablespaceEntry(txn, "pg_global")->GetTablespaceOid();
  auto pg_default_ts_oid = !GetTablespaceHandle().GetTablespaceEntry(txn, "pg_default")->GetTablespaceOid();

  // Insert pg_database
  // (namespace: catalog, tablespace: global)
  CATALOG_LOG_TRACE("Inserting pg_database into pg_class ...");
  auto pg_db_tbl_p = reinterpret_cast<uint64_t>(GetDatabaseCatalog(db_oid, "pg_database").get());
  auto pg_database_entry_oid = !GetDatabaseCatalog(db_oid, "pg_database")->Oid();

  class_handle.AddEntry(txn, pg_db_tbl_p, pg_database_entry_oid, "pg_database", pg_catalog_namespace_oid,
                        pg_global_ts_oid);

  // Insert pg_tablespace
  // (namespace: catalog, tablespace: global)
  CATALOG_LOG_TRACE("Inserting pg_tablespace into pg_class ...");
  auto pg_ts_tbl_p = reinterpret_cast<uint64_t>(GetDatabaseCatalog(db_oid, "pg_tablespace").get());
  auto pg_tablespace_entry_oid = !GetDatabaseCatalog(db_oid, "pg_tablespace")->Oid();

  class_handle.AddEntry(txn, pg_ts_tbl_p, pg_tablespace_entry_oid, "pg_tablespace", pg_catalog_namespace_oid,
                        pg_global_ts_oid);

  // Insert pg_namespace
  // (namespace: catalog, tablespace: default)
  CATALOG_LOG_TRACE("Inserting pg_namespace into pg_class ...");
  auto pg_ns_tbl_p = reinterpret_cast<uint64_t>(GetDatabaseCatalog(db_oid, "pg_namespace").get());
  auto pg_ns_entry_oid = !GetDatabaseCatalog(db_oid, "pg_namespace")->Oid();

  class_handle.AddEntry(txn, pg_ns_tbl_p, pg_ns_entry_oid, "pg_namespace", pg_catalog_namespace_oid, pg_default_ts_oid);

  // Insert pg_class
  // (namespace: catalog, tablespace: default)
  auto pg_cls_tbl_p = reinterpret_cast<uint64_t>(GetDatabaseCatalog(db_oid, "pg_class").get());
  auto pg_cls_entry_oid = !GetDatabaseCatalog(db_oid, "pg_class")->Oid();

  class_handle.AddEntry(txn, pg_cls_tbl_p, pg_cls_entry_oid, "pg_class", pg_catalog_namespace_oid, pg_default_ts_oid);

  // Insert pg_attribute
  // (namespace: catalog, tablespace: default)
  auto pg_attr_tbl_p = reinterpret_cast<uint64_t>(GetDatabaseCatalog(db_oid, "pg_attribute").get());
  auto pg_attr_entry_oid = !GetDatabaseCatalog(db_oid, "pg_attribute")->Oid();

  class_handle.AddEntry(txn, pg_attr_tbl_p, pg_attr_entry_oid, "pg_attribute", pg_catalog_namespace_oid,
                        pg_default_ts_oid);
}

void Catalog::CreatePGType(transaction::TransactionContext *txn, db_oid_t db_oid) {
  table_oid_t pg_type_oid(GetNextOid());
  std::shared_ptr<catalog::SqlTableRW> pg_type;
  pg_type = std::make_shared<catalog::SqlTableRW>(pg_type_oid);

  // define pg_type schema
  pg_type->DefineColumn("oid", type::TypeId::INTEGER, false, col_oid_t(GetNextOid()));
  pg_type->DefineColumn("typname", type::TypeId::VARCHAR, false, col_oid_t(GetNextOid()));
  pg_type->DefineColumn("typnamespace", type::TypeId::INTEGER, false, col_oid_t(GetNextOid()));
  pg_type->DefineColumn("typlen", type::TypeId::SMALLINT, false, col_oid_t(GetNextOid()));
  pg_type->DefineColumn("typtype", type::TypeId::VARCHAR, false, col_oid_t(GetNextOid()));
  AddUnusedSchemaColumns(pg_type, pg_type_unused_cols);
  pg_type->Create();

  // add to the catalog map
  map_[db_oid][pg_type_oid] = pg_type;
  name_map_[db_oid]["pg_type"] = pg_type_oid;

  CATALOG_LOG_TRACE("Inserting built-in types to pg_type ...", !pg_type_oid);
  std::vector<type::Value> row;
  auto catalog_ns_oid =
      GetDatabaseHandle().GetNamespaceHandle(txn, db_oid).GetNamespaceEntry(txn, "pg_catalog")->GetNamespaceOid();
  type_oid_t oid;

  // TODO(yeshengm): separate the generation of built-in types to another method
  // insert boolean type
  oid = type_oid_t(GetNextOid());
  row.clear();
  row.emplace_back(type::ValueFactory::GetIntegerValue(!oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue("boolean"));
  row.emplace_back(type::ValueFactory::GetIntegerValue(!catalog_ns_oid));
  row.emplace_back(type::ValueFactory::GetIntegerValue(type::TypeUtil::GetTypeSize(type::TypeId::BOOLEAN)));
  row.emplace_back(type::ValueFactory::GetVarcharValue("b"));
  SetUnusedColumns(&row, pg_type_unused_cols);
  pg_type->InsertRow(txn, row);

  // insert tinyint type
  oid = type_oid_t(GetNextOid());
  row.clear();
  row.emplace_back(type::ValueFactory::GetIntegerValue(!oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue("tinyint"));
  row.emplace_back(type::ValueFactory::GetIntegerValue(!catalog_ns_oid));
  row.emplace_back(type::ValueFactory::GetIntegerValue(type::TypeUtil::GetTypeSize(type::TypeId::TINYINT)));
  row.emplace_back(type::ValueFactory::GetVarcharValue("b"));
  SetUnusedColumns(&row, pg_type_unused_cols);
  pg_type->InsertRow(txn, row);

  // insert smallint type
  oid = type_oid_t(GetNextOid());
  row.clear();
  row.emplace_back(type::ValueFactory::GetIntegerValue(!oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue("smallint"));
  row.emplace_back(type::ValueFactory::GetIntegerValue(!catalog_ns_oid));
  row.emplace_back(type::ValueFactory::GetIntegerValue(type::TypeUtil::GetTypeSize(type::TypeId::SMALLINT)));
  row.emplace_back(type::ValueFactory::GetVarcharValue("b"));
  SetUnusedColumns(&row, pg_type_unused_cols);
  pg_type->InsertRow(txn, row);

  // insert integer type
  oid = type_oid_t(GetNextOid());
  row.clear();
  row.emplace_back(type::ValueFactory::GetIntegerValue(!oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue("integer"));
  row.emplace_back(type::ValueFactory::GetIntegerValue(!catalog_ns_oid));
  row.emplace_back(type::ValueFactory::GetIntegerValue(type::TypeUtil::GetTypeSize(type::TypeId::INTEGER)));
  row.emplace_back(type::ValueFactory::GetVarcharValue("b"));
  SetUnusedColumns(&row, pg_type_unused_cols);
  pg_type->InsertRow(txn, row);

  // insert date type
  oid = type_oid_t(GetNextOid());
  row.clear();
  row.emplace_back(type::ValueFactory::GetIntegerValue(!oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue("date"));
  row.emplace_back(type::ValueFactory::GetIntegerValue(!catalog_ns_oid));
  row.emplace_back(type::ValueFactory::GetIntegerValue(type::TypeUtil::GetTypeSize(type::TypeId::DATE)));
  row.emplace_back(type::ValueFactory::GetVarcharValue("b"));
  SetUnusedColumns(&row, pg_type_unused_cols);
  pg_type->InsertRow(txn, row);

  // insert bigint type
  oid = type_oid_t(GetNextOid());
  row.clear();
  row.emplace_back(type::ValueFactory::GetIntegerValue(!oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue("bigint"));
  row.emplace_back(type::ValueFactory::GetIntegerValue(!catalog_ns_oid));
  row.emplace_back(type::ValueFactory::GetIntegerValue(type::TypeUtil::GetTypeSize(type::TypeId::BIGINT)));
  row.emplace_back(type::ValueFactory::GetVarcharValue("b"));
  SetUnusedColumns(&row, pg_type_unused_cols);
  pg_type->InsertRow(txn, row);

  // insert decimal type
  oid = type_oid_t(GetNextOid());
  row.clear();
  row.emplace_back(type::ValueFactory::GetIntegerValue(!oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue("decimal"));
  row.emplace_back(type::ValueFactory::GetIntegerValue(!catalog_ns_oid));
  row.emplace_back(type::ValueFactory::GetIntegerValue(type::TypeUtil::GetTypeSize(type::TypeId::DECIMAL)));
  row.emplace_back(type::ValueFactory::GetVarcharValue("b"));
  SetUnusedColumns(&row, pg_type_unused_cols);
  pg_type->InsertRow(txn, row);

  // insert timestamp type
  oid = type_oid_t(GetNextOid());
  row.clear();
  row.emplace_back(type::ValueFactory::GetIntegerValue(!oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue("timestamp"));
  row.emplace_back(type::ValueFactory::GetIntegerValue(!catalog_ns_oid));
  row.emplace_back(type::ValueFactory::GetIntegerValue(type::TypeUtil::GetTypeSize(type::TypeId::TIMESTAMP)));
  row.emplace_back(type::ValueFactory::GetVarcharValue("b"));
  SetUnusedColumns(&row, pg_type_unused_cols);
  pg_type->InsertRow(txn, row);

  // insert varchar type
  oid = type_oid_t(GetNextOid());
  row.clear();
  row.emplace_back(type::ValueFactory::GetIntegerValue(!oid));
  row.emplace_back(type::ValueFactory::GetVarcharValue("varchar"));
  row.emplace_back(type::ValueFactory::GetIntegerValue(!catalog_ns_oid));
  row.emplace_back(type::ValueFactory::GetIntegerValue(-1));
  row.emplace_back(type::ValueFactory::GetVarcharValue("b"));
  SetUnusedColumns(&row, pg_type_unused_cols);
  pg_type->InsertRow(txn, row);
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
  SetUnusedColumns(&entry, DatabaseHandle::unused_schema_cols_);
  pg_database_->InsertRow(txn, entry);

  // oid -> empty map (for tables)
  map_[oid] = std::unordered_map<table_oid_t, std::shared_ptr<catalog::SqlTableRW>>();
}

void Catalog::SetUnusedColumns(std::vector<type::Value> *vec, const std::vector<SchemaCol> &cols) {
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

type::Value Catalog::ValueTypeIdToSchemaType(type::TypeId type_id) {
  switch (type_id) {
    case type::TypeId::BOOLEAN:
      return  type::ValueFactory::GetVarcharValue("boolean");

    case type::TypeId::TINYINT:
      return  type::ValueFactory::GetVarcharValue("tinyint");

    case type::TypeId::SMALLINT:
      return  type::ValueFactory::GetVarcharValue("smallint");

    case type::TypeId::INTEGER:
      return  type::ValueFactory::GetVarcharValue("integer");

    case type::TypeId::BIGINT:
      return  type::ValueFactory::GetVarcharValue("bigint");

    case type::TypeId::DATE:
      return  type::ValueFactory::GetVarcharValue("date");

    case type::TypeId::DECIMAL:
      return  type::ValueFactory::GetVarcharValue("decimal");

    case type::TypeId::TIMESTAMP:
      return  type::ValueFactory::GetVarcharValue("timestamp");

    case type::TypeId::VARCHAR:
      return  type::ValueFactory::GetVarcharValue("varchar");

    default:
      throw NOT_IMPLEMENTED_EXCEPTION("unsupported type in ValueToSchemaType");
  }
}

void Catalog::Dump(transaction::TransactionContext *txn) {
  // TODO(pakhtar): add parameter to select database

  // dump pg_database
  auto db_handle = GetDatabaseHandle();
  CATALOG_LOG_DEBUG("-- pg_database -- ");
  db_handle.Dump(txn);

  CATALOG_LOG_DEBUG("");
  CATALOG_LOG_DEBUG("-- pg_namespace -- ");
  db_oid_t terrier_oid = DEFAULT_DATABASE_OID;
  auto ns_handle = db_handle.GetNamespaceHandle(txn, terrier_oid);
  ns_handle.Dump(txn);

  // pg_attribute
  CATALOG_LOG_DEBUG("");
  CATALOG_LOG_DEBUG("-- pg_attribute -- ");
  auto attr_handle = db_handle.GetAttributeHandle(txn, terrier_oid);
  attr_handle.Dump(txn);

  // pg_type
  CATALOG_LOG_DEBUG("");
  CATALOG_LOG_DEBUG("-- pg_type -- ");
  auto type_handle = db_handle.GetTypeHandle(txn, terrier_oid);
  type_handle.Dump(txn);

  // pg_class
  CATALOG_LOG_DEBUG("");
  CATALOG_LOG_DEBUG("-- pg_class -- ");
  auto cls_handle = db_handle.GetClassHandle(txn, terrier_oid);
  cls_handle.Dump(txn);
}

}  // namespace terrier::catalog
