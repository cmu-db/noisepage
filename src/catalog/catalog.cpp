#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/attr_def_handle.h"
#include "catalog/catalog.h"
#include "catalog/class_handle.h"
#include "catalog/database_handle.h"
#include "catalog/settings_handle.h"
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

void Catalog::CreateDatabase(transaction::TransactionContext *txn, const std::string &name) {
  db_oid_t new_db_oid = db_oid_t(GetNextOid());
  Catalog::AddEntryToPGDatabase(txn, new_db_oid, name);
  BootstrapDatabase(txn, new_db_oid);
}

void Catalog::DeleteDatabase(transaction::TransactionContext *txn, const std::string &db_name) {
  // get database handle
  auto db_handle = GetDatabaseHandle();
  auto db_entry = db_handle.GetDatabaseEntry(txn, db_name);
  auto oid = db_entry->GetOid();
  // remove entry from pg_database
  db_handle.DeleteEntry(txn, db_entry);

  // TODO(pakhtar):
  // - delete all the tables

  // drop database local tables
  // - pg_attribute
  // - pg_namespace
  // - pg_class
  // - pg_type
  // - pg_attrdef

  map_.erase(oid);
  name_map_.erase(oid);
}

table_oid_t Catalog::CreateTable(transaction::TransactionContext *txn, db_oid_t db_oid, const std::string &table_name,
                                 const Schema &schema) {
  auto db_handle = GetDatabaseHandle();
  auto table_handle = db_handle.GetNamespaceHandle(txn, db_oid).GetTableHandle(txn, "public");

  // creates the storage table and adds to pg_class
  auto tbl_rw = std::shared_ptr<catalog::SqlTableRW>(table_handle.CreateTable(txn, schema, table_name));
  // auto tbl_rw = std::shared_ptr<catalog::SqlTableRW>(raw_tbl_rw);

  // add to maps
  AddToMaps(db_oid, tbl_rw->Oid(), table_name, tbl_rw);

  // enter attribute information
  AddColumnsToPGAttribute(txn, db_oid, tbl_rw->GetSqlTable());
  return tbl_rw->Oid();
}

void Catalog::DeleteTable(transaction::TransactionContext *txn, db_oid_t db_oid, table_oid_t table_oid) {
  auto db_handle = GetDatabaseHandle();

  // remove entries from pg_attribute, if attrelid == table_oid
  auto attr_handle = db_handle.GetAttributeHandle(txn, db_oid);
  auto attr_table = GetDatabaseCatalog(db_oid, "pg_attribute");
  int32_t col_index = attr_table->ColNameToIndex("attrelid");
  auto it = attr_table->begin(txn);
  while (it != attr_table->end(txn)) {
    auto layout = attr_table->GetLayout();
    storage::ProjectedColumns::RowView row_view = it->InterpretAsRow(layout, 0);
    // check if a matching row, delete if it is
    byte *col_p = row_view.AccessWithNullCheck(attr_table->ColNumToOffset(col_index));
    if (col_p == nullptr) {
      continue;
    }
    auto col_int_value = *(reinterpret_cast<int32_t *>(col_p));
    if (static_cast<uint32_t>(col_int_value) == !table_oid) {
      // delete the entry
      attr_table->GetSqlTable()->Delete(txn, *(it->TupleSlots()));
    }
    ++it;
  }

  // remove entries from pg_attrdef
  // adrelid == table oid (i.e. pg_class.oid).
  auto attrdef_handle = db_handle.GetAttrDefHandle(txn, db_oid);
  auto attrdef_table = GetDatabaseCatalog(db_oid, "pg_attrdef");
  col_index = attrdef_table->ColNameToIndex("adrelid");
  auto attrdef_it = attrdef_table->begin(txn);
  while (attrdef_it != attrdef_table->end(txn)) {
    auto layout = attrdef_table->GetLayout();
    storage::ProjectedColumns::RowView row_view = attrdef_it->InterpretAsRow(layout, 0);
    // check if a matching row, delete if it is
    byte *col_p = row_view.AccessWithNullCheck(attrdef_table->ColNumToOffset(col_index));
    if (col_p == nullptr) {
      continue;
    }
    auto col_int_value = *(reinterpret_cast<int32_t *>(col_p));
    if (static_cast<uint32_t>(col_int_value) == !table_oid) {
      // delete the entry
      attrdef_table->GetSqlTable()->Delete(txn, *(attrdef_it->TupleSlots()));
    }
    ++attrdef_it;
  }

  // remove entries from pg_class
  // oid is col 0
  auto class_handle = db_handle.GetClassHandle(txn, db_oid);
  auto class_table = GetDatabaseCatalog(db_oid, "pg_class");
  col_index = class_table->ColNameToIndex("oid");
  auto class_it = class_table->begin(txn);
  while (class_it != class_table->end(txn)) {
    auto layout = class_table->GetLayout();
    storage::ProjectedColumns::RowView row_view = class_it->InterpretAsRow(layout, 0);
    // check if a matching row, delete if it is
    byte *col_p = row_view.AccessWithNullCheck(class_table->ColNumToOffset(col_index));
    if (col_p == nullptr) {
      continue;
    }
    auto col_int_value = *(reinterpret_cast<int32_t *>(col_p));
    if (static_cast<uint32_t>(col_int_value) == !table_oid) {
      // delete the entry
      class_table->GetSqlTable()->Delete(txn, *(class_it->TupleSlots()));
      // there is just the one, stop
      break;
    }
    ++class_it;
  }

  // TODO(pakhtar): drop table
}

DatabaseHandle Catalog::GetDatabaseHandle() { return DatabaseHandle(this, pg_database_); }

TablespaceHandle Catalog::GetTablespaceHandle() { return TablespaceHandle(this, pg_tablespace_); }

SettingsHandle Catalog::GetSettingsHandle() { return SettingsHandle(pg_settings_); }

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

  CreatePGTablespace(DEFAULT_DATABASE_OID, table_oid_t(GetNextOid()));
  PopulatePGTablespace(txn);

  pg_settings_ = SettingsHandle::Create(txn, this, DEFAULT_DATABASE_OID, "pg_settings");

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
    std::vector<type::TransientValue> row;
    row.emplace_back(type::TransientValueFactory::GetInteger(!c.GetOid()));
    row.emplace_back(type::TransientValueFactory::GetInteger(!table->Oid()));
    row.emplace_back(type::TransientValueFactory::GetVarChar(c.GetName()));

    // pg_type.oid
    auto type_handle = GetDatabaseHandle().GetTypeHandle(txn, db_oid);
    auto s_type = ValueTypeIdToSchemaType(c.GetType());
    auto type_entry = type_handle.GetTypeEntry(txn, s_type);
    row.emplace_back(type::TransientValueFactory::GetInteger(!type_entry->GetOid()));

    // length of column type. Varlen columns have the sign bit set.
    // TODO(pakhtar): resolve what to store for varlens.
    auto attr_size = c.GetAttrSize();
    row.emplace_back(type::TransientValueFactory::GetInteger(attr_size));

    // column number, starting from 1 for "real" columns.
    // The first column, 0, is terrier's row oid.
    row.emplace_back(type::TransientValueFactory::GetInteger(col_num++));
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
  db_oid_t default_db_oid = DEFAULT_DATABASE_OID;

  // add it to the map
  map_[default_db_oid] = std::unordered_map<table_oid_t, std::shared_ptr<catalog::SqlTableRW>>();
  // what about the name map?
}

void Catalog::PopulatePGDatabase(transaction::TransactionContext *txn) {
  std::vector<type::TransientValue> row;
  db_oid_t terrier_oid = DEFAULT_DATABASE_OID;
  CATALOG_LOG_TRACE("Populate pg_database table");

  row.emplace_back(type::TransientValueFactory::GetInteger(!terrier_oid));
  row.emplace_back(type::TransientValueFactory::GetVarChar("terrier"));
  SetUnusedColumns(&row, DatabaseHandle::unused_schema_cols_);
  pg_database_->InsertRow(txn, row);
}

void Catalog::CreatePGTablespace(db_oid_t db_oid, table_oid_t table_oid) {
  CATALOG_LOG_TRACE("Creating pg_tablespace table");
  pg_tablespace_ = TablespaceHandle::Create(this, db_oid, "pg_tablespace");
}

void Catalog::PopulatePGTablespace(transaction::TransactionContext *txn) {
  std::vector<type::TransientValue> row;
  CATALOG_LOG_TRACE("Populate pg_tablespace table");
  auto ts_handle = GetTablespaceHandle();

  ts_handle.AddEntry(txn, "pg_global");
  ts_handle.AddEntry(txn, "pg_default");
}

void Catalog::BootstrapDatabase(transaction::TransactionContext *txn, db_oid_t db_oid) {
  CATALOG_LOG_TRACE("Bootstrapping database oid (db_oid) {}", !db_oid);
  map_[db_oid][pg_database_->Oid()] = pg_database_;
  map_[db_oid][pg_tablespace_->Oid()] = pg_tablespace_;
  map_[db_oid][pg_settings_->Oid()] = pg_settings_;
  name_map_[db_oid]["pg_database"] = pg_database_->Oid();
  name_map_[db_oid]["pg_tablespace"] = pg_tablespace_->Oid();
  name_map_[db_oid]["pg_settings"] = pg_settings_->Oid();

  // Order: pg_attribute -> pg_namespace -> pg_class
  CreatePGAttribute(txn, db_oid);
  CreatePGNamespace(txn, db_oid);
  CreatePGClass(txn, db_oid);
  CreatePGType(txn, db_oid);
  CreatePGAttrDef(txn, db_oid);

  // add column information into pg_attribute, for the catalog tables just created
  // pg_database, pg_tablespace and pg_settings are global, but
  // pg_attribute is local, so add them too.
  std::vector<std::string> c_tables = {"pg_database", "pg_tablespace", "pg_attribute", "pg_namespace",
                                       "pg_class",    "pg_type",       "pg_attrdef",   "pg_settings"};
  auto add_cols_to_pg_attr = [this, txn, db_oid](const std::string &st) {
    AddColumnsToPGAttribute(txn, db_oid, map_[db_oid][name_map_[db_oid][st]]->GetSqlTable());
  };
  std::for_each(c_tables.begin(), c_tables.end(), add_cols_to_pg_attr);
}

void Catalog::CreatePGAttribute(terrier::transaction::TransactionContext *txn, terrier::catalog::db_oid_t db_oid) {
  std::shared_ptr<catalog::SqlTableRW> pg_attribute = AttributeHandle::Create(txn, this, db_oid, "pg_attribute");
}

void Catalog::CreatePGAttrDef(transaction::TransactionContext *txn, db_oid_t db_oid) {
  std::shared_ptr<catalog::SqlTableRW> pg_attrdef = AttrDefHandle::Create(txn, this, db_oid, "pg_attrdef");
}

void Catalog::CreatePGNamespace(transaction::TransactionContext *txn, db_oid_t db_oid) {
  std::vector<type::TransientValue> row;
  std::shared_ptr<catalog::SqlTableRW> pg_namespace;

  // create the namespace table
  pg_namespace = NamespaceHandle::Create(txn, this, db_oid, "pg_namespace");

  auto ns_handle = GetDatabaseHandle().GetNamespaceHandle(txn, db_oid);

  // populate it
  ns_handle.AddEntry(txn, "pg_catalog");
  ns_handle.AddEntry(txn, "public");
}

void Catalog::CreatePGClass(transaction::TransactionContext *txn, db_oid_t db_oid) {
  std::vector<type::TransientValue> row;

  // create pg_class storage
  std::shared_ptr<catalog::SqlTableRW> pg_class = ClassHandle::Create(txn, this, db_oid, "pg_class");

  auto class_handle = GetDatabaseHandle().GetClassHandle(txn, db_oid);

  // lookup oids inserted in multiple entries
  auto pg_catalog_namespace_oid =
      !GetDatabaseHandle().GetNamespaceHandle(txn, db_oid).GetNamespaceEntry(txn, "pg_catalog")->GetOid();

  auto pg_global_ts_oid = !GetTablespaceHandle().GetTablespaceEntry(txn, "pg_global")->GetOid();
  auto pg_default_ts_oid = !GetTablespaceHandle().GetTablespaceEntry(txn, "pg_default")->GetOid();

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
  std::shared_ptr<SqlTableRW> pg_type = TypeHandle::Create(txn, this, db_oid, "pg_type");

  std::vector<type::TransientValue> row;
  // TODO(Yesheng): get rid of this strange calling chain
  auto pg_type_handle = GetDatabaseHandle().GetTypeHandle(txn, db_oid);
  auto catalog_ns_oid =
      GetDatabaseHandle().GetNamespaceHandle(txn, db_oid).GetNamespaceEntry(txn, "pg_catalog")->GetOid();

  // built-in types as in type/type_id.h
  pg_type_handle.AddEntry(txn, type_oid_t(GetNextOid()), "boolean", catalog_ns_oid,
                          type::TypeUtil::GetTypeSize(type::TypeId::BOOLEAN), "b");
  pg_type_handle.AddEntry(txn, type_oid_t(GetNextOid()), "tinyint", catalog_ns_oid,
                          type::TypeUtil::GetTypeSize(type::TypeId::TINYINT), "b");
  pg_type_handle.AddEntry(txn, type_oid_t(GetNextOid()), "smallint", catalog_ns_oid,
                          type::TypeUtil::GetTypeSize(type::TypeId::SMALLINT), "b");
  pg_type_handle.AddEntry(txn, type_oid_t(GetNextOid()), "integer", catalog_ns_oid,
                          type::TypeUtil::GetTypeSize(type::TypeId::INTEGER), "b");
  pg_type_handle.AddEntry(txn, type_oid_t(GetNextOid()), "date", catalog_ns_oid,
                          type::TypeUtil::GetTypeSize(type::TypeId::DATE), "b");
  pg_type_handle.AddEntry(txn, type_oid_t(GetNextOid()), "bigint", catalog_ns_oid,
                          type::TypeUtil::GetTypeSize(type::TypeId::BIGINT), "b");
  pg_type_handle.AddEntry(txn, type_oid_t(GetNextOid()), "decimal", catalog_ns_oid,
                          type::TypeUtil::GetTypeSize(type::TypeId::DECIMAL), "b");
  pg_type_handle.AddEntry(txn, type_oid_t(GetNextOid()), "timestamp", catalog_ns_oid,
                          type::TypeUtil::GetTypeSize(type::TypeId::TIMESTAMP), "b");
  pg_type_handle.AddEntry(txn, type_oid_t(GetNextOid()), "varchar", catalog_ns_oid, -1, "b");
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

void Catalog::AddEntryToPGDatabase(transaction::TransactionContext *txn, db_oid_t oid, const std::string &name) {
  std::vector<type::TransientValue> entry;
  entry.emplace_back(type::TransientValueFactory::GetInteger(!oid));
  entry.emplace_back(type::TransientValueFactory::GetVarChar(name));
  SetUnusedColumns(&entry, DatabaseHandle::unused_schema_cols_);
  pg_database_->InsertRow(txn, entry);

  // oid -> empty map (for tables)
  map_[oid] = std::unordered_map<table_oid_t, std::shared_ptr<catalog::SqlTableRW>>();
}

void Catalog::SetUnusedColumns(std::vector<type::TransientValue> *vec, const std::vector<SchemaCol> &cols) {
  for (const auto col : cols) {
    switch (col.type_id) {
      case type::TypeId::BOOLEAN:
        vec->emplace_back(type::TransientValueFactory::GetBoolean(false));
        break;

      case type::TypeId::TINYINT:
        vec->emplace_back(type::TransientValueFactory::GetTinyInt(0));

      case type::TypeId::SMALLINT:
        vec->emplace_back(type::TransientValueFactory::GetSmallInt(0));

      case type::TypeId::INTEGER:
        vec->emplace_back(type::TransientValueFactory::GetInteger(0));
        break;

      case type::TypeId::BIGINT:
        vec->emplace_back(type::TransientValueFactory::GetBigInt(0));

      case type::TypeId::DATE:
        vec->emplace_back(type::TransientValueFactory::GetDate(type::date_t(0)));

      case type::TypeId::DECIMAL:
        vec->emplace_back(type::TransientValueFactory::GetDecimal(0));

      case type::TypeId::TIMESTAMP:
        vec->emplace_back(type::TransientValueFactory::GetTimestamp(type::timestamp_t(0)));

      case type::TypeId::VARCHAR:
        vec->emplace_back(type::TransientValueFactory::GetNull(type::TypeId::VARCHAR));
        break;

      default:
        throw NOT_IMPLEMENTED_EXCEPTION("unsupported type in SetUnusedSchemaColumns (by vec)");
    }
  }
}

type::TransientValue Catalog::ValueTypeIdToSchemaType(type::TypeId type_id) {
  switch (type_id) {
    case type::TypeId::BOOLEAN:
      return type::TransientValueFactory::GetVarChar("boolean");

    case type::TypeId::TINYINT:
      return type::TransientValueFactory::GetVarChar("tinyint");

    case type::TypeId::SMALLINT:
      return type::TransientValueFactory::GetVarChar("smallint");

    case type::TypeId::INTEGER:
      return type::TransientValueFactory::GetVarChar("integer");

    case type::TypeId::BIGINT:
      return type::TransientValueFactory::GetVarChar("bigint");

    case type::TypeId::DATE:
      return type::TransientValueFactory::GetVarChar("date");

    case type::TypeId::DECIMAL:
      return type::TransientValueFactory::GetVarChar("decimal");

    case type::TypeId::TIMESTAMP:
      return type::TransientValueFactory::GetVarChar("timestamp");

    case type::TypeId::VARCHAR:
      return type::TransientValueFactory::GetVarChar("varchar");

    default:
      throw NOT_IMPLEMENTED_EXCEPTION("unsupported type in ValueToSchemaType");
  }
}

void Catalog::Dump(transaction::TransactionContext *txn, const db_oid_t db_oid) {
  // TODO(pakhtar): add parameter to select database

  // dump pg_database
  auto db_handle = GetDatabaseHandle();
  CATALOG_LOG_DEBUG("Dumping: {}", !db_oid);
  CATALOG_LOG_DEBUG("-- pg_database -- ");
  db_handle.Dump(txn);

  CATALOG_LOG_DEBUG("");
  CATALOG_LOG_DEBUG("-- pg_namespace -- ");
  auto ns_handle = db_handle.GetNamespaceHandle(txn, db_oid);
  ns_handle.Dump(txn);

  // pg_attribute
  CATALOG_LOG_DEBUG("");
  CATALOG_LOG_DEBUG("-- pg_attribute -- ");
  auto attr_handle = db_handle.GetAttributeHandle(txn, db_oid);
  attr_handle.Dump(txn);

  // pg_type
  CATALOG_LOG_DEBUG("");
  CATALOG_LOG_DEBUG("-- pg_type -- ");
  auto type_handle = db_handle.GetTypeHandle(txn, db_oid);
  type_handle.Dump(txn);

  // pg_class
  CATALOG_LOG_DEBUG("");
  CATALOG_LOG_DEBUG("-- pg_class -- ");
  auto cls_handle = db_handle.GetClassHandle(txn, db_oid);
  cls_handle.Dump(txn);
}

}  // namespace terrier::catalog
