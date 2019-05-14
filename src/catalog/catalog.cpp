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

Catalog::Catalog(transaction::TransactionManager *txn_manager, transaction::TransactionContext *txn)
    : txn_manager_(txn_manager), oid_(START_OID) {
  CATALOG_LOG_TRACE("Creating catalog ...");
  Bootstrap(txn);
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

  // TODO(pakhtar): delete all user tables
  // destroy all the non-global tables
  // this should become just catalog tables
  DeleteDatabaseTables(oid);

  // delete from the map
  cat_map_.erase(oid);
}

namespace_oid_t Catalog::CreateNameSpace(transaction::TransactionContext *txn, db_oid_t db_oid,
                                         const std::string &name) {
  auto db_handle = GetDatabaseHandle();
  auto ns_handle = db_handle.GetNamespaceTable(txn, db_oid);
  auto ns_entry = ns_handle.GetNamespaceEntry(txn, name);
  if (ns_entry == nullptr) {
    ns_handle.AddEntry(txn, name);
    ns_entry = ns_handle.GetNamespaceEntry(txn, name);
  }
  int32_t ns_oid_int = ns_entry->GetIntegerColumn("oid");
  return namespace_oid_t(ns_oid_int);
}

void Catalog::DeleteNameSpace(transaction::TransactionContext *txn, db_oid_t db_oid, namespace_oid_t ns_oid) {
  auto db_handle = GetDatabaseHandle();
  auto ns_handle = db_handle.GetNamespaceTable(txn, db_oid);

  auto ns_entry = ns_handle.GetNamespaceEntry(txn, ns_oid);
  if (ns_entry == nullptr) {
    return;
  }
  ns_handle.DeleteEntry(txn, ns_entry);
}

table_oid_t Catalog::CreateUserTable(transaction::TransactionContext *txn, db_oid_t db_oid, namespace_oid_t ns_oid,
                                     const std::string &table_name, const Schema &schema) {
  auto db_handle = GetDatabaseHandle();
  auto ns_handle = db_handle.GetNamespaceTable(txn, db_oid);
  auto table_handle = ns_handle.GetTableHandle(txn, ns_oid);

  // creates the storage table and adds to pg_class
  auto tbl_rw = table_handle.CreateTable(txn, schema, table_name);

  // ct_map_ is for system tables only, so user tables are not added to it

  // enter attribute information
  AddColumnsToPGAttribute(txn, db_oid, tbl_rw->GetSqlTable());
  return tbl_rw->Oid();
}

void Catalog::DeleteUserTable(transaction::TransactionContext *txn, db_oid_t db_oid, namespace_oid_t ns_oid,
                              const std::string &table_name) {
  // convert table name to table_oid
  auto user_tbl_p = GetUserTable(txn, db_oid, ns_oid, table_name);
  auto user_tbl_oid = user_tbl_p->Oid();

  DeleteUserTable(txn, db_oid, ns_oid, user_tbl_oid);
}

void Catalog::DeleteUserTable(transaction::TransactionContext *txn, db_oid_t db_oid, namespace_oid_t ns_oid,
                              table_oid_t tbl_oid) {
  auto db_handle = GetDatabaseHandle();
  auto attr_handle = db_handle.GetAttributeTable(txn, db_oid);
  auto attrdef_handle = db_handle.GetAttrDefTable(txn, db_oid);
  auto class_handle = db_handle.GetClassTable(txn, db_oid);

  // get an attribute handle
  attr_handle.DeleteEntries(txn, tbl_oid);

  // get an attr_def handle
  attrdef_handle.DeleteEntries(txn, tbl_oid);

  // delete from pg_class
  auto col_oid = col_oid_t(!tbl_oid);
  class_handle.DeleteEntry(txn, ns_oid, col_oid);
}

DatabaseCatalogTable Catalog::GetDatabaseHandle() { return DatabaseCatalogTable(this, pg_database_); }

TablespaceCatalogTable Catalog::GetTablespaceHandle() { return TablespaceCatalogTable(this, pg_tablespace_); }

SettingsCatalogTable Catalog::GetSettingsHandle() { return SettingsCatalogTable(pg_settings_); }

SqlTableHelper *Catalog::GetCatalogTable(db_oid_t db_oid, CatalogTableType cttype) {
  return cat_map_.at(db_oid).at(cttype);
}

SqlTableHelper *Catalog::GetUserTable(transaction::TransactionContext *txn, db_oid_t db_oid, namespace_oid_t ns_oid,
                                      const std::string &name) {
  try {
    auto tbl_handle = GetUserTableHandle(txn, db_oid, ns_oid);
    return tbl_handle.GetTable(txn, name);
  } catch (const std::out_of_range &e) {
    return nullptr;
  }
}

SqlTableHelper *Catalog::GetUserTable(transaction::TransactionContext *txn, db_oid_t db_oid, namespace_oid_t ns_oid,
                                      table_oid_t table_oid) {
  try {
    auto tbl_handle = GetUserTableHandle(txn, db_oid, ns_oid);
    return tbl_handle.GetTable(txn, table_oid);
  } catch (const std::out_of_range &e) {
    return nullptr;
  }
}

TableCatalogView Catalog::GetUserTableHandle(transaction::TransactionContext *txn, db_oid_t db_oid,
                                             namespace_oid_t ns_oid) {
  auto db_handle = GetDatabaseHandle();
  // TODO(pakhtar): error checking...
  // find the database
  auto ns_handle = db_handle.GetNamespaceTable(txn, db_oid);
  auto ns_entry = ns_handle.GetNamespaceEntry(txn, ns_oid);
  if (ns_entry == nullptr) {
    throw CATALOG_EXCEPTION("namespace does not exist");
  }
  return ns_handle.GetTableHandle(txn, ns_oid);
}

uint32_t Catalog::GetNextOid() { return oid_++; }

void Catalog::Bootstrap(transaction::TransactionContext *txn) {
  CATALOG_LOG_TRACE("Bootstrapping global catalogs ...");
  CreatePGDatabase(table_oid_t(GetNextOid()));
  PopulatePGDatabase(txn);

  CreatePGTablespace(DEFAULT_DATABASE_OID, table_oid_t(GetNextOid()));
  PopulatePGTablespace(txn);

  pg_settings_ = SettingsCatalogTable::Create(txn, this, DEFAULT_DATABASE_OID, "pg_settings");

  BootstrapDatabase(txn, DEFAULT_DATABASE_OID);
}

// TODO(pakhtar): resolve second arg.
void Catalog::AddColumnsToPGAttribute(transaction::TransactionContext *txn, db_oid_t db_oid,
                                      const std::shared_ptr<storage::SqlTable> &table) {
  Schema schema = table->GetSchema();
  std::vector<Schema::Column> cols = schema.GetColumns();
  catalog::SqlTableHelper *pg_attribute = GetCatalogTable(db_oid, CatalogTableType::ATTRIBUTE);
  int32_t col_num = 0;
  for (auto &c : cols) {
    std::vector<type::TransientValue> row;
    row.emplace_back(type::TransientValueFactory::GetInteger(!c.GetOid()));
    row.emplace_back(type::TransientValueFactory::GetInteger(!table->Oid()));
    row.emplace_back(type::TransientValueFactory::GetVarChar(c.GetName()));

    // pg_type.oid
    auto type_handle = GetDatabaseHandle().GetTypeTable(txn, db_oid);
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
  pg_database_ = new catalog::SqlTableHelper(table_oid);

  // columns we use
  for (auto col : DatabaseCatalogTable::schema_cols_) {
    pg_database_->DefineColumn(col.col_name, col.type_id, false, col_oid_t(GetNextOid()));
  }
  // create the table
  pg_database_->Create();
}

void Catalog::PopulatePGDatabase(transaction::TransactionContext *txn) {
  std::vector<type::TransientValue> row;
  db_oid_t terrier_oid = DEFAULT_DATABASE_OID;
  CATALOG_LOG_TRACE("Populate pg_database table");

  row.emplace_back(type::TransientValueFactory::GetInteger(!terrier_oid));
  row.emplace_back(type::TransientValueFactory::GetVarChar("terrier"));
  SetUnusedColumns(&row, DatabaseCatalogTable::schema_cols_);
  pg_database_->InsertRow(txn, row);
}

void Catalog::CreatePGTablespace(db_oid_t db_oid, table_oid_t table_oid) {
  CATALOG_LOG_TRACE("Creating pg_tablespace table");
  pg_tablespace_ = TablespaceCatalogTable::Create(this, db_oid, "pg_tablespace");
}

void Catalog::PopulatePGTablespace(transaction::TransactionContext *txn) {
  CATALOG_LOG_TRACE("Populate pg_tablespace table");
  auto ts_handle = GetTablespaceHandle();

  ts_handle.AddEntry(txn, "pg_global");
  ts_handle.AddEntry(txn, "pg_default");
}

void Catalog::BootstrapDatabase(transaction::TransactionContext *txn, db_oid_t db_oid) {
  CATALOG_LOG_TRACE("Bootstrapping database oid (db_oid) {}", !db_oid);
  AddToMap(db_oid, CatalogTableType::DATABASE, pg_database_);
  AddToMap(db_oid, CatalogTableType::TABLESPACE, pg_tablespace_);
  AddToMap(db_oid, CatalogTableType::SETTINGS, pg_settings_);

  // Order: pg_attribute -> pg_namespace -> pg_class
  CreatePGAttribute(txn, db_oid);
  CreatePGNamespace(txn, db_oid);
  CreatePGType(txn, db_oid);
  CreatePGAttrDef(txn, db_oid);
  CreatePGClass(txn, db_oid);

  std::vector<CatalogTableType> c_tables = {DATABASE, TABLESPACE, ATTRIBUTE, NAMESPACE, CLASS, TYPE, ATTRDEF, SETTINGS};
  auto add_cols_to_pg_attr = [this, txn, db_oid](const CatalogTableType cttype) {
    auto table_p = GetCatalogTable(db_oid, cttype);
    AddColumnsToPGAttribute(txn, db_oid, table_p->GetSqlTable());
  };
  std::for_each(c_tables.begin(), c_tables.end(), add_cols_to_pg_attr);
}

void Catalog::CreatePGAttribute(terrier::transaction::TransactionContext *txn, terrier::catalog::db_oid_t db_oid) {
  AttributeCatalogTable::Create(txn, this, db_oid, "pg_attribute");
}

void Catalog::CreatePGAttrDef(transaction::TransactionContext *txn, db_oid_t db_oid) {
  AttrDefCatalogTable::Create(txn, this, db_oid, "pg_attrdef");
}

void Catalog::CreatePGNamespace(transaction::TransactionContext *txn, db_oid_t db_oid) {
  // create the namespace table
  NamespaceCatalogTable::Create(txn, this, db_oid, "pg_namespace");

  auto ns_handle = GetDatabaseHandle().GetNamespaceTable(txn, db_oid);

  // populate it
  ns_handle.AddEntry(txn, "pg_catalog");
  ns_handle.AddEntry(txn, "public");
}

void Catalog::CreatePGClass(transaction::TransactionContext *txn, db_oid_t db_oid) {
  std::vector<type::TransientValue> row;

  // create pg_class storage
  ClassCatalogTable::Create(txn, this, db_oid, "pg_class");

  auto class_handle = GetDatabaseHandle().GetClassTable(txn, db_oid);

  // lookup oids inserted in multiple entries
  auto pg_catalog_namespace_oid =
      !GetDatabaseHandle().GetNamespaceTable(txn, db_oid).GetNamespaceEntry(txn, "pg_catalog")->GetOid();

  auto pg_global_ts_oid = !GetTablespaceHandle().GetTablespaceEntry(txn, "pg_global")->GetOid();
  auto pg_default_ts_oid = !GetTablespaceHandle().GetTablespaceEntry(txn, "pg_default")->GetOid();

  // Insert pg_database
  // (namespace: catalog, tablespace: global)
  CATALOG_LOG_TRACE("Inserting pg_database into pg_class ...");
  auto pg_db_tbl_p = reinterpret_cast<uint64_t>(GetCatalogTable(db_oid, CatalogTableType::DATABASE));
  auto pg_database_entry_oid = !GetCatalogTable(db_oid, CatalogTableType::DATABASE)->Oid();

  class_handle.AddEntry(txn, pg_db_tbl_p, pg_database_entry_oid, "pg_database", pg_catalog_namespace_oid,
                        pg_global_ts_oid);

  // Insert pg_tablespace
  // (namespace: catalog, tablespace: global)
  CATALOG_LOG_TRACE("Inserting pg_tablespace into pg_class ...");
  auto pg_ts_tbl_p = reinterpret_cast<uint64_t>(GetCatalogTable(db_oid, CatalogTableType::TABLESPACE));
  auto pg_tablespace_entry_oid = !GetCatalogTable(db_oid, CatalogTableType::TABLESPACE)->Oid();

  class_handle.AddEntry(txn, pg_ts_tbl_p, pg_tablespace_entry_oid, "pg_tablespace", pg_catalog_namespace_oid,
                        pg_global_ts_oid);

  // Insert pg_namespace
  // (namespace: catalog, tablespace: default)
  CATALOG_LOG_TRACE("Inserting pg_namespace into pg_class ...");
  auto pg_ns_tbl_p = reinterpret_cast<uint64_t>(GetCatalogTable(db_oid, CatalogTableType::NAMESPACE));
  auto pg_ns_entry_oid = !GetCatalogTable(db_oid, CatalogTableType::NAMESPACE)->Oid();

  class_handle.AddEntry(txn, pg_ns_tbl_p, pg_ns_entry_oid, "pg_namespace", pg_catalog_namespace_oid, pg_default_ts_oid);

  // Insert pg_class
  // (namespace: catalog, tablespace: default)
  auto pg_cls_tbl_p = reinterpret_cast<uint64_t>(GetCatalogTable(db_oid, CatalogTableType::CLASS));
  auto pg_cls_entry_oid = !GetCatalogTable(db_oid, CatalogTableType::CLASS)->Oid();

  class_handle.AddEntry(txn, pg_cls_tbl_p, pg_cls_entry_oid, "pg_class", pg_catalog_namespace_oid, pg_default_ts_oid);

  // Insert pg_attribute
  // (namespace: catalog, tablespace: default)
  auto pg_attr_tbl_p = reinterpret_cast<uint64_t>(GetCatalogTable(db_oid, CatalogTableType::ATTRIBUTE));
  auto pg_attr_entry_oid = !GetCatalogTable(db_oid, CatalogTableType::ATTRIBUTE)->Oid();

  class_handle.AddEntry(txn, pg_attr_tbl_p, pg_attr_entry_oid, "pg_attribute", pg_catalog_namespace_oid,
                        pg_default_ts_oid);

  // Insert pg_attrdef
  // (namespace: catalog, tablespace: default)
  auto pg_attrdef_tbl_p = reinterpret_cast<uint64_t>(GetCatalogTable(db_oid, CatalogTableType::ATTRDEF));
  auto pg_attrdef_entry_oid = !GetCatalogTable(db_oid, CatalogTableType::ATTRDEF)->Oid();

  class_handle.AddEntry(txn, pg_attrdef_tbl_p, pg_attrdef_entry_oid, "pg_attrdef", pg_catalog_namespace_oid,
                        pg_default_ts_oid);

  // Insert pg_type
  // (namespace: catalog, tablespace: default)
  auto pg_type_tbl_p = reinterpret_cast<uint64_t>(GetCatalogTable(db_oid, CatalogTableType::TYPE));
  auto pg_type_entry_oid = !GetCatalogTable(db_oid, CatalogTableType::TYPE)->Oid();

  class_handle.AddEntry(txn, pg_type_tbl_p, pg_type_entry_oid, "pg_type", pg_catalog_namespace_oid, pg_default_ts_oid);
}

void Catalog::CreatePGType(transaction::TransactionContext *txn, db_oid_t db_oid) {
  TypeCatalogTable::Create(txn, this, db_oid, "pg_type");

  std::vector<type::TransientValue> row;
  // TODO(Yesheng): get rid of this strange calling chain
  auto pg_type_handle = GetDatabaseHandle().GetTypeTable(txn, db_oid);
  auto catalog_ns_oid =
      GetDatabaseHandle().GetNamespaceTable(txn, db_oid).GetNamespaceEntry(txn, "pg_catalog")->GetOid();

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

void Catalog::DeleteDatabaseTables(db_oid_t db_oid) {
  auto table_oid_map = cat_map_.at(db_oid);
  CATALOG_LOG_DEBUG("Deleting tables for db_oid {}", !db_oid);
  auto tbl = table_oid_map.begin();
  while (tbl != table_oid_map.end()) {
    auto tbl_p = tbl->second;
    tbl++;
    if ((tbl_p == pg_database_) || (tbl_p == pg_tablespace_) || (tbl_p == pg_settings_)) {
      continue;
    }
    delete tbl_p;
  }
}

void Catalog::DestroyDB(db_oid_t oid) {
  // Note that we are using shared pointers for SqlTableHelper. Catalog class have references to all the catalog tables,
  // (i.e, tables that have namespace "pg_catalog") but not user created tables. We cannot use a shared pointer for a
  // user table because it will be automatically freed if no one holds it.
  // Since we don't automatically free these tables, we need to free tables when we destroy the database
  auto txn = txn_manager_->BeginTransaction();

  auto pg_class = GetCatalogTable(oid, CatalogTableType::CLASS);
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

  // get the pg_catalog oid
  auto pg_catalog_oid = GetDatabaseHandle().GetNamespaceTable(txn, oid).NameToOid(txn, "pg_catalog");
  for (uint32_t i = 0; i < num_rows; i++) {
    auto row = columns->InterpretAsRow(i);
    byte *col_p = row.AccessForceNotNull(col_map.at(col_oids[3]));
    auto nsp_oid = *reinterpret_cast<uint32_t *>(col_p);
    if (nsp_oid != !pg_catalog_oid) {
      // user created tables, need to free them
      byte *addr_col = row.AccessForceNotNull(col_map.at(col_oids[0]));
      int64_t ptr = *reinterpret_cast<int64_t *>(addr_col);
      delete reinterpret_cast<SqlTableHelper *>(ptr);
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
  SetUnusedColumns(&entry, DatabaseCatalogTable::schema_cols_);
  pg_database_->InsertRow(txn, entry);

  // map for system catalog tables
  cat_map_[oid] = std::unordered_map<CatalogTableType, catalog::SqlTableHelper *>();
}

void Catalog::SetUnusedColumns(std::vector<type::TransientValue> *vec, const std::vector<SchemaCol> &cols) {
  for (const auto col : cols) {
    if (col.used) {
      continue;
    }
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

std::string Catalog::ValueTypeIdToSchemaType(type::TypeId type_id) {
  switch (type_id) {
    case type::TypeId::BOOLEAN:
      return std::string("boolean");

    case type::TypeId::TINYINT:
      return std::string("tinyint");

    case type::TypeId::SMALLINT:
      return std::string("smallint");

    case type::TypeId::INTEGER:
      return std::string("integer");

    case type::TypeId::BIGINT:
      return std::string("bigint");

    case type::TypeId::DATE:
      return std::string("date");

    case type::TypeId::DECIMAL:
      return std::string("decimal");

    case type::TypeId::TIMESTAMP:
      return std::string("timestamp");

    case type::TypeId::VARCHAR:
      return std::string("varchar");

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
  auto ns_handle = db_handle.GetNamespaceTable(txn, db_oid);
  ns_handle.Dump(txn);

  // pg_attribute
  CATALOG_LOG_DEBUG("");
  CATALOG_LOG_DEBUG("-- pg_attribute -- ");
  auto attr_handle = db_handle.GetAttributeTable(txn, db_oid);
  attr_handle.Dump(txn);

  // pg_type
  CATALOG_LOG_DEBUG("");
  CATALOG_LOG_DEBUG("-- pg_type -- ");
  auto type_handle = db_handle.GetTypeTable(txn, db_oid);
  type_handle.Dump(txn);

  // pg_class
  CATALOG_LOG_DEBUG("");
  CATALOG_LOG_DEBUG("-- pg_class -- ");
  auto cls_handle = db_handle.GetClassTable(txn, db_oid);
  cls_handle.Dump(txn);
}

}  // namespace terrier::catalog
