#include "catalog/catalog_accessor.h"

#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "type/type_util.h"

namespace terrier::catalog {

db_oid_t CatalogAccessor::GetDatabaseOid(const std::string &name) {
  auto db_handle = catalog_->GetDatabaseHandle();
  auto db_entry = db_handle.GetDatabaseEntry(txn_, name);

  // If db_entry is nullptr, then the database couldn't be found and is invalid
  if (db_entry == nullptr) return INVALID_DATABASE_OID;

  return db_entry->GetOid();
}

db_oid_t CatalogAccessor::CreateDatabase(const std::string &name) {
  // Check to see if the database exists since the catalog doesn't
  db_oid_t db_oid = GetDatabaseOid(name);
  if (db_oid != INVALID_DATABASE_OID) return INVALID_DATABASE_OID;

  // Safe to call create
  return catalog_->CreateDatabase(txn_, name);
}

bool CatalogAccessor::DropDatabase(db_oid_t db) { return catalog_->DeleteDatabase(txn_, db); }

// TODO(John): Should this function do some sanity checks on the OIDs passed?
void CatalogAccessor::SetSearchPath(std::vector<namespace_oid_t> namespaces) { search_path_ = std::move(namespaces); }

namespace_oid_t CatalogAccessor::GetNamespaceOid(const std::string &name) {
  auto db_handle = catalog_->GetDatabaseHandle();
  auto ns_handle = db_handle.GetNamespaceTable(txn_, db_);
  auto ns_entry = ns_handle.GetNamespaceEntry(txn_, name);

  // If ns_entry is nullptr, then the database couldn't be found and is invalid
  if (ns_entry == nullptr) return INVALID_NAMESPACE_OID;

  return ns_entry->GetOid();
}

namespace_oid_t CatalogAccessor::CreateNamespace(const std::string &name) {
  // Check to see if the namespace exists in the database
  auto ns_oid = GetNamespaceOid(name);
  if (ns_oid != INVALID_NAMESPACE_OID) return INVALID_NAMESPACE_OID;

  // Safe to call create
  return catalog_->CreateNameSpace(txn_, db_, name);
}

bool CatalogAccessor::DropNamespace(namespace_oid_t ns) { return catalog_->DeleteNameSpace(txn_, db_, ns); }

table_oid_t CatalogAccessor::GetTableOid(const std::string &name) {
  SqlTableHelper *wrapper;

  // Search the namespaces in the order specified by our search path
  for (auto ns : search_path_) {
    wrapper = catalog_->GetUserTable(txn_, db_, ns, name);

    // If we found a matching table, return its OID
    if (wrapper != nullptr) return wrapper->Oid();
  }

  // Getting here means we never found a matching table, so return INVALID
  return INVALID_TABLE_OID;
}

table_oid_t CatalogAccessor::GetTableOid(namespace_oid_t ns, const std::string &name) {
  // Search the specified namespace for the table
  SqlTableHelper *wrapper = catalog_->GetUserTable(txn_, db_, ns, name);

  // If we found the table, return its OID else return INVALID
  return (wrapper != nullptr) ? wrapper->Oid() : INVALID_TABLE_OID;
}

table_oid_t CatalogAccessor::CreateTable(namespace_oid_t ns, const std::string &name,
                                         std::vector<ColumnDefinition> columns) {
  std::vector<Schema::Column> catalogColumns;
  catalogColumns.reserve(columns.size());

  // Loop through the column definitions and create Schema::Columns by assigning OIDs
  for (auto const &colDef : columns) {
    // Case off of VARLEN_COLUMN since there exist two different constructors
    if (type::TypeUtil::GetTypeSize(colDef.GetType()) == VARLEN_COLUMN)
      catalogColumns.emplace_back(colDef.GetName(), colDef.GetType(), colDef.GetMaxVarlenSize(), colDef.IsNullable(),
                                  col_oid_t(catalog_->GetNextOid()));
    else
      catalogColumns.emplace_back(colDef.GetName(), colDef.GetType(), colDef.IsNullable(),
                                  col_oid_t(catalog_->GetNextOid()));
  }

  // Create the Schema object
  Schema schema(catalogColumns);
  return catalog_->CreateUserTable(txn_, db_, ns, name, schema);
}

bool CatalogAccessor::RenameTable(table_oid_t table, const std::string &new_table_name) {
  // Since the name will be indexed, we'll need to do a delete and insert.
  // However, since the catalog does not expose an insert function where the
  // caller provides the OID, we'll need to manually execute the transaction on
  // both the catalog's map and the underlying SqlTable backing the catalog.

  // TODO(John): Implement this via the description above.
  TERRIER_ASSERT(true, "This function is not implemented yet");
  return false;
}

bool CatalogAccessor::DropTable(table_oid_t table) { return catalog_->DeleteUserTable(txn_, db_, table); }

bool CatalogAccessor::SetTablePointer(table_oid_t table, storage::SqlTable *table_ptr) {
  // TODO(John): Implementing this function is blocked on #380.
  // Specifically, we need to decouple the catalog's internal wrapper of user
  // tables from the storage layer to enable the SettingsManager and execution
  // engine finer-grained control of the storage.
  TERRIER_ASSERT(true, "This function is not implemented yet");
  return false;
}

storage::SqlTable *CatalogAccessor::GetTable(table_oid_t table) {
  auto db_handle = catalog_->GetDatabaseHandle();
  auto class_handle = db_handle.GetClassTable(txn_, db_);

  // pg_class expects the OID as a 'col_oid_t' so we need to strip the
  // 'table_oid_t' we have to a basic integer and reconstruct it as a column OID
  auto class_entry = class_handle.GetClassEntry(txn_, col_oid_t(!table));

  // Return a nullptr if needed, before dereferencing the helper pointer.
  if (class_entry == nullptr) return nullptr;

  return &*(class_entry->GetPtr()->GetSqlTable());
}

std::vector<col_oid_t> CatalogAccessor::AddColumns(table_oid_t table, std::vector<ColumnDefinition> columns) {
  // Blocked on fetching table entry by OID and database OID
  // TODO(John): Check if this transaction has an outstanding update on the table entry.
  // If not, create an update that increments the schema version field by one.

  std::vector<col_oid_t> col_oids;
  col_oids.reserve(columns.size());

  std::vector<Schema::Column> catalog_columns;
  // Loop through the column definitions and create Schema::Columns by assigning OIDs
  for (auto const &colDef : columns) {
    // Case off of VARLEN_COLUMN since there exist two different constructors
    if (type::TypeUtil::GetTypeSize(colDef.GetType()) == VARLEN_COLUMN)
      catalog_columns.emplace_back(colDef.GetName(), colDef.GetType(), colDef.GetMaxVarlenSize(), colDef.IsNullable(),
                                   col_oid_t(catalog_->GetNextOid()));
    else
      catalog_columns.emplace_back(colDef.GetName(), colDef.GetType(), colDef.IsNullable(),
                                   col_oid_t(catalog_->GetNextOid()));
    col_oids.emplace_back(catalog_columns.back().GetOid());
  }
  // TODO(John): Implement this by inserting these into pg_attribute

  TERRIER_ASSERT(true, "This function is not implemented yet");
  return col_oids;
}

std::vector<bool> CatalogAccessor::DropColumns(table_oid_t table, std::vector<col_oid_t> columns) {
  // Blocked on fetching table entry by OID and database OID
  // TODO(John): Check if this transaction has an outstanding update on the table entry.
  // If not, create an update that increments the schema version field by one.

  std::vector<bool> col_successes;
  col_successes.reserve(columns.size());

  // Loop through the column OIDs and delete them from the underlying table
  for (auto oid : columns) {
    // TODO(John): Implement the metadata deletion by deleting the row in pg_attribute
    col_successes.emplace_back(oid != INVALID_COLUMN_OID);
  }

  TERRIER_ASSERT(true, "This function is not implemented yet");
  return col_successes;
}

bool CatalogAccessor::SetColumnNullable(table_oid_t table, col_oid_t column, bool nullable) {
  // TODO(John):  Find the row in pg_attribute and update the not-null constraint
  // If the bool actually changed, then check if the transaction has an outstanding update
  // on the table entry.  If not, create an update that increments the schema version field
  // by one.
  TERRIER_ASSERT(true, "This function is not implemented yet");
  return false;
}

bool CatalogAccessor::RenameColumn(table_oid_t table, col_oid_t column, const std::string &new_column_name) {
  // TODO(John):  Find the row in pg_attribute and update the name to the new name.  This will likely
  // need to be a delete and insert since the 'attname' column should be indexed.
  TERRIER_ASSERT(true, "This function is not implemented yet");
  return false;
}

Schema *CatalogAccessor::GetSchema(table_oid_t table) {
  // TODO(John): We should probably add a column to pg_class where we can store
  // a pointer to a pre-allocated one and handle deallocation through a more
  // complicated set of deferrals.

  // std::vector<Schema::Column> columns;
  // auto db_handle = GetDatabaseHandle();
  // auto attr_handle = db_handle.GetAttributeTable(txn_, db_);

  // auto col_oids = attr_handle.GetTableColumns(txn_, table);
  // for (auto col : col_oids) {
  //   auto col_entry = attr_handle.GetAttributeEntry(txn_, table, col);
  //   auto name = col_entry.GetAttname();
  //   auto type = type::TypeID(static_cast<uint8_t>(!col_entry.GetAtttypid()));
  //   columns.emplace_back(col_entry.GetAttname(), )
  // }
  // // TODO(John): Actually build the schema from pg_attribute...
  // // This is blocked on efficiently fetching information without namespace OID
  // TERRIER_ASSERT(true, "This function is not implemented yet");

  // auto *schema = new Schema(columns);
  // txn_->RegisterAbortAction([&]() { delete schema; });
  // txn_->RegisterCommitAction([&]() { delete schema; });

  // return *schema;

  // TODO(John): Everything above is the correct way to do return a schema
  // object (i.e. using the actual catalog).  However, since end-to-end testing
  // needs to get this information and we don't want to enforce using the catalog
  // instead of the SqlTable hack, we have this hack.  Someone can hunt me down
  // and make me write a thousand catalogs as penance if this isn't fixed by
  // September 2019.
  auto db_handle = catalog_->GetDatabaseHandle();
  auto class_handle = db_handle.GetClassTable(txn_, db_);

  // pg_class expects the OID as a 'col_oid_t' so we need to strip the
  // 'table_oid_t' we have to a basic integer and reconstruct it as a column OID
  auto class_entry = class_handle.GetClassEntry(txn_, col_oid_t(!table));

  // Return a nullptr if needed, before dereferencing the helper pointer.
  if (class_entry == nullptr) return nullptr;

  return &*(class_entry->GetPtr()->GetSchema());
}

index_oid_t GetIndexOid(const std::string &name) {
  // TODO(John): Implement this similar to GetTable
  // Blocked on the catalog supporting indexes
  TERRIER_ASSERT(true, "This function is not implemented yet");
  return INVALID_INDEX_OID;
}

index_oid_t GetIndexOid(namespace_oid_t ns, const std::string &name) {
  // TODO(John): Implement this similar to GetTable
  // Blocked on the catalog supporting indexes
  TERRIER_ASSERT(true, "This function is not implemented yet");
  return INVALID_INDEX_OID;
}

std::vector<index_oid_t> GetIndexOids(table_oid_t table) {
  // TODO(John): This should be a simple index search on pg_class

  std::vector<index_oid_t> indexes;
  // Blocked on the catalog supporting indexes
  TERRIER_ASSERT(true, "This function is not implemented yet");
  return indexes;
}

bool DropIndex(index_oid_t index) {
  // TODO(John): Implement this similar to DropTable
  // Blocked on the catalog supporting indexes
  TERRIER_ASSERT(true, "This function is not implemented yet");
  return false;
}

bool SetIndexPointer(index_oid_t index, storage::index::Index *index_ptr) {
  // TODO(John): Implement this similar to SetTablePointer
  // Blocked on the catalog supporting indexes
  TERRIER_ASSERT(true, "This function is not implemented yet");
  return false;
}

storage::index::Index *SetIndexPointer(index_oid_t index) {
  // TODO(John): Implement this once there is an API to use in the catalog
  // Blocked on the catalog supporting indexes
  TERRIER_ASSERT(true, "This function is not implemented yet");
  return nullptr;
}

}  // namespace terrier::catalog
