#include "catalog/catalog_accessor.h"

#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "type/type_util.h"

namespace terrier::catalog {

db_oid_t CatalogAccessor::GetDatabaseOid(std::string name) {
  // TODO(John):  Implement a function to lookup OIDs by name in the catalog
  TERRIER_ASSERT(true, "This function is not implemented yet");
  return INVALID_DATABASE_OID;
}

db_oid_t CatalogAccessor::CreateDatabase(std::string name) {
  db_oid_t db_oid = GetDatabaseOid(name);

  // Check to see if the database exists since the catalog doesn't
  if (db_oid != INVALID_DATABASE_OID) return INVALID_DATABASE_OID;

  return catalog_->CreateDatabase(txn_, name);
}

bool CatalogAccessor::DropDatabase(db_oid_t db) {
  // TODO(John):  Need to translate the OID into a name for the call to the underlying catalog.
  TERRIER_ASSERT(true, "This function is not implemented yet");
  return false;
}

// TODO(John): Should this function do some sanity checks on the OIDs passed?
void CatalogAccessor::SetSearchPath(std::vector<namespace_oid_t> namespaces) { search_path_ = namespaces; }

namespace_oid_t CatalogAccessor::GetNamespaceOid(std::string name) {
  // TODO(John): Implement a function to lookup OIDs by name in the catalog
  TERRIER_ASSERT(true, "This function is not implemented yet");
  return INVALID_NAMESPACE_OID;
}

namespace_oid_t CatalogAccessor::CreateNamespace(std::string name) {
  return catalog_->CreateNameSpace(txn_, db_, name);
}

bool CatalogAccessor::DropNamespace(namespace_oid_t ns) {
  // TODO(John): Modify 'DeleteNameSpace' so that it returns whether or not
  // anything was deleted.
  catalog_->DeleteNameSpace(txn_, db_, ns);
  return true;
}

table_oid_t CatalogAccessor::GetTableOid(std::string name) {
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

table_oid_t CatalogAccessor::GetTableOid(namespace_oid_t ns, std::string name) {
  // Search the specified namespace for the table
  SqlTableHelper *wrapper = catalog_->GetUserTable(txn_, db_, ns, name);

  // If we found the table, return its OID else return INVALID
  return (wrapper != nullptr) ? wrapper->Oid() : INVALID_TABLE_OID;
}

table_oid_t CatalogAccessor::CreateTable(namespace_oid_t ns, std::string table_name,
                                         std::vector<ColumnDefinition> columns) {
  std::vector<Schema::Column> catalogColumns;
  catalogColumns.reserve(columns.size());

  // Loop through the column definitions and create Schema::Columns by assigning OIDs
  for (auto colDef : columns) {
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
  return catalog_->CreateUserTable(txn_, db_, ns, table_name, schema);
}

bool CatalogAccessor::RenameTable(table_oid_t table, std::string new_table_name) {
  // Since the name will be indexed, we'll need to do a delete and insert.
  // However, since the catalog does not expose an insert function where the
  // caller provides the OID, we'll need to manually execute the transaction on
  // both the catalog's map and the underlying SqlTable backing the catalog.

  // TODO(John): Implement this via the description above.
  TERRIER_ASSERT(true, "This function is not implemented yet");
  return false;
}

bool CatalogAccessor::DropTable(table_oid_t table) {
  // Need to find the corresponding namespace for this table OID in order
  // to call the catalog's delete function.

  // TODO(John): Implement this via the description above.
  TERRIER_ASSERT(true, "This function is not implemented yet");
  return false;
}

bool CatalogAccessor::SetTablePointer(table_oid_t table, storage::SqlTable *table_ptr) {
  // TODO(John): Implementing this function is blocked on #380.
  // Specifically, we need to decouple the catalog's internal wrapper of user
  // tables from the storage layer to enable the SettingsManager and execution
  // engine finer-grained control of the storage.
  TERRIER_ASSERT(true, "This function is not implemented yet");
  return false;
}

storage::SqlTable *CatalogAccessor::GetTable(table_oid_t table) {
  // Need to find the corresponding namespace for this table OID in order
  // to call the catalog's 'GetUserTable' function.

  // TODO(John): Implement this via the description above.
  TERRIER_ASSERT(true, "This function is not implemented yet");
  return nullptr;
}

std::vector<col_oid_t> CatalogAccessor::AddColumns(table_oid_t table, std::vector<ColumnDefinition> columns) {
  // Blocked on fetching table entry by OID and database OID
  // TODO(John): Check if this transaction has an outstanding update on the table entry.
  // If not, create an update that increments the schema version field by one.

  std::vector<col_oid_t> col_oids;
  col_oids.reserve(columns.size());

  std::vector<Schema::Column> catalog_columns;
  // Loop through the column definitions and create Schema::Columns by assigning OIDs
  for (auto colDef : columns) {
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

bool CatalogAccessor::RenameColumn(table_oid_t table, col_oid_t column, std::string new_column_name) {
  // TODO(John):  Find the row in pg_attribute and update the name to the new name.  This will likely
  // need to be a delete and insert since the 'attname' column should be indexed.
  TERRIER_ASSERT(true, "This function is not implemented yet");
  return false;
}

const Schema &CatalogAccessor::GetSchema(table_oid_t table) {
  // TODO(John): We should probably add a column to pg_class where we can store
  // a pointer to a pre-allocated one and handle deallocation through a more
  // complicated set of deferrals.

  std::vector<Schema::Column> columns;
  // TODO(John): Actually build the schema from pg_attribute...
  // This is blocked on efficiently fetching information without namespace OID
  TERRIER_ASSERT(true, "This function is not implemented yet");

  auto *schema = new Schema(columns);
  txn_->RegisterAbortAction([&]() { delete schema; });
  txn_->RegisterCommitAction([&]() { delete schema; });

  return *schema;
}

index_oid_t GetIndexOid(std::string name) {
  // TODO(John): Implement this similar to GetTable
  // Blocked on the catalog supporting indexes
  TERRIER_ASSERT(true, "This function is not implemented yet");
  return INVALID_INDEX_OID;
}

index_oid_t GetIndexOid(namespace_oid_t ns, std::string name) {
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
