#include <utility>
#include <vector>

#include "catalog/index_schema.h"
#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_attribute.h"
#include "catalog/postgres/pg_class.h"
#include "catalog/postgres/pg_constraint.h"
#include "catalog/postgres/pg_database.h"
#include "catalog/postgres/pg_index.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/postgres/pg_type.h"
#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "type/transient_value_factory.h"

namespace terrier::storage::postgres {

#define MAX_NAME_LENGTH 63  // This mimics PostgreSQL behavior

static Schema Builder::GetDatabaseTableSchema() {
  std::vector<Schema::Column> columns;

  columns.emplace_back("datoid", type::TypeId::INTEGER, false, MakeNull(type::TypeId::INTEGER));
  columns.back().SetOid(DATOID_COL_OID);

  columns.emplace_back("datname", type::TypeId::VARCHAR, false, MakeNull(type::TypeId::VARCHAR));
  columns.back().SetOid(DATNAME_COL_OID);

  return Schema(columns);
}

static IndexSchema Builder::GetDatabaseOidIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::Integer, false,
                       new parser::ColumnValueExpression(DATABASE_TABLE_OID, DATOID_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  // Primary
  IndexSchema schema(columns, true, true, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

static IndexSchema Builder::GetDatabaseNameIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::VARCHAR, false,
                       new parser::ColumnValueExpression(DATABASE_TABLE_OID, DATNAME_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  // Unique, not primary
  IndexSchema schema(columns, true, false, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

static DatabaseCatalog *Builder::CreateDatabaseCatalog(storage::BlockStore *block_store, db_oid_t oid) {
  auto dbc = new DatabaseCatalog(oid);

  dbc.namespaces_ = new SqlTable(block_store, Builder::GetNamespaceTableSchema());
  dbc.classes_ = new SqlTable(block_store, Builder::GetClassTableSchema());
  dbc.indexes_ = new SqlTable(block_store, Builder::GetIndexTableSchema());
  dbc.columns_ = new SqlTable(block_store, Builder::GetColumnTableSchema());
  dbc.types_ = new SqlTable(block_store, Builder::GetTypeTableSchema());
  dbc.constraints_ = new SqlTable(block_store, Builder::GetConstraintTableSchema());

  // Indexes on pg_namespace
  dbc.namespaces_oid_index_ = Builder::BuildUniqueIndex(Builder::GetNamespaceOidIndexSchema(), NAMESPACE_OID_INDEX_OID);
  dbc.namespaces_name_index_ =
      Builder::BuildUniqueIndex(Builder::GetNamespaceNameIndexSchema(), NAMESPACE_NAME_INDEX_OID);

  // Indexes on pg_class
  dbc.classes_oid_index_ = Builder::BuildUniqueIndex(Builder::GetClassOidIndexSchema(), CLASS_OID_INDEX_OID);
  dbc.classes_name_index_ = Builder::BuildUniqueIndex(Builder::GetClassNameIndexSchema(), CLASS_NAME_INDEX_OID);
  dbc.classes_namespace_index_ =
      Builder::BuildLookupIndex(Builder::GetClassNamespaceIndexSchema(), CLASS_NAMESPACE_INDEX_OID);

  // Indexes on pg_index
  dbc.indexes_oid_index_ = Builder::BuildUniqueIndex(Builder::GetIndexOidIndexSchema(), INDEX_OID_INDEX_OID);
  dbc.indexes_table_index_ = Builder::BuildLookupIndex(Builder::GetIndexTableIndexSchema(), INDEX_TABLE_INDEX_OID);

  // Indexes on pg_attribute
  dbc.columns_oid_index_ = Builder::BuildUniqueIndex(Builder::GetColumnOidIndexSchema(), COLUMN_OID_INDEX_OID);
  dbc.columns_name_index_ = Builder::BuildUniqueIndex(Builder::GetColumnNameIndexSchema(), COLUMN_NAME_INDEX_OID);

  // Indexes on pg_type
  dbc.types_oid_index_ = Builder::BuildUniqueIndex(Builder::GetTypeOidIndexSchema(), TYPE_OID_INDEX_OID);
  dbc.types_name_index_ = Builder::BuildUniqueIndex(Builder::GetTypeNameIndexSchema(), TYPE_NAME_INDEX_OID);
  dbc.types_namespace_index_ =
      Builder::BuildLookupIndex(Builder::GetTypeNamespaceIndexSchema(), TYPE_NAMESPACE_INDEX_OID);

  // Indexes on pg_constraint
  dbc.constraints_oid_index_ =
      Builder::BuildUniqueIndex(Builder::GetConstraintOidIndexSchema(), CONSTRAINT_OID_INDEX_OID);
  dbc.constraints_name_index_ =
      Builder::BuildUniqueIndex(Builder::GetConstraintNameIndexSchema(), CONSTRAINT_NAME_INDEX_OID);
  dbc.constraints_namespace_index_ =
      Builder::BuildLookupIndex(Builder::GetConstraintNamespaceIndexSchema(), CONSTRAINT_NAMESPACE_INDEX_OID);
  dbc.constraints_table_index_ =
      Builder::BuildLookupIndex(Builder::GetConstraintTableIndexSchema(), CONSTRAINT_TABLE_INDEX_OID);
  dbc.constraints_index_index_ =
      Builder::BuildLookupIndex(Builder::GetConstraintIndexIndexSchema(), CONSTRAINT_INDEX_INDEX_OID);
  dbc.constraints_foreignkey_index_ =
      Builder::BuildLookupIndex(Builder::GetConstraintForeignKeyIndexSchema(), CONSTRAINT_FOREIGNKEY_INDEX_OID);

  return dbc;
}

static void BootstrapDatabaseCatalog(transaction::TransactionContext *txn, DatabaseCatalog *catalog) {
  // TODO (John):  Actually implement this...
  // General flow:
  //   Add namespaces:  pg_catalog, public
  //   Add types:  in pg_catalog namespace with OIDs corresponding to internal enum values
  // [[After this can be deferred as it is not necessary for basic functionality]]
  //   Add tables and indexes to pg_class
  //   Add columns to pg_attribute
  //   Add index metadata to pg_index
  //   Add constraints
  //   Update pg_class to include pointers (necessary to trigger special-cased logic during recovery/replication)
}

/**
 * Helper function to handle generating the implicit "NULL" default values
 * @param type of the value which is NULL
 * @return NULL expression with the correct type
 */
static parser::AbstractExpression *MakeNull(type::TypeId type) {
  return new parser::ConstantValueExpression(std::move(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
}

static Schema Builder::GetAttributeTableSchema() {
  std::vector<Schema::Column> columns;

  columns.emplace_back("attnum", type::TypeId::INTEGER, false, MakeNull(type::TypeId::INTEGER));
  columns.back().SetOid(ATTNUM_COL_OID);

  columns.emplace_back("attrelid", type::TypeId::INTEGER, false, MakeNull(type::TypeId::INTEGER));
  columns.back().SetOid(ATTRELID_COL_OID);

  columns.emplace_back("attname", type::TypeId::VARCHAR, false, MakeNull(type::TypeId::VARCHAR));
  columns.back().SetOid(ATTAME_COL_OID);

  columns.emplace_back("atttypid", type::TypeId::INTEGER, false, MakeNull(type::TypeId::INTEGER));
  columns.back().SetOid(ATTTYPID_COL_OID);

  columns.emplace_back("attlen", type::TypeId::SMALLINT, false, MakeNull(type::TypeId::SMALLINT));
  columns.back().SetOid(ATTLEN_COL_OID);

  columns.emplace_back("attnotnull", type::TypeId::BOOLEAN, false, MakeNull(type::TypeId::BOOLEAN));
  columns.back().SetOid(ATTNOTNULL_COL_OID);

  columns.emplace_back("adbin", type::TypeId::BIGINT, false, MakeNull(type::TypeId::BIGINT));
  columns.back().SetOid(ADBIN_COL_OID);

  columns.emplace_back("adsrc", type::TypeId::VARCHAR, false, MakeNull(type::TypeId::VARCHAR));
  columns.back().SetOid(ADSRC_COL_OID);

  return Schema(columns);
}

static Schema Builder::GetClassTableSchema() {
  std::vector<Schema::Column> columns;

  columns.emplace_back("reloid", type::TypeId::INTEGER, false, MakeNull(type::TypeId::INTEGER));
  columns.back().SetOid(RELOID_COL_OID);

  columns.emplace_back("relname", type::TypeId::VARCHAR, false, MakeNull(type::TypeId::VARCHAR));
  columns.back().SetOid(RELNAME_COL_OID);

  columns.emplace_back("relnamespace", type::TypeId::INTEGER, false, MakeNull(type::TypeId::INTEGER));
  columns.back().SetOid(RELNAMESPACE_COL_OID);

  columns.emplace_back("relkind", type::TypeId::TINYINT, false, MakeNull(type::TypeId::TINYINT));
  columns.back().SetOid(RELKIND_COL_OID);

  columns.emplace_back("schema", type::TypeId::BIGINT, false, MakeNull(type::TypeId::BIGINT));
  columns.back().SetOid(REL_SCHEMA_COL_OID);

  columns.emplace_back("pointer", type::TypeId::BIGINT, true, MakeNull(type::TypeId::BIGINT));
  columns.back().SetOid(REL_PTR_COL_OID);

  columns.emplace_back("nextcoloid", type::TypeId::INTEGER, true, MakeNull(type::TypeId::INTEGER));
  columns.back().SetOid(REL_NEXTCOLOID_COL_OID);

  return Schema(columns);
}

static Schema Builder::GetConstraintTableSchema() {
  std::vector<Schema::Column> columns;

  columns.emplace_back("conoid", type::TypeId::INTEGER, false, MakeNull(type::TypeId::INTEGER));
  columns.back().SetOid(CONOID_COL_OID);

  columns.emplace_back("conname", type::TypeId::VARCHAR, false, MakeNull(type::TypeId::VARCHAR));
  columns.back().SetOid(CONNAME_COL_OID);

  columns.emplace_back("connamespace", type::TypeId::INTEGER, false, MakeNull(type::TypeId::INTEGER));
  columns.back().SetOid(CONNAMESPACE_COL_OID);

  columns.emplace_back("contype", type::TypeId::TINYINT, false, MakeNull(type::TypeId::TINYINT));
  columns.back().SetOid(CONTYPE_COL_OID);

  columns.emplace_back("condeferrable", type::TypeId::BOOLEAN, false, MakeNull(type::TypeId::BOOLEAN));
  columns.back().SetOid(CONDEFERRABLE_COL_OID);

  columns.emplace_back("condeferred", type::TypeId::BOOLEAN, false, MakeNull(type::TypeId::BOOLEAN));
  columns.back().SetOid(CONDEFERRED_COL_OID);

  columns.emplace_back("convalidated", type::TypeId::BOOLEAN, false, MakeNull(type::TypeId::BOOLEAN));
  columns.back().SetOid(CONVALIDATED_COL_OID);

  columns.emplace_back("conrelid", type::TypeId::INTEGER, false, MakeNull(type::TypeId::INTEGER));
  columns.back().SetOid(CONRELID_COL_OID);

  columns.emplace_back("conindid", type::TypeId::INTEGER, true, MakeNull(type::TypeId::INTEGER));
  columns.back().SetOid(CONINDID_COL_OID);

  columns.emplace_back("confrelid", type::TypeId::INTEGER, true, MakeNull(type::TypeId::INTEGER));
  columns.back().SetOid(CONFRELID_COL_OID);

  columns.emplace_back("conbin", type::TypeId::BIGINT, false, MakeNull(type::TypeId::BIGINT));
  columns.back().SetOid(CONBIN_COL_OID);

  columns.emplace_back("consrc", type::TypeId::VARCHAR, false, MakeNull(type::TypeId::VARCHAR));
  columns.back().SetOid(CONSRC_COL_OID);

  return Schema(columns);
}

static Schema Builder::GetIndexTableSchema() {
  std::vector<Schema::Column> columns;

  columns.emplace_back("indoid", type::TypeId::INTEGER, false, MakeNull(type::TypeId::INTEGER));
  columns.back().SetOid(INDOID_COL_OID);

  columns.emplace_back("indrelid", type::TypeId::INTEGER, false, MakeNull(type::TypeId::INTEGER));
  columns.back().SetOid(INDRELID_COL_OID);

  columns.emplace_back("indisunique", type::TypeId::BOOLEAN, false, MakeNull(type::TypeId::BOOLEAN));
  columns.back().SetOid(INDISUNIQUE_COL_OID);

  columns.emplace_back("indisprimary", type::TypeId::BOOLEAN, false, MakeNull(type::TypeId::BOOLEAN));
  columns.back().SetOid(INDISPRIMARY_COL_OID);

  columns.emplace_back("indisexclusion", type::TypeId::BOOLEAN, false, MakeNull(type::TypeId::BOOLEAN));
  columns.back().SetOid(INDISEXCLUSION_COL_OID);

  columns.emplace_back("indimmediate", type::TypeId::BOOLEAN, false, MakeNull(type::TypeId::BOOLEAN));
  columns.back().SetOid(INDISIMMEDIATE_COL_OID);

  columns.emplace_back("indisvalid", type::TypeId::BOOLEAN, false, MakeNull(type::TypeId::BOOLEAN));
  columns.back().SetOid(INDISVALID_COL_OID);

  columns.emplace_back("indisready", type::TypeId::BOOLEAN, false, MakeNull(type::TypeId::BOOLEAN));
  columns.back().SetOid(INDISREADY_COL_OID);

  columns.emplace_back("indislive", type::TypeId::BOOLEAN, false, MakeNull(type::TypeId::BOOLEAN));
  columns.back().SetOid(INDISLIVE_COL_OID);

  return Schema(columns);
}

static Schema Builder::GetNamespaceTableSchema() {
  std::vector<Schema::Column> columns;

  columns.emplace_back("nspoid", type::TypeId::INTEGER, false, MakeNull(type::TypeId::INTEGER));
  columns.back().SetOid(NSPOID_COL_OID);

  columns.emplace_back("nspname", type::TypeId::VARCHAR, false, MakeNull(type::TypeId::VARCHAR));
  columns.back().SetOid(NSPNAME_COL_OID);

  return Schema(columns);
}

static Schema Builder::GetTypeTableSchema() {
  std::vector<Schema::Column> columns;

  columns.emplace_back("typoid", type::TypeId::INTEGER, false, MakeNull(type::TypeId::INTEGER));
  columns.back().SetOid(TYPOID_COL_OID);

  columns.emplace_back("typname", type::TypeId::VARCHAR, false, MakeNull(type::TypeId::VARCHAR));
  columns.back().SetOid(TYPNAME_COL_OID);

  columns.emplace_back("typnamespace", type::TypeId::INTEGER, false, MakeNull(type::TypeId::INTEGER));
  columns.back().SetOid(TYPNAMESPACE_COL_OID);

  columns.emplace_back("typlen", type::TypeId::SMALLINT, false, MakeNull(type::TypeId::SMALLINT));
  columns.back().SetOid(TYPLEN_COL_OID);

  columns.emplace_back("typbyval", type::TypeId::BOOLEAN, false, MakeNull(type::TypeId::BOOLEAN));
  columns.back().SetOid(TYPBYVAL_COL_OID);

  columns.emplace_back("typtype", type::TypeId::TINYINT, false, MakeNull(type::TypeId::TINYINT));
  columns.back().SetOid(TYPTYPE_COL_OID);

  return Schema(columns);
}

static IndexSchema Builder::GetNamespaceOidIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::Integer, false,
                       new parser::ColumnValueExpression(NAMESPACE_TABLE_OID, NSPOID_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  // Primary
  IndexSchema schema(columns, true, true, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

static IndexSchema Builder::GetNamespaceNameIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::VARCHAR, false,
                       new parser::ColumnValueExpression(NAMESPACE_TABLE_OID, NSPNAME_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  // Unique, not primary
  IndexSchema schema(columns, true, false, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

static IndexSchema Builder::GetClassOidIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::INTEGER, false,
                       new parser::ColumnValueExpression(CLASS_TABLE_OID, RELOID_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  // Primary
  IndexSchema schema(columns, true, true, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

static IndexSchema Builder::GetClassNameIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::Integer, false,
                       new parser::ColumnValueExpression(CLASS_TABLE_OID, RELNAMESPACE_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  columns.emplace_back(type::TypeId::VARCHAR, false,
                       new parser::ColumnValueExpression(CLASS_TABLE_OID, RELNAME_COL_OID));
  columns.back().SetOid(col_oid_t(2));

  // Unique, not primary
  IndexSchema schema(columns, true, false, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

static IndexSchema Builder::GetClassNamespaceIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::Integer, false,
                       new parser::ColumnValueExpression(CLASS_TABLE_OID, RELNAMESPACE_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  // Unique, not primary
  IndexSchema schema(columns, false, false, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

static IndexSchema Builder::GetIndexOidIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::INTEGER, false,
                       new parser::ColumnValueExpression(INDEX_TABLE_OID, INDOID_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  // Primary
  IndexSchema schema(columns, true, true, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

static IndexSchema Builder::GetIndexTableIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::INTEGER, false,
                       new parser::ColumnValueExpression(INDEX_TABLE_OID, INDRELID_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  // Not unique
  IndexSchema schema(columns, false, false, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

static IndexSchema Builder::GetColumnOidIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::INTEGER, false,
                       new parser::ColumnValueExpression(COLUMN_TABLE_OID, ATTRELID_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  columns.emplace_back(type::TypeId::INTEGER, false,
                       new parser::ColumnValueExpression(COLUMN_TABLE_OID, ATTNUM_COL_OID));
  columns.back().SetOid(col_oid_t(2));

  // Primary
  IndexSchema schema(columns, true, true, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

static IndexSchema Builder::GetColumnNameIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::INTEGER, false,
                       new parser::ColumnValueExpression(COLUMN_TABLE_OID, ATTRELID_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  columns.emplace_back(type::TypeId::VARCHAR, false,
                       new parser::ColumnValueExpression(COLUMN_TABLE_OID, ATTNAME_COL_OID));
  columns.back().SetOid(col_oid_t(2));

  // Unique, not primary
  IndexSchema schema(columns, true, false, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

static IndexSchema Builder::GetColumnClassIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::INTEGER, false,
                       new parser::ColumnValueExpression(COLUMN_TABLE_OID, ATTRELID_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  // Not unique
  IndexSchema schema(columns, false, false, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

static IndexSchema Builder::GetTypeOidIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::INTEGER, false, new parser::ColumnValueExpression(TYPE_TABLE_OID, TYPOID_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  // Primary
  IndexSchema schema(columns, true, true, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

static IndexSchema Builder::GetTypeNameIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::Integer, false,
                       new parser::ColumnValueExpression(TYPE_TABLE_OID, TYPNAMESPACE_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  columns.emplace_back(type::TypeId::VARCHAR, false,
                       new parser::ColumnValueExpression(TYPE_TABLE_OID, TYPNAME_COL_OID));
  columns.back().SetOid(col_oid_t(2));

  // Unique, not primary
  IndexSchema schema(columns, true, false, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

static IndexSchema Builder::GetTypeNamespaceIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::Integer, false,
                       new parser::ColumnValueExpression(TYPE_TABLE_OID, TYPNAMESPACE_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  // Unique, not primary
  IndexSchema schema(columns, false, false, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

static IndexSchema Builder::GetConstraintOidIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::INTEGER, false,
                       new parser::ColumnValueExpression(CONSTRAINT_TABLE_OID, CONOID_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  // Primary
  IndexSchema schema(columns, true, true, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

static IndexSchema Builder::GetConstraintNameIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::Integer, false,
                       new parser::ColumnValueExpression(CONSTRAINT_TABLE_OID, CONNAMESPACE_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  columns.emplace_back(type::TypeId::VARCHAR, false,
                       new parser::ColumnValueExpression(CONSTRAINT_TABLE_OID, CONNAME_COL_OID));
  columns.back().SetOid(col_oid_t(2));

  // Unique, not primary
  IndexSchema schema(columns, true, false, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

static IndexSchema Builder::GetConstraintNamespaceIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::Integer, false,
                       new parser::ColumnValueExpression(CONSTRAINT_TABLE_OID, CONNAMESPACE_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  // Unique, not primary
  IndexSchema schema(columns, false, false, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

static IndexSchema Builder::GetConstraintTableIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::INTEGER, false,
                       new parser::ColumnValueExpression(CONSTRAINT_TABLE_OID, CONRELID_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  // Not unique
  IndexSchema schema(columns, false, false, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

static IndexSchema Builder::GetConstraintIndexIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::INTEGER, false,
                       new parser::ColumnValueExpression(CONSTRAINT_TABLE_OID, CONINDID_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  // Not unique
  IndexSchema schema(columns, false, false, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

static IndexSchema Builder::GetConstraintForeignTableIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back(type::TypeId::INTEGER, false,
                       new parser::ColumnValueExpression(CONSTRAINT_TABLE_OID, CONFRELID_COL_OID));
  columns.back().SetOid(col_oid_t(1));

  // Not unique
  IndexSchema schema(columns, false, false, false, true);
  schema.SetValid(true);
  schema.SetReady(true);

  return schema;
}

}  // namespace terrier::storage::postgres
