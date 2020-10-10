#include "catalog/postgres/builder.h"

#include <utility>
#include <vector>

#include "catalog/database_catalog.h"
#include "catalog/index_schema.h"
#include "catalog/postgres/pg_attribute.h"
#include "catalog/postgres/pg_class.h"
#include "catalog/postgres/pg_constraint.h"
#include "catalog/postgres/pg_database.h"
#include "catalog/postgres/pg_index.h"
#include "catalog/postgres/pg_language.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/postgres/pg_proc.h"
#include "catalog/postgres/pg_type.h"
#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "storage/index/index_builder.h"
#include "storage/sql_table.h"

namespace terrier::catalog::postgres {

constexpr uint8_t MAX_NAME_LENGTH = 63;  // This mimics PostgreSQL behavior

Schema Builder::GetDatabaseTableSchema() {
  std::vector<Schema::Column> columns;

  columns.emplace_back("datoid", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(DATOID_COL_OID);

  columns.emplace_back("datname", type::TypeId::VARCHAR, MAX_NAME_LENGTH, false,
                       parser::ConstantValueExpression(type::TypeId::VARCHAR));
  columns.back().SetOid(DATNAME_COL_OID);

  columns.emplace_back("pointer", type::TypeId::BIGINT, false, parser::ConstantValueExpression(type::TypeId::BIGINT));
  columns.back().SetOid(DAT_CATALOG_COL_OID);

  return Schema(columns);
}

IndexSchema Builder::GetDatabaseOidIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("datoid", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(INVALID_DATABASE_OID, DATABASE_TABLE_OID, DATOID_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  // Primary
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, true, true, false, true);

  return schema;
}

IndexSchema Builder::GetDatabaseNameIndexSchema() {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("datname", type::TypeId::VARCHAR, MAX_NAME_LENGTH, false,
                       parser::ColumnValueExpression(INVALID_DATABASE_OID, DATABASE_TABLE_OID, DATNAME_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  // Unique, not primary
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, true, false, false, true);

  return schema;
}

DatabaseCatalog *Builder::CreateDatabaseCatalog(
    const common::ManagedPointer<storage::BlockStore> block_store, const db_oid_t oid,
    const common::ManagedPointer<storage::GarbageCollector> garbage_collector) {
  auto dbc = new DatabaseCatalog(oid, garbage_collector);

  dbc->namespaces_ = new storage::SqlTable(block_store, Builder::GetNamespaceTableSchema());
  dbc->classes_ = new storage::SqlTable(block_store, Builder::GetClassTableSchema());
  dbc->indexes_ = new storage::SqlTable(block_store, Builder::GetIndexTableSchema());
  dbc->columns_ = new storage::SqlTable(block_store, Builder::GetColumnTableSchema());
  dbc->types_ = new storage::SqlTable(block_store, Builder::GetTypeTableSchema());
  dbc->constraints_ = new storage::SqlTable(block_store, Builder::GetConstraintTableSchema());
  dbc->languages_ = new storage::SqlTable(block_store, Builder::GetLanguageTableSchema());
  dbc->procs_ = new storage::SqlTable(block_store, Builder::GetProcTableSchema());

  // Indexes on pg_namespace
  dbc->namespaces_oid_index_ =
      Builder::BuildUniqueIndex(Builder::GetNamespaceOidIndexSchema(oid), NAMESPACE_OID_INDEX_OID);
  dbc->namespaces_name_index_ =
      Builder::BuildUniqueIndex(Builder::GetNamespaceNameIndexSchema(oid), NAMESPACE_NAME_INDEX_OID);

  // Indexes on pg_class
  dbc->classes_oid_index_ = Builder::BuildUniqueIndex(Builder::GetClassOidIndexSchema(oid), CLASS_OID_INDEX_OID);
  dbc->classes_name_index_ = Builder::BuildUniqueIndex(Builder::GetClassNameIndexSchema(oid), CLASS_NAME_INDEX_OID);
  dbc->classes_namespace_index_ =
      Builder::BuildLookupIndex(Builder::GetClassNamespaceIndexSchema(oid), CLASS_NAMESPACE_INDEX_OID);

  // Indexes on pg_index
  dbc->indexes_oid_index_ = Builder::BuildUniqueIndex(Builder::GetIndexOidIndexSchema(oid), INDEX_OID_INDEX_OID);
  dbc->indexes_table_index_ = Builder::BuildLookupIndex(Builder::GetIndexTableIndexSchema(oid), INDEX_TABLE_INDEX_OID);

  // Indexes on pg_attribute
  dbc->columns_oid_index_ = Builder::BuildUniqueIndex(Builder::GetColumnOidIndexSchema(oid), COLUMN_OID_INDEX_OID);
  dbc->columns_name_index_ = Builder::BuildUniqueIndex(Builder::GetColumnNameIndexSchema(oid), COLUMN_NAME_INDEX_OID);

  // Indexes on pg_type
  dbc->types_oid_index_ = Builder::BuildUniqueIndex(Builder::GetTypeOidIndexSchema(oid), TYPE_OID_INDEX_OID);
  dbc->types_name_index_ = Builder::BuildUniqueIndex(Builder::GetTypeNameIndexSchema(oid), TYPE_NAME_INDEX_OID);
  dbc->types_namespace_index_ =
      Builder::BuildLookupIndex(Builder::GetTypeNamespaceIndexSchema(oid), TYPE_NAMESPACE_INDEX_OID);

  // Indexes on pg_constraint
  dbc->constraints_oid_index_ =
      Builder::BuildUniqueIndex(Builder::GetConstraintOidIndexSchema(oid), CONSTRAINT_OID_INDEX_OID);
  dbc->constraints_name_index_ =
      Builder::BuildUniqueIndex(Builder::GetConstraintNameIndexSchema(oid), CONSTRAINT_NAME_INDEX_OID);
  dbc->constraints_namespace_index_ =
      Builder::BuildLookupIndex(Builder::GetConstraintNamespaceIndexSchema(oid), CONSTRAINT_NAMESPACE_INDEX_OID);
  dbc->constraints_table_index_ =
      Builder::BuildLookupIndex(Builder::GetConstraintTableIndexSchema(oid), CONSTRAINT_TABLE_INDEX_OID);
  dbc->constraints_index_index_ =
      Builder::BuildLookupIndex(Builder::GetConstraintIndexIndexSchema(oid), CONSTRAINT_INDEX_INDEX_OID);
  dbc->constraints_foreigntable_index_ =
      Builder::BuildLookupIndex(Builder::GetConstraintForeignTableIndexSchema(oid), CONSTRAINT_FOREIGNTABLE_INDEX_OID);

  // Indexes on pg_language
  dbc->languages_oid_index_ =
      Builder::BuildUniqueIndex(Builder::GetLanguageOidIndexSchema(oid), LANGUAGE_OID_INDEX_OID);
  dbc->languages_name_index_ =
      Builder::BuildUniqueIndex(Builder::GetLanguageNameIndexSchema(oid), LANGUAGE_NAME_INDEX_OID);

  // Indexes on pg_proc
  dbc->procs_oid_index_ = Builder::BuildUniqueIndex(Builder::GetProcOidIndexSchema(oid), PRO_OID_INDEX_OID);
  dbc->procs_name_index_ = Builder::BuildLookupIndex(Builder::GetProcNameIndexSchema(oid), PRO_NAME_INDEX_OID);

  dbc->next_oid_.store(START_OID);

  return dbc;
}

Schema Builder::GetColumnTableSchema() {
  std::vector<Schema::Column> columns;

  columns.emplace_back("attnum", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(ATTNUM_COL_OID);

  columns.emplace_back("attrelid", type::TypeId::INTEGER, false,
                       parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(ATTRELID_COL_OID);

  columns.emplace_back("attname", type::TypeId::VARCHAR, MAX_NAME_LENGTH, false,
                       parser::ConstantValueExpression(type::TypeId::VARCHAR));
  columns.back().SetOid(ATTNAME_COL_OID);

  columns.emplace_back("atttypid", type::TypeId::INTEGER, false,
                       parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(ATTTYPID_COL_OID);

  columns.emplace_back("attlen", type::TypeId::SMALLINT, false,
                       parser::ConstantValueExpression(type::TypeId::SMALLINT));
  columns.back().SetOid(ATTLEN_COL_OID);

  columns.emplace_back("attnotnull", type::TypeId::BOOLEAN, false,
                       parser::ConstantValueExpression(type::TypeId::BOOLEAN));
  columns.back().SetOid(ATTNOTNULL_COL_OID);

  columns.emplace_back("adsrc", type::TypeId::VARCHAR, 4096, false,
                       parser::ConstantValueExpression(type::TypeId::VARCHAR));
  columns.back().SetOid(ADSRC_COL_OID);

  return Schema(columns);
}

Schema Builder::GetClassTableSchema() {
  std::vector<Schema::Column> columns;

  columns.emplace_back("reloid", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(RELOID_COL_OID);

  columns.emplace_back("relname", type::TypeId::VARCHAR, MAX_NAME_LENGTH, false,
                       parser::ConstantValueExpression(type::TypeId::VARCHAR));
  columns.back().SetOid(RELNAME_COL_OID);

  columns.emplace_back("relnamespace", type::TypeId::INTEGER, false,
                       parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(RELNAMESPACE_COL_OID);

  columns.emplace_back("relkind", type::TypeId::TINYINT, false, parser::ConstantValueExpression(type::TypeId::TINYINT));
  columns.back().SetOid(RELKIND_COL_OID);

  columns.emplace_back("schema", type::TypeId::BIGINT, false, parser::ConstantValueExpression(type::TypeId::BIGINT));
  columns.back().SetOid(REL_SCHEMA_COL_OID);

  columns.emplace_back("pointer", type::TypeId::BIGINT, true, parser::ConstantValueExpression(type::TypeId::BIGINT));
  columns.back().SetOid(REL_PTR_COL_OID);

  columns.emplace_back("nextcoloid", type::TypeId::INTEGER, true,
                       parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(REL_NEXTCOLOID_COL_OID);

  return Schema(columns);
}

Schema Builder::GetConstraintTableSchema() {
  std::vector<Schema::Column> columns;

  columns.emplace_back("conoid", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(CONOID_COL_OID);

  columns.emplace_back("conname", type::TypeId::VARCHAR, MAX_NAME_LENGTH, false,
                       parser::ConstantValueExpression(type::TypeId::VARCHAR));
  columns.back().SetOid(CONNAME_COL_OID);

  columns.emplace_back("connamespace", type::TypeId::INTEGER, false,
                       parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(CONNAMESPACE_COL_OID);

  columns.emplace_back("contype", type::TypeId::TINYINT, false, parser::ConstantValueExpression(type::TypeId::TINYINT));
  columns.back().SetOid(CONTYPE_COL_OID);

  columns.emplace_back("condeferrable", type::TypeId::BOOLEAN, false,
                       parser::ConstantValueExpression(type::TypeId::BOOLEAN));
  columns.back().SetOid(CONDEFERRABLE_COL_OID);

  columns.emplace_back("condeferred", type::TypeId::BOOLEAN, false,
                       parser::ConstantValueExpression(type::TypeId::BOOLEAN));
  columns.back().SetOid(CONDEFERRED_COL_OID);

  columns.emplace_back("convalidated", type::TypeId::BOOLEAN, false,
                       parser::ConstantValueExpression(type::TypeId::BOOLEAN));
  columns.back().SetOid(CONVALIDATED_COL_OID);

  columns.emplace_back("conrelid", type::TypeId::INTEGER, false,
                       parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(CONRELID_COL_OID);

  columns.emplace_back("conindid", type::TypeId::INTEGER, true, parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(CONINDID_COL_OID);

  columns.emplace_back("confrelid", type::TypeId::INTEGER, true,
                       parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(CONFRELID_COL_OID);

  columns.emplace_back("conbin", type::TypeId::BIGINT, false, parser::ConstantValueExpression(type::TypeId::BIGINT));
  columns.back().SetOid(CONBIN_COL_OID);

  columns.emplace_back("consrc", type::TypeId::VARCHAR, 4096, false,
                       parser::ConstantValueExpression(type::TypeId::VARCHAR));
  columns.back().SetOid(CONSRC_COL_OID);

  return Schema(columns);
}

Schema Builder::GetIndexTableSchema() {
  std::vector<Schema::Column> columns;

  columns.emplace_back("indoid", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(INDOID_COL_OID);

  columns.emplace_back("indrelid", type::TypeId::INTEGER, false,
                       parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(INDRELID_COL_OID);

  columns.emplace_back("indisunique", type::TypeId::BOOLEAN, false,
                       parser::ConstantValueExpression(type::TypeId::BOOLEAN));
  columns.back().SetOid(INDISUNIQUE_COL_OID);

  columns.emplace_back("indisprimary", type::TypeId::BOOLEAN, false,
                       parser::ConstantValueExpression(type::TypeId::BOOLEAN));
  columns.back().SetOid(INDISPRIMARY_COL_OID);

  columns.emplace_back("indisexclusion", type::TypeId::BOOLEAN, false,
                       parser::ConstantValueExpression(type::TypeId::BOOLEAN));
  columns.back().SetOid(INDISEXCLUSION_COL_OID);

  columns.emplace_back("indimmediate", type::TypeId::BOOLEAN, false,
                       parser::ConstantValueExpression(type::TypeId::BOOLEAN));
  columns.back().SetOid(INDIMMEDIATE_COL_OID);

  columns.emplace_back("indisvalid", type::TypeId::BOOLEAN, false,
                       parser::ConstantValueExpression(type::TypeId::BOOLEAN));
  columns.back().SetOid(INDISVALID_COL_OID);

  columns.emplace_back("indisready", type::TypeId::BOOLEAN, false,
                       parser::ConstantValueExpression(type::TypeId::BOOLEAN));
  columns.back().SetOid(INDISREADY_COL_OID);

  columns.emplace_back("indislive", type::TypeId::BOOLEAN, false,
                       parser::ConstantValueExpression(type::TypeId::BOOLEAN));
  columns.back().SetOid(INDISLIVE_COL_OID);

  columns.emplace_back("implementation", type::TypeId::TINYINT, false,
                       parser::ConstantValueExpression(type::TypeId::TINYINT));
  columns.back().SetOid(IND_TYPE_COL_OID);

  return Schema(columns);
}

Schema Builder::GetNamespaceTableSchema() {
  std::vector<Schema::Column> columns;

  columns.emplace_back("nspoid", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(NSPOID_COL_OID);

  columns.emplace_back("nspname", type::TypeId::VARCHAR, MAX_NAME_LENGTH, false,
                       parser::ConstantValueExpression(type::TypeId::VARCHAR));
  columns.back().SetOid(NSPNAME_COL_OID);

  return Schema(columns);
}

Schema Builder::GetTypeTableSchema() {
  std::vector<Schema::Column> columns;

  columns.emplace_back("typoid", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(TYPOID_COL_OID);

  columns.emplace_back("typname", type::TypeId::VARCHAR, MAX_NAME_LENGTH, false,
                       parser::ConstantValueExpression(type::TypeId::VARCHAR));
  columns.back().SetOid(TYPNAME_COL_OID);

  columns.emplace_back("typnamespace", type::TypeId::INTEGER, false,
                       parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(TYPNAMESPACE_COL_OID);

  columns.emplace_back("typlen", type::TypeId::SMALLINT, false,
                       parser::ConstantValueExpression(type::TypeId::SMALLINT));
  columns.back().SetOid(TYPLEN_COL_OID);

  columns.emplace_back("typbyval", type::TypeId::BOOLEAN, false,
                       parser::ConstantValueExpression(type::TypeId::BOOLEAN));
  columns.back().SetOid(TYPBYVAL_COL_OID);

  columns.emplace_back("typtype", type::TypeId::TINYINT, false, parser::ConstantValueExpression(type::TypeId::TINYINT));
  columns.back().SetOid(TYPTYPE_COL_OID);

  return Schema(columns);
}

Schema Builder::GetLanguageTableSchema() {
  std::vector<Schema::Column> columns;

  columns.emplace_back("lanoid", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(LANOID_COL_OID);

  columns.emplace_back("lanname", type::TypeId::VARCHAR, MAX_NAME_LENGTH, false,
                       parser::ConstantValueExpression(type::TypeId::VARCHAR));
  columns.back().SetOid(LANNAME_COL_OID);

  columns.emplace_back("lanispl", type::TypeId::BOOLEAN, false, parser::ConstantValueExpression(type::TypeId::BOOLEAN));
  columns.back().SetOid(LANISPL_COL_OID);

  columns.emplace_back("lanpltrusted", type::TypeId::BOOLEAN, false,
                       parser::ConstantValueExpression(type::TypeId::BOOLEAN));
  columns.back().SetOid(LANPLTRUSTED_COL_OID);

  columns.emplace_back("lanplcallfoid", type::TypeId::INTEGER, true,
                       parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(LANPLCALLFOID_COL_OID);

  columns.emplace_back("laninline", type::TypeId::INTEGER, true,
                       parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(LANINLINE_COL_OID);

  columns.emplace_back("lanvalidator", type::TypeId::INTEGER, true,
                       parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(LANVALIDATOR_COL_OID);

  return Schema(columns);
}

IndexSchema Builder::GetNamespaceOidIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("nspoid", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, NAMESPACE_TABLE_OID, NSPOID_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  // Primary
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, true, true, false, true);

  return schema;
}

IndexSchema Builder::GetNamespaceNameIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("nspname", type::TypeId::VARCHAR, MAX_NAME_LENGTH, false,
                       parser::ColumnValueExpression(db, NAMESPACE_TABLE_OID, NSPNAME_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  // Unique, not primary
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, true, false, false, true);

  return schema;
}

IndexSchema Builder::GetClassOidIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("reloid", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, CLASS_TABLE_OID, RELOID_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  // Primary
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, true, true, false, true);

  return schema;
}

IndexSchema Builder::GetClassNameIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("relnamespace", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, CLASS_TABLE_OID, RELNAMESPACE_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  columns.emplace_back("relname", type::TypeId::VARCHAR, MAX_NAME_LENGTH, false,
                       parser::ColumnValueExpression(db, CLASS_TABLE_OID, RELNAME_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(2));

  // Unique, not primary
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, true, false, false, true);

  return schema;
}

IndexSchema Builder::GetClassNamespaceIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("relnamespace", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, CLASS_TABLE_OID, RELNAMESPACE_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  // Not unique
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, false, false, false, true);

  return schema;
}

IndexSchema Builder::GetIndexOidIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("indoid", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, INDEX_TABLE_OID, INDOID_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  // Primary
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, true, true, false, true);

  return schema;
}

IndexSchema Builder::GetIndexTableIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("indrelid", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, INDEX_TABLE_OID, INDRELID_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  // Not unique
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, false, false, false, true);

  return schema;
}

IndexSchema Builder::GetColumnOidIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("attrelid", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, COLUMN_TABLE_OID, ATTRELID_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  columns.emplace_back("attnum", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, COLUMN_TABLE_OID, ATTNUM_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(2));

  // Primary, must be a BWTREE due to ScanAscending usage
  IndexSchema schema(columns, storage::index::IndexType::BWTREE, true, true, false, true);

  return schema;
}

IndexSchema Builder::GetColumnNameIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("attrelid", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, COLUMN_TABLE_OID, ATTRELID_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  columns.emplace_back("attname", type::TypeId::VARCHAR, MAX_NAME_LENGTH, false,
                       parser::ColumnValueExpression(db, COLUMN_TABLE_OID, ATTNAME_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(2));

  // Unique, not primary
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, true, false, false, true);

  return schema;
}

IndexSchema Builder::GetTypeOidIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("typoid", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, TYPE_TABLE_OID, TYPOID_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  // Primary
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, true, true, false, true);

  return schema;
}

IndexSchema Builder::GetTypeNameIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("typnamespace", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, TYPE_TABLE_OID, TYPNAMESPACE_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  columns.emplace_back("typname", type::TypeId::VARCHAR, MAX_NAME_LENGTH, false,
                       parser::ColumnValueExpression(db, TYPE_TABLE_OID, TYPNAME_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(2));

  // Unique, not primary
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, true, false, false, true);

  return schema;
}

IndexSchema Builder::GetTypeNamespaceIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("typnamespace", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, TYPE_TABLE_OID, TYPNAMESPACE_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  // Not unique
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, false, false, false, true);

  return schema;
}

IndexSchema Builder::GetConstraintOidIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("conoid", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, CONSTRAINT_TABLE_OID, CONOID_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  // Primary
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, true, true, false, true);

  return schema;
}

IndexSchema Builder::GetConstraintNameIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("connamespace", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, CONSTRAINT_TABLE_OID, CONNAMESPACE_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  columns.emplace_back("conname", type::TypeId::VARCHAR, MAX_NAME_LENGTH, false,
                       parser::ColumnValueExpression(db, CONSTRAINT_TABLE_OID, CONNAME_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(2));

  // Unique, not primary
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, true, false, false, true);

  return schema;
}

IndexSchema Builder::GetConstraintNamespaceIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("connamespace", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, CONSTRAINT_TABLE_OID, CONNAMESPACE_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  // Not unique
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, false, false, false, true);

  return schema;
}

IndexSchema Builder::GetConstraintTableIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("conrelid", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, CONSTRAINT_TABLE_OID, CONRELID_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  // Not unique
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, false, false, false, true);

  return schema;
}

IndexSchema Builder::GetConstraintIndexIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("conindid", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, CONSTRAINT_TABLE_OID, CONINDID_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  // Not unique
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, false, false, false, true);

  return schema;
}

IndexSchema Builder::GetConstraintForeignTableIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("confrelid", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, CONSTRAINT_TABLE_OID, CONFRELID_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  // Not unique
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, false, false, false, true);

  return schema;
}

IndexSchema Builder::GetLanguageOidIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("lanoid", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, LANGUAGE_TABLE_OID, LANOID_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  // Primary
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, true, true, false, true);

  return schema;
}

IndexSchema Builder::GetLanguageNameIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("lanname", type::TypeId::VARCHAR, MAX_NAME_LENGTH, false,
                       parser::ColumnValueExpression(db, LANGUAGE_TABLE_OID, LANNAME_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  // Unique, not primary
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, true, false, false, true);

  return schema;
}

Schema Builder::GetProcTableSchema() {
  std::vector<Schema::Column> columns;

  columns.emplace_back("prooid", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(PROOID_COL_OID);

  columns.emplace_back("proname", type::TypeId::VARCHAR, MAX_NAME_LENGTH, false,
                       parser::ConstantValueExpression(type::TypeId::VARCHAR));
  columns.back().SetOid(PRONAME_COL_OID);

  columns.emplace_back("pronamespace", type::TypeId::INTEGER, false,
                       parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(PRONAMESPACE_COL_OID);

  columns.emplace_back("prolang", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(PROLANG_COL_OID);

  columns.emplace_back("procost", type::TypeId::DECIMAL, true, parser::ConstantValueExpression(type::TypeId::DECIMAL));
  columns.back().SetOid(PROCOST_COL_OID);

  columns.emplace_back("prorows", type::TypeId::DECIMAL, true, parser::ConstantValueExpression(type::TypeId::DECIMAL));
  columns.back().SetOid(PROROWS_COL_OID);

  columns.emplace_back("provariadic", type::TypeId::INTEGER, false,
                       parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(PROVARIADIC_COL_OID);

  columns.emplace_back("proisagg", type::TypeId::BOOLEAN, false,
                       parser::ConstantValueExpression(type::TypeId::BOOLEAN));
  columns.back().SetOid(PROISAGG_COL_OID);

  columns.emplace_back("proiswindow", type::TypeId::BOOLEAN, false,
                       parser::ConstantValueExpression(type::TypeId::BOOLEAN));
  columns.back().SetOid(PROISWINDOW_COL_OID);

  columns.emplace_back("proisstrict", type::TypeId::BOOLEAN, false,
                       parser::ConstantValueExpression(type::TypeId::BOOLEAN));
  columns.back().SetOid(PROISSTRICT_COL_OID);

  columns.emplace_back("proretset", type::TypeId::BOOLEAN, false,
                       parser::ConstantValueExpression(type::TypeId::BOOLEAN));
  columns.back().SetOid(PRORETSET_COL_OID);

  columns.emplace_back("provolatile", type::TypeId::BOOLEAN, false,
                       parser::ConstantValueExpression(type::TypeId::BOOLEAN));
  columns.back().SetOid(PROVOLATILE_COL_OID);

  columns.emplace_back("pronargs", type::TypeId::SMALLINT, false,
                       parser::ConstantValueExpression(type::TypeId::TINYINT));
  columns.back().SetOid(PRONARGS_COL_OID);

  columns.emplace_back("pronargdefaults", type::TypeId::SMALLINT, false,
                       parser::ConstantValueExpression(type::TypeId::TINYINT));
  columns.back().SetOid(PRONARGDEFAULTS_COL_OID);

  columns.emplace_back("prorettype", type::TypeId::INTEGER, false,
                       parser::ConstantValueExpression(type::TypeId::INTEGER));
  columns.back().SetOid(PRORETTYPE_COL_OID);

  columns.emplace_back("proargtypes", type::TypeId::VARBINARY, 4096, false,
                       parser::ConstantValueExpression(type::TypeId::VARBINARY));
  columns.back().SetOid(PROARGTYPES_COL_OID);

  columns.emplace_back("proallargtypes", type::TypeId::VARBINARY, 4096, false,
                       parser::ConstantValueExpression(type::TypeId::VARBINARY));
  columns.back().SetOid(PROALLARGTYPES_COL_OID);

  columns.emplace_back("proargmodes", type::TypeId::VARBINARY, 4096, false,
                       parser::ConstantValueExpression(type::TypeId::VARBINARY));
  columns.back().SetOid(PROARGMODES_COL_OID);

  columns.emplace_back("proargdefaults", type::TypeId::VARBINARY, 4096, false,
                       parser::ConstantValueExpression(type::TypeId::VARBINARY));
  columns.back().SetOid(PROARGDEFAULTS_COL_OID);

  columns.emplace_back("proargnames", type::TypeId::VARBINARY, 4096, false,
                       parser::ConstantValueExpression(type::TypeId::VARBINARY));
  columns.back().SetOid(PROARGNAMES_COL_OID);

  columns.emplace_back("prosrc", type::TypeId::VARCHAR, 4096, false,
                       parser::ConstantValueExpression(type::TypeId::VARCHAR));
  columns.back().SetOid(PROSRC_COL_OID);

  columns.emplace_back("proconfig", type::TypeId::VARBINARY, 4096, false,
                       parser::ConstantValueExpression(type::TypeId::VARBINARY));
  columns.back().SetOid(PROCONFIG_COL_OID);

  columns.emplace_back("ctx_pointer", type::TypeId::BIGINT, true,
                       parser::ConstantValueExpression(type::TypeId::BIGINT));
  columns.back().SetOid(PRO_CTX_PTR_COL_OID);

  return Schema(columns);
}

IndexSchema Builder::GetProcOidIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("prooid", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, PRO_TABLE_OID, PROOID_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  // Primary
  IndexSchema schema(columns, storage::index::IndexType::HASHMAP, true, true, false, true);

  return schema;
}

IndexSchema Builder::GetProcNameIndexSchema(db_oid_t db) {
  std::vector<IndexSchema::Column> columns;

  columns.emplace_back("pronamespace", type::TypeId::INTEGER, false,
                       parser::ColumnValueExpression(db, PRO_TABLE_OID, PRONAMESPACE_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(1));

  columns.emplace_back("proname", type::TypeId::VARCHAR, MAX_NAME_LENGTH, false,
                       parser::ColumnValueExpression(db, PRO_TABLE_OID, PRONAME_COL_OID));
  columns.back().SetOid(indexkeycol_oid_t(2));

  // Non-Unique, not primary
  IndexSchema schema(columns, storage::index::IndexType::BWTREE, false, false, false, false);

  return schema;
}

storage::index::Index *Builder::BuildUniqueIndex(const IndexSchema &key_schema, index_oid_t oid) {
  TERRIER_ASSERT(key_schema.Unique(), "KeySchema must represent a unique index.");
  storage::index::IndexBuilder index_builder;
  index_builder.SetKeySchema(key_schema);
  return index_builder.Build();
}

storage::index::Index *Builder::BuildLookupIndex(const IndexSchema &key_schema, index_oid_t oid) {
  TERRIER_ASSERT(!(key_schema.Unique()), "KeySchema must represent a non-unique index.");
  storage::index::IndexBuilder index_builder;
  index_builder.SetKeySchema(key_schema);
  return index_builder.Build();
}

}  // namespace terrier::catalog::postgres
