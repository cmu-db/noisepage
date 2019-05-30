#include <utility>

#include "catalog/postgres/builder.h"
#include "catalog/postgres/pg_attribute.h"
#include "catalog/postgres/pg_class.h"
#include "catalog/postgres/pg_constraint.h"
#include "catalog/postgres/pg_index.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/postgres/pg_type.h"
#include "catalog/schema.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "type/transient_value_factory.h"

namespace terrier::storage::postgres {

#define MAX_NAME_LENGTH 63 // This mimics PostgreSQL behavior

static DatabaseCatalog Builder::NewCatalog(transaction::TransactionContext *txn, storage::BlockStore block_store) {
  auto dbc = new DatabaseCatalog();

  dbc.namespaces_ = new SqlTable(block_store, Builder::GetNamespaceTableSchema());
  dbc.classes_ = new SqlTable(block_store, Builder::GetClassTableSchema());
  dbc.indexes_ = new SqlTable(block_store, Builder::GetIndexTableSchema());
  dbc.columns_ = new SqlTable(block_store, Builder::GetColumnTableSchema());
  dbc.types_ = new SqlTable(block_store, Builder::GetTypeTableSchema());
  dbc.constraints_ = new SqlTable(block_store, Builder::GetConstraintTableSchema());

  // Instantiate all of the indexes

  return dbc;
}

/**
 * Helper function to handle generating the implicit "NULL" default values
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

static Schema Builder::GetTypeTableSchema()  {
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

} // namespace terrier::storage::postgres
