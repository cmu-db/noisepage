#include "catalog/attribute_handle.h"
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "loggers/catalog_logger.h"
#include "storage/block_layout.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"
namespace terrier::catalog {

std::shared_ptr<AttributeHandle::AttributeEntry> AttributeHandle::GetAttributeEntry(
    transaction::TransactionContext *txn, col_oid_t oid) {
  std::vector<type::Value> search_vec, ret_row;
  search_vec.push_back(type::ValueFactory::GetIntegerValue(!oid));
  search_vec.push_back(type::ValueFactory::GetIntegerValue(!table_->Oid()));
  ret_row = pg_attribute_hrw_->FindRow(txn, search_vec);
  return std::make_shared<AttributeEntry>(oid, ret_row);
}

std::shared_ptr<AttributeHandle::AttributeEntry> AttributeHandle::GetAttributeEntry(
    transaction::TransactionContext *txn, const std::string &name) {
  std::vector<type::Value> search_vec, ret_row;
  search_vec.push_back(type::ValueFactory::GetNullValue(type::TypeId::INTEGER));
  search_vec.push_back(type::ValueFactory::GetIntegerValue(!table_->Oid()));
  search_vec.push_back(type::ValueFactory::GetVarcharValue(name.c_str()));
  ret_row = pg_attribute_hrw_->FindRow(txn, search_vec);
  if (ret_row.empty()) {
    throw CATALOG_EXCEPTION("attribute doesn't exist");
  }
  col_oid_t oid(ret_row[0].GetIntValue());
  return std::make_shared<AttributeEntry>(oid, ret_row);
}

col_oid_t AttributeHandle::NameToOid(transaction::TransactionContext *txn, const std::string &name) {
  Schema schema = table_->GetSqlTable()->GetSchema();
  auto cols = schema.GetColumns();
  for (auto &c : cols) {
    if (name == c.GetName()) {
      return c.GetOid();
    }
  }
  throw CATALOG_EXCEPTION("column doesn't exist");
  return col_oid_t(0);
}

std::shared_ptr<catalog::SqlTableRW> AttributeHandle::Create(transaction::TransactionContext *txn, Catalog *catalog,
                                                             db_oid_t db_oid, const std::string &name) {
  std::shared_ptr<catalog::SqlTableRW> pg_attr;

  // get an oid
  table_oid_t pg_attr_oid(catalog->GetNextOid());

  // uninitialized storage
  pg_attr = std::make_shared<catalog::SqlTableRW>(pg_attr_oid);

  // columns we use
  for (auto col : AttributeHandle::schema_cols_) {
    pg_attr->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }

  // columns we don't use
  for (auto col : AttributeHandle::unused_schema_cols_) {
    pg_attr->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }
  // now actually create, with the provided schema
  pg_attr->Create();
  catalog->AddToMaps(db_oid, pg_attr_oid, name, pg_attr);
  // catalog->AddColumnsToPGAttribute(txn, db_oid, pg_attr->GetSqlTable());
  return pg_attr;
}

// note that this is not identical to Postgres's column sequence

const std::vector<SchemaCol> AttributeHandle::schema_cols_ = {
    {0, "oid", type::TypeId::INTEGER},     {1, "attrelid", type::TypeId::INTEGER},
    {2, "attname", type::TypeId::VARCHAR}, {3, "atttypid", type::TypeId::INTEGER},
    {4, "attlen", type::TypeId::INTEGER},  {5, "attnum", type::TypeId::INTEGER}};

// TODO(pakhtar): add unused columns
const std::vector<SchemaCol> AttributeHandle::unused_schema_cols_ = {};

}  // namespace terrier::catalog
