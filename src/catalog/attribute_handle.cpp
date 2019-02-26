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
  CATALOG_EXCEPTION("column doesn't exist");
  return col_oid_t(0);
}
}  // namespace terrier::catalog
