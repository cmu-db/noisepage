#include "catalog/type_handle.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace terrier::catalog {
TypeHandle::TypeHandle(Catalog *catalog, std::shared_ptr<catalog::SqlTableRW> pg_type)
    : pg_type_rw_(std::move(pg_type)) {}

type_oid_t TypeHandle::TypeToOid(transaction::TransactionContext *txn, const std::string &type) {
  auto te = GetTypeEntry(txn, type);
  return type_oid_t(te->GetColumn(0).GetIntValue());
}

std::shared_ptr<TypeHandle::TypeEntry> TypeHandle::GetTypeEntry(transaction::TransactionContext *txn, type_oid_t oid) {
  std::vector<type::Value> search_vec, ret_row;
  search_vec.push_back(type::ValueFactory::GetIntegerValue(!oid));
  ret_row = pg_type_rw_->FindRow(txn, search_vec);
  return std::make_shared<TypeEntry>(oid, ret_row);
}

std::shared_ptr<TypeHandle::TypeEntry> TypeHandle::GetTypeEntry(transaction::TransactionContext *txn,
                                                                const std::string &type) {
  std::vector<type::Value> search_vec, ret_row;
  search_vec.push_back(type::ValueFactory::GetNullValue(type::TypeId::INTEGER));
  search_vec.push_back(type::ValueFactory::GetVarcharValue(type.c_str()));
  ret_row = pg_type_rw_->FindRow(txn, search_vec);
  type_oid_t oid(ret_row[0].GetIntValue());
  return std::make_shared<TypeHandle::TypeEntry>(oid, ret_row);
}

/*
 * Lookup a type and return the entry, e.g. "boolean"
 */
std::shared_ptr<TypeHandle::TypeEntry> TypeHandle::GetTypeEntry(transaction::TransactionContext *txn,
                                                                type::Value type) {
  std::vector<type::Value> search_vec, ret_row;
  for (int32_t i = 0; i < 1; i++) {
    search_vec.push_back(type::ValueFactory::GetNullValue(type::TypeId::INTEGER));
  }
  search_vec.push_back(type);
  ret_row = pg_type_rw_->FindRow(txn, search_vec);
  type_oid_t oid(ret_row[0].GetIntValue());
  return std::make_shared<TypeHandle::TypeEntry>(oid, ret_row);
}

}  // namespace terrier::catalog
