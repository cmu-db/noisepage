#include <catalog/transient_value_util.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "catalog/type_handle.h"

namespace terrier::catalog {

const std::vector<SchemaCol> TypeHandle::schema_cols_ = {{0, "oid", type::TypeId::INTEGER},
                                                         {1, "typname", type::TypeId::VARCHAR},
                                                         {2, "typnamespace", type::TypeId::INTEGER},
                                                         {4, "typlen", type::TypeId::INTEGER},
                                                         {6, "typtype", type::TypeId::VARCHAR}};

const std::vector<SchemaCol> TypeHandle::unused_schema_cols_ = {
    {3, "typowner", type::TypeId::INTEGER},      {5, "typbyval", type::TypeId::BOOLEAN},
    {7, "typcatagory", type::TypeId::VARCHAR},   {8, "typispreferred", type::TypeId::BOOLEAN},
    {9, "typisdefined", type::TypeId::BOOLEAN},  {10, "typdelim", type::TypeId::VARCHAR},
    {11, "typrelid", type::TypeId::INTEGER},     {12, "typelem", type::TypeId::INTEGER},
    {13, "typarray", type::TypeId::INTEGER},     {14, "typinput", type::TypeId::INTEGER},
    {15, "typoutput", type::TypeId::INTEGER},    {16, "typreceive", type::TypeId::INTEGER},
    {17, "typsend", type::TypeId::INTEGER},      {18, "typmodin", type::TypeId::INTEGER},
    {19, "typmodout", type::TypeId::INTEGER},    {20, "typanalyze", type::TypeId::INTEGER},
    {21, "typalign", type::TypeId::VARCHAR},     {22, "typstorage", type::TypeId::VARCHAR},
    {23, "typnotnull", type::TypeId::BOOLEAN},   {24, "typbasetype", type::TypeId::INTEGER},
    {25, "typtypmod", type::TypeId::INTEGER},    {26, "typndims", type::TypeId::INTEGER},
    {27, "typcollation", type::TypeId::INTEGER}, {28, "typdefaultbin", type::TypeId::VARCHAR},
    {29, "typdefault", type::TypeId::VARCHAR},   {30, "typacl", type::TypeId::VARCHAR},
};

TypeHandle::TypeHandle(Catalog *catalog, std::shared_ptr<catalog::SqlTableRW> pg_type)
    : catalog_(catalog), pg_type_rw_(std::move(pg_type)) {}

type_oid_t TypeHandle::TypeToOid(transaction::TransactionContext *txn, const std::string &type) {
  auto te = GetTypeEntry(txn, type);
  return type_oid_t(type::TransientValuePeeker::PeekInteger(te->GetColumn(0)));
}

std::shared_ptr<TypeHandle::TypeEntry> TypeHandle::GetTypeEntry(transaction::TransactionContext *txn, type_oid_t oid) {
  std::vector<type::TransientValue> search_vec, ret_row;
  search_vec.push_back(type::TransientValueFactory::GetInteger(!oid));
  ret_row = pg_type_rw_->FindRow(txn, search_vec);
  return std::make_shared<TypeEntry>(oid, std::move(ret_row));
}

std::shared_ptr<TypeHandle::TypeEntry> TypeHandle::GetTypeEntry(transaction::TransactionContext *txn,
                                                                const std::string &type) {
  std::vector<type::TransientValue> search_vec, ret_row;
  search_vec.push_back(type::TransientValueFactory::GetNull(type::TypeId::INTEGER));
  search_vec.push_back(type::TransientValueFactory::GetVarChar(type.c_str()));
  ret_row = pg_type_rw_->FindRow(txn, search_vec);
  type_oid_t oid(type::TransientValuePeeker::PeekInteger(ret_row[0]));
  return std::make_shared<TypeHandle::TypeEntry>(oid, std::move(ret_row));
}

/*
 * Lookup a type and return the entry, e.g. "boolean"
 */
std::shared_ptr<TypeHandle::TypeEntry> TypeHandle::GetTypeEntry(transaction::TransactionContext *txn,
                                                                const type::TransientValue &type) {
  std::vector<type::TransientValue> search_vec, ret_row;
  for (int32_t i = 0; i < 1; i++) {
    search_vec.push_back(type::TransientValueFactory::GetNull(type::TypeId::INTEGER));
  }
  search_vec.push_back(TransientValueUtil::MakeCopy(type));
  ret_row = pg_type_rw_->FindRow(txn, search_vec);
  type_oid_t oid(type::TransientValuePeeker::PeekInteger(ret_row[0]));
  return std::make_shared<TypeHandle::TypeEntry>(oid, std::move(ret_row));
}

void TypeHandle::AddEntry(transaction::TransactionContext *txn, type_oid_t oid, const std::string &typname,
                          namespace_oid_t typnamespace, int32_t typlen, const std::string &typtype) {
  std::vector<type::TransientValue> row;
  // FIXME might be problematic
  row.emplace_back(type::TransientValueFactory::GetInteger(!oid));
  row.emplace_back(type::TransientValueFactory::GetVarChar(typname.c_str()));
  row.emplace_back(type::TransientValueFactory::GetInteger(!typnamespace));
  row.emplace_back(type::TransientValueFactory::GetInteger(typlen));
  row.emplace_back(type::TransientValueFactory::GetVarChar(typtype.c_str()));
  catalog_->SetUnusedColumns(&row, TypeHandle::unused_schema_cols_);
  pg_type_rw_->InsertRow(txn, row);
}

std::shared_ptr<catalog::SqlTableRW> TypeHandle::Create(transaction::TransactionContext *txn, Catalog *catalog,
                                                        db_oid_t db_oid, const std::string &name) {
  table_oid_t pg_type_oid(catalog->GetNextOid());
  std::shared_ptr<catalog::SqlTableRW> pg_type = std::make_shared<catalog::SqlTableRW>(pg_type_oid);

  // used columns
  for (auto col : schema_cols_) {
    pg_type->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }
  // unused columns
  for (auto col : unused_schema_cols_) {
    pg_type->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }
  pg_type->Create();
  catalog->AddToMaps(db_oid, pg_type_oid, name, pg_type);

  return pg_type;
}

}  // namespace terrier::catalog
