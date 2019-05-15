#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "catalog/type_handle.h"
#include "type/transient_value_factory.h"

namespace terrier::catalog {

const std::vector<SchemaCol> TypeCatalogTable::schema_cols_ = {{0, true, "oid", type::TypeId::INTEGER},
                                                               {1, true, "typname", type::TypeId::VARCHAR},
                                                               {2, true, "typnamespace", type::TypeId::INTEGER},
                                                               {4, true, "typlen", type::TypeId::INTEGER},
                                                               {6, true, "typtype", type::TypeId::VARCHAR},
                                                               {3, false, "typowner", type::TypeId::INTEGER},
                                                               {5, false, "typbyval", type::TypeId::BOOLEAN},
                                                               {7, false, "typcatagory", type::TypeId::VARCHAR},
                                                               {8, false, "typispreferred", type::TypeId::BOOLEAN},
                                                               {9, false, "typisdefined", type::TypeId::BOOLEAN},
                                                               {10, false, "typdelim", type::TypeId::VARCHAR},
                                                               {11, false, "typrelid", type::TypeId::INTEGER},
                                                               {12, false, "typelem", type::TypeId::INTEGER},
                                                               {13, false, "typarray", type::TypeId::INTEGER},
                                                               {14, false, "typinput", type::TypeId::INTEGER},
                                                               {15, false, "typoutput", type::TypeId::INTEGER},
                                                               {16, false, "typreceive", type::TypeId::INTEGER},
                                                               {17, false, "typsend", type::TypeId::INTEGER},
                                                               {18, false, "typmodin", type::TypeId::INTEGER},
                                                               {19, false, "typmodout", type::TypeId::INTEGER},
                                                               {20, false, "typanalyze", type::TypeId::INTEGER},
                                                               {21, false, "typalign", type::TypeId::VARCHAR},
                                                               {22, false, "typstorage", type::TypeId::VARCHAR},
                                                               {23, false, "typnotnull", type::TypeId::BOOLEAN},
                                                               {24, false, "typbasetype", type::TypeId::INTEGER},
                                                               {25, false, "typtypmod", type::TypeId::INTEGER},
                                                               {26, false, "typndims", type::TypeId::INTEGER},
                                                               {27, false, "typcollation", type::TypeId::INTEGER},
                                                               {28, false, "typdefaultbin", type::TypeId::VARCHAR},
                                                               {29, false, "typdefault", type::TypeId::VARCHAR},
                                                               {30, false, "typacl", type::TypeId::VARCHAR}};

TypeCatalogTable::TypeCatalogTable(Catalog *catalog, SqlTableHelper *pg_type)
    : catalog_(catalog), pg_type_rw_(pg_type) {}

type_oid_t TypeCatalogTable::TypeToOid(transaction::TransactionContext *txn, const std::string &type) {
  auto te = GetTypeEntry(txn, type);
  if (te == nullptr) {
    // type does not exist
    return type_oid_t(NULL_OID);
  }
  return type_oid_t(type::TransientValuePeeker::PeekInteger(te->GetColumn(0)));
}

std::shared_ptr<TypeCatalogEntry> TypeCatalogTable::GetTypeEntry(transaction::TransactionContext *txn, type_oid_t oid) {
  std::vector<type::TransientValue> search_vec, ret_row;
  search_vec.push_back(type::TransientValueFactory::GetInteger(!oid));
  ret_row = pg_type_rw_->FindRow(txn, search_vec);
  if (ret_row.empty()) {
    return nullptr;
  }
  return std::make_shared<TypeCatalogEntry>(oid, pg_type_rw_, std::move(ret_row));
}

std::shared_ptr<TypeCatalogEntry> TypeCatalogTable::GetTypeEntry(transaction::TransactionContext *txn,
                                                                 const std::string &type) {
  std::vector<type::TransientValue> search_vec, ret_row;
  search_vec.push_back(type::TransientValueFactory::GetNull(type::TypeId::INTEGER));
  search_vec.push_back(type::TransientValueFactory::GetVarChar(type));
  ret_row = pg_type_rw_->FindRow(txn, search_vec);
  if (ret_row.empty()) {
    return nullptr;
  }
  type_oid_t oid(type::TransientValuePeeker::PeekInteger(ret_row[0]));
  return std::make_shared<TypeCatalogEntry>(oid, pg_type_rw_, std::move(ret_row));
}

void TypeCatalogTable::AddEntry(transaction::TransactionContext *txn, type_oid_t oid, const std::string &typname,
                                namespace_oid_t typnamespace, int32_t typlen, const std::string &typtype) {
  std::vector<type::TransientValue> row;
  // FIXME might be problematic
  row.emplace_back(type::TransientValueFactory::GetInteger(!oid));
  row.emplace_back(type::TransientValueFactory::GetVarChar(typname));
  row.emplace_back(type::TransientValueFactory::GetInteger(!typnamespace));
  row.emplace_back(type::TransientValueFactory::GetInteger(typlen));
  row.emplace_back(type::TransientValueFactory::GetVarChar(typtype));
  catalog_->SetUnusedColumns(&row, TypeCatalogTable::schema_cols_);
  pg_type_rw_->InsertRow(txn, row);
}

SqlTableHelper *TypeCatalogTable::Create(transaction::TransactionContext *txn, Catalog *catalog, db_oid_t db_oid,
                                         const std::string &name) {
  table_oid_t pg_type_oid(catalog->GetNextOid());
  auto pg_type = new catalog::SqlTableHelper(pg_type_oid);

  // used columns
  for (auto col : schema_cols_) {
    pg_type->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }
  pg_type->Create();
  catalog->AddToMap(db_oid, CatalogTableType::TYPE, pg_type);
  return pg_type;
}

std::string_view TypeCatalogEntry::GetTypename() { return GetVarcharColumn("typname"); }
int32_t TypeCatalogEntry::GetTypenamespace() { return GetIntegerColumn("typenamespace"); }
int32_t TypeCatalogEntry::GetTypelen() { return GetIntegerColumn("typelen"); }
std::string_view TypeCatalogEntry::GetTypetype() { return GetVarcharColumn("typtype"); };

}  // namespace terrier::catalog
