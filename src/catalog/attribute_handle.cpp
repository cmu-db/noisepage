#include "catalog/attribute_handle.h"
#include <iostream>
#include <memory>
#include <string>
#include <utility>
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

// note that this is not identical to Postgres's column sequence

const std::vector<SchemaCol> AttributeCatalogTable::schema_cols_ = {
    {0, true, "oid", type::TypeId::INTEGER},     {1, true, "attrelid", type::TypeId::INTEGER},
    {2, true, "attname", type::TypeId::VARCHAR}, {3, true, "atttypid", type::TypeId::INTEGER},
    {4, true, "attlen", type::TypeId::INTEGER},  {5, true, "attnum", type::TypeId::INTEGER}};

// TODO(pakhtar): add unused columns

std::shared_ptr<AttributeCatalogEntry> AttributeCatalogTable::GetAttributeEntry(transaction::TransactionContext *txn,
                                                                                table_oid_t table_oid,
                                                                                col_oid_t col_oid) {
  std::vector<type::TransientValue> search_vec, ret_row;
  search_vec.push_back(type::TransientValueFactory::GetInteger(!col_oid));
  search_vec.push_back(type::TransientValueFactory::GetInteger(!table_oid));
  ret_row = pg_attribute_hrw_->FindRow(txn, search_vec);
  if (ret_row.empty()) {
    return nullptr;
  }
  col_oid_t oid(type::TransientValuePeeker::PeekInteger(ret_row[0]));
  return std::make_shared<AttributeCatalogEntry>(oid, pg_attribute_hrw_, std::move(ret_row));
}

std::shared_ptr<AttributeCatalogEntry> AttributeCatalogTable::GetAttributeEntry(transaction::TransactionContext *txn,
                                                                                table_oid_t table_oid,
                                                                                const std::string &name) {
  std::vector<type::TransientValue> search_vec, ret_row;
  search_vec.push_back(type::TransientValueFactory::GetNull(type::TypeId::INTEGER));
  search_vec.push_back(type::TransientValueFactory::GetInteger(!table_oid));
  search_vec.push_back(type::TransientValueFactory::GetVarChar(name));
  ret_row = pg_attribute_hrw_->FindRow(txn, search_vec);
  if (ret_row.empty()) {
    return nullptr;
    // throw CATALOG_EXCEPTION("attribute doesn't exist");
  }
  col_oid_t oid(type::TransientValuePeeker::PeekInteger(ret_row[0]));
  return std::make_shared<AttributeCatalogEntry>(oid, pg_attribute_hrw_, std::move(ret_row));
}

void AttributeCatalogTable::DeleteEntries(transaction::TransactionContext *txn, table_oid_t table_oid) {
  // auto layout = pg_attribute_hrw_->GetLayout();
  int32_t col_index = pg_attribute_hrw_->ColNameToIndex("attrelid");

  auto it = pg_attribute_hrw_->begin(txn);
  while (it != pg_attribute_hrw_->end(txn)) {
    // storage::ProjectedColumns::RowView row_view = it->InterpretAsRow(layout, 0);
    storage::ProjectedColumns::RowView row_view = it->InterpretAsRow(0);
    // check if a matching row, delete if it is
    byte *col_p = row_view.AccessWithNullCheck(pg_attribute_hrw_->ColNumToOffset(col_index));
    if (col_p == nullptr) {
      continue;
    }
    auto col_int_value = *(reinterpret_cast<int32_t *>(col_p));
    if (static_cast<uint32_t>(col_int_value) == !table_oid) {
      // delete the entry
      pg_attribute_hrw_->GetSqlTable()->Delete(txn, *(it->TupleSlots()));
    }
    ++it;
  }
}

SqlTableHelper *AttributeCatalogTable::Create(transaction::TransactionContext *txn, Catalog *catalog, db_oid_t db_oid,
                                              const std::string &name) {
  catalog::SqlTableHelper *pg_attr;

  // get an oid
  table_oid_t pg_attr_oid(catalog->GetNextOid());

  // uninitialized storage
  pg_attr = new catalog::SqlTableHelper(pg_attr_oid);

  for (auto col : AttributeCatalogTable::schema_cols_) {
    pg_attr->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }

  // now actually create, with the provided schema
  pg_attr->Create();
  catalog->AddToMap(db_oid, CatalogTableType::ATTRIBUTE, pg_attr);
  return pg_attr;
}

}  // namespace terrier::catalog
