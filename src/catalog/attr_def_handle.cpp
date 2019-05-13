#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/attr_def_handle.h"
#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"

namespace terrier::catalog {

const std::vector<SchemaCol> AttrDefCatalogTable::schema_cols_ = {{0, true, "oid", type::TypeId::INTEGER},
                                                                  {1, true, "adrelid", type::TypeId::INTEGER},
                                                                  {2, true, "adnum", type::TypeId::INTEGER},
                                                                  {3, true, "adbin", type::TypeId::VARCHAR},
                                                                  {4, false, "adsrc", type::TypeId::VARCHAR}};

// Find entry with (row) oid and return it
std::shared_ptr<AttrDefCatalogEntry> AttrDefCatalogTable::GetAttrDefEntry(transaction::TransactionContext *txn,
                                                                          col_oid_t oid) {
  std::vector<type::TransientValue> search_vec, ret_row;
  search_vec.push_back(type::TransientValueFactory::GetInteger(!oid));
  ret_row = pg_attrdef_rw_->FindRow(txn, search_vec);
  if (ret_row.empty()) {
    return nullptr;
  }
  return std::make_shared<AttrDefCatalogEntry>(oid, pg_attrdef_rw_, std::move(ret_row));
}

void AttrDefCatalogTable::DeleteEntries(transaction::TransactionContext *txn, table_oid_t table_oid) {
  // auto layout = pg_attrdef_rw_->GetLayout();
  int32_t col_index = pg_attrdef_rw_->ColNameToIndex("adrelid");

  auto it = pg_attrdef_rw_->begin(txn);
  while (it != pg_attrdef_rw_->end(txn)) {
    // storage::ProjectedColumns::RowView row_view = it->InterpretAsRow(layout, 0);
    storage::ProjectedColumns::RowView row_view = it->InterpretAsRow(0);
    // check if a matching row, delete if it is
    byte *col_p = row_view.AccessWithNullCheck(pg_attrdef_rw_->ColNumToOffset(col_index));
    if (col_p == nullptr) {
      continue;
    }
    auto col_int_value = *(reinterpret_cast<int32_t *>(col_p));
    if (static_cast<uint32_t>(col_int_value) == !table_oid) {
      // delete the entry
      pg_attrdef_rw_->GetSqlTable()->Delete(txn, *(it->TupleSlots()));
    }
    ++it;
  }
}

SqlTableHelper *AttrDefCatalogTable::Create(transaction::TransactionContext *txn, Catalog *catalog, db_oid_t db_oid,
                                            const std::string &name) {
  catalog::SqlTableHelper *pg_attrdef;

  // get an oid
  table_oid_t pg_attrdef_oid(catalog->GetNextOid());

  // uninitialized storage
  pg_attrdef = new catalog::SqlTableHelper(pg_attrdef_oid);

  for (auto col : AttrDefCatalogTable::schema_cols_) {
    pg_attrdef->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }

  // now actually create, with the provided schema
  pg_attrdef->Create();
  catalog->AddToMap(db_oid, CatalogTableType::ATTRDEF, pg_attrdef);
  return pg_attrdef;
}

}  // namespace terrier::catalog
