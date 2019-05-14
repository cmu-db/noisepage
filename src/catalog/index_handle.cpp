#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/catalog_sql_table.h"
#include "catalog/index_handle.h"
#include "catalog/type_handle.h"

namespace terrier::catalog {

const std::vector<SchemaCol> IndexCatalogTable::schema_cols_ = {
    {20, true, "indexptr", type::TypeId::BIGINT},
    {0, true, "indexrelid", type::TypeId::INTEGER},
    {21, true, "indexname", type::TypeId::VARCHAR},
    {1, true, "indrelid", type::TypeId::INTEGER},
    {2, true, "indnatts", type::TypeId::INTEGER},
    {3, true, "indnkeyatts", type::TypeId::INTEGER},
    {4, true, "indisunique", type::TypeId::BOOLEAN},
    {5, true, "indisprimary", type::TypeId::BOOLEAN},
    {9, true, "indisvalid", type::TypeId::BOOLEAN},
    {11, true, "indisready", type::TypeId::BOOLEAN},
    {12, true, "indislive", type::TypeId::BOOLEAN},
    {21, true, "indisblocking", type::TypeId::BOOLEAN},
    {6, false, "indisexclusion", type::TypeId::BOOLEAN},
    {7, false, "indimmediate", type::TypeId::BOOLEAN},
    {8, false, "indisclustered", type::TypeId::BOOLEAN},
    {10, false, "indcheckxmin", type::TypeId::BOOLEAN},
    {13, false, "indisreplident", type::TypeId::BOOLEAN},
    {14, false, "indkey", type::TypeId::BOOLEAN},        // Should be of type int2vector
    {15, false, "indcollation", type::TypeId::BOOLEAN},  // Should be of type oidvector
    {16, false, "indclass", type::TypeId::BOOLEAN},      // Should be of type oidvector
    {17, false, "indoption", type::TypeId::BOOLEAN},     // Should be of type int2vector
    {18, false, "indexprs", type::TypeId::BOOLEAN},      // Should be of type pg_node_tree
    {19, false, "indpred", type::TypeId::BOOLEAN}        // Should be of type pg_node_tree
};

IndexCatalogTable::IndexCatalogTable(Catalog *catalog, catalog::SqlTableHelper *pg_index)
    : catalog_(catalog), pg_index_rw_(pg_index) {}

std::shared_ptr<IndexCatalogEntry> IndexCatalogTable::GetIndexEntry(transaction::TransactionContext *txn,
                                                                    index_oid_t oid) {
  std::vector<type::TransientValue> search_vec, ret_row;
  search_vec.emplace_back(type::TransientValueFactory::GetNull(type::TypeId::BIGINT));
  search_vec.emplace_back(type::TransientValueFactory::GetInteger(!oid));
  ret_row = pg_index_rw_->FindRow(txn, search_vec);
  if (ret_row.empty()) {
    return nullptr;
  }
  return std::make_shared<IndexCatalogEntry>(oid, pg_index_rw_, std::move(ret_row));
}

std::shared_ptr<IndexCatalogEntry> IndexCatalogTable::GetIndexEntry(transaction::TransactionContext *txn,
                                                                    const std::string &index_name) {
  std::vector<type::TransientValue> search_vec, ret_row;
  search_vec.push_back(type::TransientValueFactory::GetNull(type::TypeId::BIGINT));
  search_vec.push_back(type::TransientValueFactory::GetNull(type::TypeId::INTEGER));
  search_vec.push_back(type::TransientValueFactory::GetVarChar(index_name));
  ret_row = pg_index_rw_->FindRow(txn, search_vec);
  if (ret_row.empty()) {
    return nullptr;
  }
  index_oid_t oid(type::TransientValuePeeker::PeekInteger(ret_row[0]));
  return std::make_shared<IndexCatalogEntry>(oid, pg_index_rw_, std::move(ret_row));
}

void IndexCatalogTable::AddEntry(transaction::TransactionContext *txn, storage::index::Index *index_ptr,
                                 index_oid_t indexrelid, const std::string &indexname, table_oid_t indrelid,
                                 int32_t indnatts, int32_t indnkeyatts, bool indisunique, bool indisprimary,
                                 bool indisvalid, bool indisready, bool indislive, bool indisblocking) {
  std::vector<type::TransientValue> row;
  // FIXME(xueyuanz): Might be problematic since the columns are out of order.
  row.emplace_back(type::TransientValueFactory::GetBigInt(reinterpret_cast<int64_t>(index_ptr)));
  row.emplace_back(type::TransientValueFactory::GetInteger(!indexrelid));
  row.emplace_back(type::TransientValueFactory::GetVarChar(indexname));
  row.emplace_back(type::TransientValueFactory::GetInteger(!indrelid));
  row.emplace_back(type::TransientValueFactory::GetInteger(indnatts));
  row.emplace_back(type::TransientValueFactory::GetInteger(indnkeyatts));
  row.emplace_back(type::TransientValueFactory::GetBoolean(indisunique));
  row.emplace_back(type::TransientValueFactory::GetBoolean(indisprimary));
  row.emplace_back(type::TransientValueFactory::GetBoolean(indisvalid));
  row.emplace_back(type::TransientValueFactory::GetBoolean(indisready));
  row.emplace_back(type::TransientValueFactory::GetBoolean(indislive));
  row.emplace_back(type::TransientValueFactory::GetBoolean(indisblocking));
  catalog_->SetUnusedColumns(&row, IndexCatalogTable::schema_cols_);
  pg_index_rw_->InsertRow(txn, row);
}

catalog::SqlTableHelper *IndexCatalogTable::Create(transaction::TransactionContext *txn, Catalog *catalog,
                                                   db_oid_t db_oid, const std::string &name) {
  table_oid_t pg_index_oid(catalog->GetNextOid());
  auto *storage_table = new catalog::SqlTableHelper(pg_index_oid);

  // used columns
  for (auto col : schema_cols_) {
    storage_table->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }

  storage_table->Create();
  catalog->AddToMap(db_oid, CatalogTableType::INDEX, storage_table);

  return storage_table;
}

bool IndexCatalogTable::DeleteEntry(transaction::TransactionContext *txn,
                                    const std::shared_ptr<IndexCatalogEntry> &entry) {
  std::vector<type::TransientValue> search_vec;
  // get the oid of this row
  auto indexrel_oid = entry->GetIntegerColumn("indexrelid");

  search_vec.emplace_back(type::TransientValueFactory::GetNull(type::TypeId::BIGINT));
  search_vec.emplace_back(type::TransientValueFactory::GetInteger(indexrel_oid));

  // lookup and get back the projected column. Recover the tuple_slot
  auto proj_col_p = pg_index_rw_->FindRowProjCol(txn, search_vec);
  auto tuple_slot_p = proj_col_p->TupleSlots();
  // delete
  bool status = pg_index_rw_->GetSqlTable()->Delete(txn, *tuple_slot_p);
  delete[] reinterpret_cast<byte *>(proj_col_p);
  return status;
}

void IndexCatalogTable::SetEntryColumn(transaction::TransactionContext *txn, index_oid_t indexreloid,
                                       const std::string &col, const type::TransientValue &value) {
  std::shared_ptr<IndexCatalogEntry> entry = GetIndexEntry(txn, indexreloid);
  DeleteEntry(txn, entry);
  std::vector<type::TransientValue> new_values;
  new_values.reserve(schema_cols_.size());
  int32_t col_id = pg_index_rw_->ColNameToIndex(col);
  for (size_t i = 0; i < schema_cols_.size(); i++) {
    if (static_cast<int32_t>(i) == col_id) {
      new_values.emplace_back(type::TransientValueFactory::GetCopy(value));
    } else {
      new_values.emplace_back(type::TransientValueFactory::GetCopy(entry->GetColumn(static_cast<int32_t>(i))));
    }
  }
  pg_index_rw_->InsertRow(txn, new_values);
}

}  // namespace terrier::catalog
