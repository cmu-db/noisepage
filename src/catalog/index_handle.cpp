#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/index_handle.h"
#include "catalog/type_handle.h"
#include "catalog/catalog_sql_table.h"

namespace terrier::catalog {

const std::vector<SchemaCol>IndexHandle::schema_cols_ = {
    {0, true, "indexrelid", type::TypeId::INTEGER},
    {1, true, "indrelid", type::TypeId::INTEGER},
    {2, true, "indnatts", type::TypeId::INTEGER},
    {3, true, "indnkeyatts", type::TypeId::INTEGER},
    {4, true, "indisunique", type::TypeId::BOOLEAN},
    {5, true, "indisprimary", type::TypeId::BOOLEAN},
    {6, false, "indisexclusion", type::TypeId::BOOLEAN},
    {7, false, "indimmediate", type::TypeId::BOOLEAN},
    {8, false, "indisclustered", type::TypeId::BOOLEAN},
    {10, false, "indcheckxmin", type::TypeId::BOOLEAN},
    {9, true, "indisvalid", type::TypeId::BOOLEAN},
    {11, true, "indisready", type::TypeId::BOOLEAN},
    {12, true, "indislive", type::TypeId::BOOLEAN},
    {13, false, "indisreplident", type::TypeId::BOOLEAN},
    {14, false, "indkey", type::TypeId::BOOLEAN},           // Should be of type int2vector
    {15, false, "indcollation", type::TypeId::BOOLEAN},     // Should be of type oidvector
    {16, false, "indclass", type::TypeId::BOOLEAN},         // Should be of type oidvector
    {17, false, "indoption", type::TypeId::BOOLEAN},        // Should be of type int2vector
    {18, false, "indexprs", type::TypeId::BOOLEAN},         // Should be of type pg_node_tree
    {19, false, "indpred", type::TypeId::BOOLEAN}          // Should be of type pg_node_tree
};

IndexHandle::IndexHandle(catalog::SqlTableHelper* pg_index)
    : pg_index_rw_(pg_index) {}

std::shared_ptr<IndexEntry> IndexHandle::GetIndexEntry(transaction::TransactionContext *txn,
                                                                    index_oid_t oid) {
  std::vector<type::TransientValue> search_vec, ret_row;
  search_vec.push_back(type::TransientValueFactory::GetInteger(!oid));
  ret_row = pg_index_rw_->FindRow(txn, search_vec);
  return std::make_shared<IndexEntry>(oid, pg_index_rw_,std::move(ret_row));
}

void IndexHandle::AddEntry(transaction::TransactionContext *txn, index_oid_t indexrelid, table_oid_t indrelid,
                           int32_t indnatts, int32_t indnkeyatts, bool indisunique, bool indisprimary, bool indisvalid,
                           bool indisready, bool indislive) {
  std::vector<type::TransientValue> row;
  // FIXME(xueyuanz): Might be problematic since the columns are out of order.
  row.emplace_back(type::TransientValueFactory::GetInteger(!indexrelid));
  row.emplace_back(type::TransientValueFactory::GetInteger(!indrelid));
  row.emplace_back(type::TransientValueFactory::GetInteger(indnatts));
  row.emplace_back(type::TransientValueFactory::GetInteger(indnkeyatts));
  row.emplace_back(type::TransientValueFactory::GetBoolean(indisunique));
  row.emplace_back(type::TransientValueFactory::GetBoolean(indisprimary));
  row.emplace_back(type::TransientValueFactory::GetBoolean(indisvalid));
  row.emplace_back(type::TransientValueFactory::GetBoolean(indisready));
  row.emplace_back(type::TransientValueFactory::GetBoolean(indislive));
  pg_index_rw_->InsertRow(txn, row);
}

catalog::SqlTableHelper *IndexHandle::Create(transaction::TransactionContext *txn, Catalog *catalog,
                                                             db_oid_t db_oid, const std::string &name) {
  table_oid_t pg_index_oid(catalog->GetNextOid());
  catalog::SqlTableHelper *storage_table = new catalog::SqlTableHelper(pg_index_oid);

  // used columns
  for (auto col : schema_cols_) {
    storage_table->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }

  storage_table->Create();
  catalog->AddToMaps(db_oid, pg_index_oid, name, storage_table);

  return storage_table;
}

}  // namespace terrier::catalog
