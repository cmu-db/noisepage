#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/index_handle.h"
#include "catalog/type_handle.h"

namespace terrier::catalog {

const std::vector<SchemaCol> IndexHandle::schema_cols_ = {
    {0, "indexrelid", type::TypeId::INTEGER},  {1, "indrelid", type::TypeId::INTEGER},
    {2, "indnatts", type::TypeId::INTEGER},    {3, "indnkeyatts", type::TypeId::INTEGER},
    {4, "indisunique", type::TypeId::BOOLEAN}, {5, "indisprimary", type::TypeId::BOOLEAN},
    {9, "indisvalid", type::TypeId::BOOLEAN},  {11, "indisready", type::TypeId::BOOLEAN},
    {12, "indislive", type::TypeId::BOOLEAN},
};

const std::vector<SchemaCol> IndexHandle::unused_schema_cols_ = {
    {6, "indisexclusion", type::TypeId::BOOLEAN},  {7, "indimmediate", type::TypeId::BOOLEAN},
    {8, "indisclustered", type::TypeId::BOOLEAN},  {10, "indcheckxmin", type::TypeId::BOOLEAN},
    {13, "indisreplident", type::TypeId::BOOLEAN},

    // FIXME(xueyuanz): the following SchemaCol types are not supported yet.
    // {14, "indkey", type::TypeId::},           // Should be of type int2vector
    // {15, "indcollation", type::TypeId::},     // Should be of type oidvector
    // {16, "indclass", type::TypeId::},         // Should be of type oidvector
    // {17, "indoption", type::TypeId::},        // Should be of type int2vector
    // {18, "indexprs", type::TypeId::},         // Should be of type pg_node_tree
    // {19, "indpred", type::TypeId::},          // Should be of type pg_node_tree
};

IndexHandle::IndexHandle(Catalog *catalog, std::shared_ptr<catalog::SqlTableRW> pg_index)
    : catalog_(catalog), pg_index_rw_(std::move(pg_index)) {}

std::shared_ptr<IndexHandle::IndexEntry> IndexHandle::GetIndexEntry(transaction::TransactionContext *txn,
                                                                    index_oid_t oid) {
  std::vector<type::TransientValue> search_vec, ret_row;
  search_vec.push_back(type::TransientValueFactory::GetInteger(!oid));
  ret_row = pg_index_rw_->FindRow(txn, search_vec);
  return std::make_shared<IndexEntry>(oid, std::move(ret_row));
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
  catalog_->SetUnusedColumns(&row, IndexHandle::unused_schema_cols_);
  pg_index_rw_->InsertRow(txn, row);
}

std::shared_ptr<catalog::SqlTableRW> IndexHandle::Create(transaction::TransactionContext *txn, Catalog *catalog,
                                                         db_oid_t db_oid, const std::string &name) {
  table_oid_t pg_index_oid(catalog->GetNextOid());
  std::shared_ptr<catalog::SqlTableRW> pg_index = std::make_shared<catalog::SqlTableRW>(pg_index_oid);

  // used columns
  for (auto col : schema_cols_) {
    pg_index->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }
  // unused columns
  for (auto col : unused_schema_cols_) {
    pg_index->DefineColumn(col.col_name, col.type_id, false, col_oid_t(catalog->GetNextOid()));
  }
  pg_index->Create();
  catalog->AddToMaps(db_oid, pg_index_oid, name, pg_index);

  return pg_index;
}

}  // namespace terrier::catalog
