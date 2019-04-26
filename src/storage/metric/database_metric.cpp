#include "storage/metric/database_metric.h"
#include <vector>
#include "catalog/catalog_defs.h"
#include "storage/metric/thread_level_stats_collector.h"
#include "transaction/transaction_manager.h"

namespace terrier::storage::metric {

catalog::SqlTableHelper *DatabaseMetricRawData::GetStatsTable(transaction::TransactionManager *const txn_manager,
                                                          catalog::Catalog *const catalog, transaction::TransactionContext *txn) {
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog->GetDatabaseHandle();
  auto tablee = db_handle.GetNamespaceHandle(txn, terrier_oid);
  auto table_handle = tablee.GetTableHandle(txn, "public");

  // define schema
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog->GetNextOid()));
  cols.emplace_back("commit_num", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog->GetNextOid()));
  cols.emplace_back("abort_num", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog->GetNextOid()));
  catalog::Schema schema(cols);
  // create table
  auto table_ptr = table_handle.GetTable(txn, "database_metric_table");
  if (table_ptr == nullptr) table_ptr = table_handle.CreateTable(txn, schema, "database_metric_table");
  return table_ptr;
}

void DatabaseMetricRawData::UpdateAndPersist(transaction::TransactionManager *const txn_manager,
                                             catalog::Catalog *const catalog, transaction::TransactionContext *txn) {
  auto table = GetStatsTable(txn_manager, catalog, txn);
  TERRIER_ASSERT(table != nullptr, "Stats table cannot be nullptr.");
  for (auto &entry : data_) {
    // one iteration per database
    auto database_oid = static_cast<uint32_t>(entry.first);
    auto &counter = entry.second;
    auto commit_cnt = counter.commit_cnt_;
    auto abort_cnt = counter.abort_cnt_;
    
    std::vector<type::TransientValue> search_vec;
    search_vec.emplace_back(type::TransientValueFactory::GetInteger(database_oid));
    auto row = table->FindRow(txn, search_vec);

    if (row.empty()) {
      // no entry exists for this database yet
      row.clear();
      row.emplace_back(type::TransientValueFactory::GetInteger(database_oid));
      row.emplace_back(type::TransientValueFactory::GetInteger(commit_cnt));
      row.emplace_back(type::TransientValueFactory::GetInteger(abort_cnt));
      table->InsertRow(txn, row);
    } else {
      // update existing entry
      auto old_commit_cnt = type::TransientValuePeeker::PeekInteger(row[1]);
      auto old_abort_cnt = type::TransientValuePeeker::PeekInteger(row[2]);
      row.clear();
      row.emplace_back(type::TransientValueFactory::GetInteger(database_oid));
      row.emplace_back(type::TransientValueFactory::GetInteger(commit_cnt + old_commit_cnt));
      row.emplace_back(type::TransientValueFactory::GetInteger(abort_cnt + old_abort_cnt));
      auto proj_col_p = table->FindRowProjCol(txn, search_vec);
      auto tuple_slot_p = proj_col_p->TupleSlots();
      // delete
      table->GetSqlTable()->Delete(txn, *tuple_slot_p);
      delete[] reinterpret_cast<byte *>(proj_col_p);
      // insert
      table->InsertRow(txn, row);
    }
    row = table->FindRow(txn, search_vec);
  }
}

}  // namespace terrier::storage::metric
