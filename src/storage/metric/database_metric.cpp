#include "catalog/catalog_defs.h"
#include "storage/metric/database_metric.h"
#include "storage/metric/thread_level_stats_collector.h"
#include "transaction/transaction_manager.h"
#include "util/transaction_benchmark_util.h"

namespace terrier::storage::metric {

void DatabaseMetricRawData::UpdateAndPersist(transaction::TransactionManager *const txn_manager,
    catalog::Catalog *const catalog) {
  auto txn = txn_manager->BeginTransaction();
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog->GetDatabaseHandle();
  auto table_handle = db_handle.GetNamespaceHandle(txn, terrier_oid).GetTableHandle(txn, "public");
  auto table = table_handle.GetTable(txn, "database_metric_table");
  std::vector<type::TransientValue> row;
  
  for (auto &entry : counters_) {
    // one iteration per database
    unsigned int database_oid = static_cast<unsigned int>(entry.first);
    auto &counter = entry.second;
    uint64_t commit_cnt = counter.commit_cnt_;
    uint64_t abort_cnt = counter.abort_cnt_;

    std::vector<type::TransientValue> search_vec;
    search_vec.emplace_back(type::TransientValueFactory::GetInteger(database_oid));
    auto row = table->FindRow(txn, search_vec);
    if (row.size() <= 1) {
      // no entry exists for this database yet  
      row.clear();
      row.emplace_back(type::TransientValueFactory::GetInteger(database_oid));
      row.emplace_back(type::TransientValueFactory::GetInteger(commit_cnt));
      row.emplace_back(type::TransientValueFactory::GetInteger(abort_cnt));
      table->InsertRow(txn, row);
    } else {
      // update existing entry
      int old_commit_cnt = type::TransientValuePeeker::PeekInteger(row[1]);
      int old_abort_cnt = type::TransientValuePeeker::PeekInteger(row[2]);  
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
  }
  
  txn_manager->Commit(txn, TestCallbacks::EmptyCallback, nullptr);
}

}  // namespace terrier::storage::metric
