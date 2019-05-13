#include "storage/metric/transaction_metric.h"
#include <vector>
#include "catalog/catalog_defs.h"
#include "storage/metric/thread_level_stats_collector.h"
#include "transaction/transaction_manager.h"
#include "util/transaction_test_util.h"

namespace terrier::storage::metric {

catalog::SqlTableHelper *TransactionMetricRawData::GetStatsTable(transaction::TransactionManager *const txn_manager,
                                                                 catalog::Catalog *const catalog,
                                                                 transaction::TransactionContext *txn) {
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog->GetDatabaseHandle();
  auto table_handle = db_handle.GetNamespaceTable(txn, terrier_oid).GetTableHandle(txn, "public");

  // define schema
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::BIGINT, false, catalog::col_oid_t(catalog->GetNextOid()));
  cols.emplace_back("latency", type::TypeId::BIGINT, false, catalog::col_oid_t(catalog->GetNextOid()));
  cols.emplace_back("tuple_read", type::TypeId::BIGINT, false, catalog::col_oid_t(catalog->GetNextOid()));
  cols.emplace_back("tuple_insert", type::TypeId::BIGINT, false, catalog::col_oid_t(catalog->GetNextOid()));
  cols.emplace_back("tuple_delete", type::TypeId::BIGINT, false, catalog::col_oid_t(catalog->GetNextOid()));
  cols.emplace_back("tuple_update", type::TypeId::BIGINT, false, catalog::col_oid_t(catalog->GetNextOid()));
  catalog::Schema schema(cols);

  // create table
  auto table_ptr = table_handle.GetTable(txn, "txn_metric_table");
  if (table_ptr == nullptr) table_ptr = table_handle.CreateTable(txn, schema, "txn_metric_table");
  return table_ptr;
}

void TransactionMetricRawData::UpdateAndPersist(transaction::TransactionManager *const txn_manager,
                                                catalog::Catalog *catalog, transaction::TransactionContext *txn) {
  auto table = GetStatsTable(txn_manager, catalog, txn);
  TERRIER_ASSERT(table != nullptr, "Stats table cannot be nullptr.");
  std::vector<type::TransientValue> row;

  for (auto &entry : data_) {
    // one iteration per transaction
    auto txn_oid = static_cast<uint64_t>(entry.first);
    auto &counter = entry.second;
    auto latency = counter.latency_;
    auto tuple_read = counter.tuple_read_;
    auto tuple_insert = counter.tuple_insert_;
    auto tuple_delete = counter.tuple_delete_;
    auto tuple_update = counter.tuple_update_;

    std::vector<type::TransientValue> search_vec;
    search_vec.emplace_back(type::TransientValueFactory::GetBigInt(txn_oid));
    auto row = table->FindRow(txn, search_vec);
    if (row.size() <= 1) {
      // no entry exists for this transaction yet
      row.clear();
      row.emplace_back(type::TransientValueFactory::GetBigInt(txn_oid));
      row.emplace_back(type::TransientValueFactory::GetBigInt(latency));
      row.emplace_back(type::TransientValueFactory::GetBigInt(tuple_read));
      row.emplace_back(type::TransientValueFactory::GetBigInt(tuple_insert));
      row.emplace_back(type::TransientValueFactory::GetBigInt(tuple_delete));
      row.emplace_back(type::TransientValueFactory::GetBigInt(tuple_update));
      table->InsertRow(txn, row);
    } else {
      TERRIER_ASSERT(false, "There should not be update of the transaction data.");
    }
  }
}

}  // namespace terrier::storage::metric
