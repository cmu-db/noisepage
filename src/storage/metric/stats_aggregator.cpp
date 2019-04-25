#include "storage/metric/stats_aggregator.h"
#include <memory>
#include <vector>

namespace terrier::storage::metric {

using RawDataCollect = std::vector<std::shared_ptr<AbstractRawData>>;
RawDataCollect StatsAggregator::AggregateRawData() {
  RawDataCollect acc = std::vector<std::shared_ptr<AbstractRawData>>();
  auto collector_map = ThreadLevelStatsCollector::GetAllCollectors();
  for (auto iter = collector_map.Begin(); iter != collector_map.End(); ++iter) {
    auto data_block = iter->second->GetDataToAggregate();
    if (acc.empty()) {
      acc = data_block;
    } else {
      for (size_t i = 0; i < data_block.size(); i++) {
        acc[i]->Aggregate(data_block[i].get());
      }
    }
  }
  return acc;
}

void StatsAggregator::Aggregate() {
  auto acc = AggregateRawData();
  for (auto &raw_data : acc) {
    raw_data->UpdateAndPersist(txn_manager_, catalog_);
  }
}

void StatsAggregator::CreateDatabaseTable() {
  auto txn = txn_manager_->BeginTransaction();
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto table_handle = db_handle.GetNamespaceHandle(txn, terrier_oid).GetTableHandle(txn, "public");

  // define schema
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("commit_num", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("abort_num", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  catalog::Schema schema(cols);

  // create table
  auto table_ptr = table_handle.GetTable(txn, "database_metric_table");
  if (table_ptr == nullptr) table_handle.CreateTable(txn, schema, "database_metric_table");
  txn_manager_->Commit(txn, TestCallbacks::EmptyCallback, nullptr);
}

void StatsAggregator::CreateTransactionTable() {
  auto txn = txn_manager_->BeginTransaction();
  const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
  auto db_handle = catalog_->GetDatabaseHandle();
  auto table_handle = db_handle.GetNamespaceHandle(txn, terrier_oid).GetTableHandle(txn, "public");

  // define schema
  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("id", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("latency", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("tuple_read", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("tuple_insert", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("tuple_delete", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  cols.emplace_back("tuple_update", type::TypeId::INTEGER, false, catalog::col_oid_t(catalog_->GetNextOid()));
  catalog::Schema schema(cols);

  // create table
  auto table_ptr = table_handle.GetTable(txn, "txn_metric_table");
  if (table_ptr == nullptr) table_handle.CreateTable(txn, schema, "txn_metric_table");
  txn_manager_->Commit(txn, TestCallbacks::EmptyCallback, nullptr);
}

}  // namespace terrier::storage::metric
