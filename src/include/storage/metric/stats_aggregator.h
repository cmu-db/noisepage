#pragma once

#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/namespace_handle.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "storage/metric/abstract_raw_data.h"
#include "storage/metric/thread_level_stats_collector.h"
#include "util/transaction_benchmark_util.h"

namespace terrier::storage::metric {

/**
 * Background thread that periodically collects data from thread level collectors
 */
class StatsAggregator {
  /**
   * Per-thread stats aggregator
   */
 public:
  /**
   * Instantiate a new stats collector
   * @param txn_manager transaction manager of the system for persisting collected data
   */
  explicit StatsAggregator(transaction::TransactionManager *txn_manager, catalog::Catalog *catalog)
      : txn_manager_(txn_manager), catalog_(catalog) {}

  /**
   * Aggregate metrics from all threads which have collected stats,
   * combine with what was previously persisted in internal SQL tables
   * and insert new total into SQLtable
   */
  void Aggregate();

  /**
   * Worker method for Aggregate() that performs stats collection
   * @return raw data collected from all threads
   */
  std::vector<std::shared_ptr<AbstractRawData>> AggregateRawData();

  /**
   * @return the txn_manager of the system
   */
  transaction::TransactionManager *GetTxnManager() { return txn_manager_; }

 private:
  void CreateDatabaseTable() {
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
  /**
   * Transaction manager of the system
   */
  transaction::TransactionManager *const txn_manager_;
  catalog::Catalog *const catalog_;
};

}  // namespace terrier::storage::metric
