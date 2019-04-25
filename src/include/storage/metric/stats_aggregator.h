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
#include "util/transaction_test_util.h"

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
  /**
   * Create an internal table for storing collected database level statistics
   */
  void CreateDatabaseTable();

  /**
   * Create an internal table for storing collected transaction level statistics
  */
  void CreateTransactionTable();

  /**
   * Transaction manager of the system
   */
  transaction::TransactionManager *const txn_manager_;
  catalog::Catalog *const catalog_;
};

}  // namespace terrier::storage::metric
