#pragma once

#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/macros.h"
#include "storage/metric/abstract_raw_data.h"
#include "storage/metric/thread_level_stats_collector.h"

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
  explicit StatsAggregator(transaction::TransactionManager *txn_manager) : txn_manager_(txn_manager) {}

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
   * Transaction manager of the system
   */
  transaction::TransactionManager *const txn_manager_;
};

}  // namespace terrier::storage::metric
