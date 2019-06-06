#pragma once

#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/schema.h"
#include "common/macros.h"
#include "storage/metric/abstract_raw_data.h"
#include "storage/metric/thread_level_stats_collector.h"
#include "util/transaction_test_util.h"

namespace terrier {

namespace settings {
class SettingsManager;
}  // namespace settings

namespace storage::metric {

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
   * @param catalog catalog of the system for SqlTable lookups/creation
   * @param settings_manager settings manager of the system for retrieving relevant settings
   */
  explicit StatsAggregator(transaction::TransactionManager *txn_manager, settings::SettingsManager *settings_manager)
      : txn_manager_(txn_manager), settings_manager_(settings_manager) {}

  /**
   * Aggregate metrics from all threads which have collected stats,
   * combine with what was previously persisted in internal SQL tables
   * and insert new total into SQLtable.
   *
   * @warning this method should be called before manipulating the worker pool, especially if
   * some of the worker threads are reassigned to tasks other than execution.
   *
   * @param txn transaction context used for data aggregation and persistence
   */
  void Aggregate(transaction::TransactionContext *txn);

  /**
   * Worker method for Aggregate() that performs stats collection
   * @return raw data collected from all threads
   */
  std::vector<std::shared_ptr<AbstractRawData>> AggregateRawData();

  /**
   * @return txn_manager of the system
   */
  transaction::TransactionManager *const GetTxnManager() { return txn_manager_; }

  /**
   * @return settings manager of the system
   */
  settings::SettingsManager *const GetSettingsManager() { return settings_manager_; }

 private:
  /**
   * Transaction manager of the system
   */
  transaction::TransactionManager *const txn_manager_;

  /**
   * Settings manager of the system
   */
  settings::SettingsManager *const settings_manager_;
};

}  // namespace storage::metric
}  // namespace terrier
