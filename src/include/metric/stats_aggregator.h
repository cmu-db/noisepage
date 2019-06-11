#pragma once

#include <fstream>
#include <map>

#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/schema.h"
#include "common/macros.h"
#include "metric/abstract_raw_data.h"
#include "metric/thread_level_stats_collector.h"
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
   * Aggregate metrics from all threads which have collected stats,
   * combine with what was previously persisted in internal SQL tables
   * and insert new total into SQLtable.
   *
   * @warning this method should be called before manipulating the worker pool, especially if
   * some of the worker threads are reassigned to tasks other than execution.
   */
  void Aggregate();

  /**
   * Worker method for Aggregate() that performs stats collection
   * @return raw data collected from all threads
   */
  std::vector<AbstractRawData *> AggregateRawData();

 private:
};

}  // namespace storage::metric
}  // namespace terrier
