#pragma once

#include <memory>
#include <vector>

#include "metric/abstract_raw_data.h"
#include "metric/thread_level_stats_collector.h"

namespace terrier::metric {

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
  std::vector<std::unique_ptr<AbstractRawData>> AggregateRawData();
};

}  // namespace terrier::metric
