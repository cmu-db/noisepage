#pragma once

#include <condition_variable>
#include <fstream>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/dedicated_thread_task.h"
#include "common/macros.h"
#include "stats/abstract_raw_data.h"
#include "stats/thread_level_stats_collector.h"

namespace terrier::stats {

class StatsAggregator : public DedicatedThreadTask {
  /**
   * Per-thread stats aggregator
   */
 public:
  /**
   * Instantiate a new stats collector
   * @param aggregation_interval time interval in ms between successive stats collection
   */
  explicit StatsAggregator(int64_t aggregation_interval) : aggregation_interval_ms_(aggregation_interval) {}

  void Terminate() override;

  void RunTask() override;

  /**
   * Aggregate metrics from all threads which have collected stats,
   * combine with what was previously in catalog
   * and insert new total into catalog
   */
  void Aggregate();

  std::vector<std::shared_ptr<AbstractRawData>> AggregateRawData();

 private:
  /**
   * Interval for stats collcection
   */
  int64_t aggregation_interval_ms_;
  /**
   * mutex for aggregate task scheduling. No conflict generally
   */
  std::mutex mutex_;
  /**
   * Condition variable for notifying a finished execution
   */
  std::condition_variable exec_finished_;
  /**
   * True if this thread is terminatin
   */
  bool exiting_ = false;
};

}  // namespace terrier::stats
