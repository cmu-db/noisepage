//===----------------------------------------------------------------------===//
//
//                         Terrier
//
// object_pool_stats.h
//
// Identification: src/include/common/stats/object_pool_stats.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <string>

#include "common/stats/abstract_stats.h"
#include "common/stats/stats_collector.h"

namespace terrier::common {

/**
 * Inheritance class of AbstractStats for ObjectPool stats
 */

class ObjectPoolStats : public AbstractStats {
 public:
  ObjectPoolStats() = delete;

  /** @brief Register a stats collector and register itself to the stats collector.
   *  @param stats_collector  stats collector which this class send the counters to.
   */
  explicit ObjectPoolStats(StatsCollector *stats_collector) : AbstractStats(stats_collector) {
    stats_collector_->RegisterCounter(create_block_counter_name_);
    stats_collector_->RegisterCounter(reuse_block_counter_name_);
  }

  /** @brief synchronize with the stats collector and deregister itself from the stats collector. */
  ~ObjectPoolStats() override { SyncAllCounters(); }

  /** @brief increment create block couter. */
  void IncrementCreateBlockCounter() { create_block_counter_++; };

  /** @brief increment reuse block couter. */
  void IncrementReuseBlockCounter() { reuse_block_counter_++; }

  /** @brief clear create block couter. */
  void ClearCreateBlockCounter() { create_block_counter_ = 0; }

  /** @brief clear create block couter. */
  void ClearReuseBlockCounter() { reuse_block_counter_ = 0; }

  /** @brief synchronize all counters with the stats collector */
  void SyncAllCounters() override {
    // Write create block counter's value to the collector and clear it.
    stats_collector_->AddValue(create_block_counter_name_, create_block_counter_);
    ClearCreateBlockCounter();
    // Write reuse block counter's value to the collector and clear it.
    stats_collector_->AddValue(reuse_block_counter_name_, reuse_block_counter_);
    ClearReuseBlockCounter();
  }

 private:
  // counters as statistics
  std::atomic<int> create_block_counter_ = ATOMIC_VAR_INIT(0);
  std::string create_block_counter_name_ = "create block";
  std::atomic<int> reuse_block_counter_ = ATOMIC_VAR_INIT(0);
  std::string reuse_block_counter_name_ = "reuse block";
};

}  // namespace terrier::common
