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

class ObjectPoolStats : public AbstractStats {
 public:
  ObjectPoolStats() = delete;

  ObjectPoolStats(StatsCollector *stats_collector) : AbstractStats(stats_collector) {
    stats_collector_->RegisterCounter(create_block_counter_name_);
    stats_collector_->RegisterCounter(reuse_block_counter_name_);
  }

  ~ObjectPoolStats() {
    SyncAllCounters();
  }

  void IncrementCreateBlockCounter() { create_block_counter_++; };

  void IncrementReuseBlockCounter() { reuse_block_counter_++; }

  void ClearCreateBlockCounter() { create_block_counter_ = 0; }

  void ClearReuseBlockCounter() { reuse_block_counter_ = 0; }

  void SyncAllCounters() {
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



