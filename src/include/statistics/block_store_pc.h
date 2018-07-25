//===----------------------------------------------------------------------===//
//
//                         Terrier
//
// block_store_stats.h
//
// Identification: src/include/statistics/block_store_stats.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "performance_counters.h"

#ifndef NDEBUG

namespace terrier {

template <typename T> struct DefaultConstructorAllocator;
template <typename T, class Allocator> class ObjectPool;

namespace storage {
class RawBlock;
using BlockStore = ObjectPool<RawBlock, DefaultConstructorAllocator<RawBlock>>;
}

namespace statistics {

/**
 *  Performance counters class for the block store.
 *
 *  This class specializes PerformanceCounters class, and works for collecting
 *  performance stuff in ObjectPool within object_pool.h through storage::BlockStore
 *  within storage_defs.h.
 **/

template <>
class PerformanceCounters<storage::BlockStore> {
 public:
  PerformanceCounters() {};

  /** @brief increment counter by counter name
   *  @param name  Counter name to be incremented
   */
  void IncrementCounter(std::string name) { counters_[name]++;};

  /** @brief decrement counter by counter name
   *  @param name  Counter name to be decremented
   */
  void DecrementCounter(std::string name) { counters_[name]--;};

  /** @brief Print the statistics in the Json value. */
  void PrintPerformanceCounters();

 private:
  /** @brief Get the Json value about the statistics. */
  Json::Value GetStatsAsJson();

  /** Counter map as statistics */
  tbb::concurrent_unordered_map<std::string, int> counters_;
};


}  // namespace statistics
}  // namespace terrier

#endif

