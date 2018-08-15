//===----------------------------------------------------------------------===//
//
//                         Terrier
//
// stats_collector.h
//
// Identification: src/include/common/stats/stats_collector.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <tbb/concurrent_unordered_map.h>
#include <string>
#include <vector>
#include "common/macros.h"
#include "common/stats/abstract_stats.h"

#ifdef __APPLE__
#include <json/json.h>
#else
#include <jsoncpp/json/json.h>
#endif

namespace terrier::common {

/**
 *  StatsCollector aggregates counters as statistics from stats classes implementing
 *  AbstractStats. The target stats classes have to register themselves to this class,
 *  and also have to register counters. StatsCollector doesn't automatically collect
 *  counters for the efficiency. So if it would be necessary to print the latest counters,
 *  a method to collect all counters need to be called.
 */

class StatsCollector {
 public:
  StatsCollector() = default;

  /** @brief Register a stats object into the stats collection list.
   *  @param stats  stats object to be collected its counters.
   */
  void RegisterStats(AbstractStats *stats) { stats_vector_.push_back(stats); }

  /** @brief Deregister a stats object from the stats collection list.
   *  @param stats  stats object to be out of collecting stats.
   */
  void DeregisterStats(AbstractStats *stats) {
    for (auto itr = stats_vector_.begin(); itr < stats_vector_.end(); itr++) {
      if (*itr == stats) stats_vector_.erase(itr);
    }
  }

  /** @brief Register a counter and initialize it.
   *  @param name  counter name to be registered.
   */
  void RegisterCounter(const std::string &name) {
    if (counters_.find(name) == counters_.end()) {
      counters_[name] = 0;
    }
  }

  /** @brief Add value to counter by counter name
   *  @param name  Counter name to be incremented
   */
  void AddValue(const std::string &name, const int value) { counters_[name] += value; }

  /** @brief decrement counter by counter name
   *  @param name  Counter name to be decremented
   */
  void SubtractValue(const std::string &name, const int value) { counters_[name] -= value; }

  /** @brief clear counter by counter name
   *  @param name  Counter name to be decremented
   */
  void ClearCounter(const std::string &name) { counters_[name] = 0; }

  /** @brief clear all counters registered */
  void ClearCounters() {
    for (auto counter : counters_) {
      ClearCounter(counter.first);
    }
  }

  /** @brief collect all the latest counters' values from registered stats */
  void ColloectCounters() {
    for (auto stats : stats_vector_) {
      stats->SyncAllCounters();
    }
  }

  /** @brief get a counter by counter name */
  int GetCounter(const std::string &name) { return counters_[name]; }

  /** @brief Get the Json value about the statistics. */
  Json::Value GetStatsAsJson() {
    Json::Value json_value;
    for (auto counter : counters_) {
      json_value[counter.first] = Json::Value(counter.second);
    }
    return json_value;
  }

  /** @brief Print the statistics in the Json value. */
  void PrintStats() {
    Json::Value json_value = GetStatsAsJson();
    printf("The Json value about the statistics is shown below: \n%s", json_value.toStyledString().c_str());
  }

 private:
  /** Stats objects collected statistics */
  std::vector<AbstractStats *> stats_vector_;

  /** Counter map as statistics */
  tbb::concurrent_unordered_map<std::string, int> counters_;
};

}  // namespace terrier::common
