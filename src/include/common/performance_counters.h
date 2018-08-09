//===----------------------------------------------------------------------===//
//
//                         Terrier
//
// statistics.h
//
// Identification: src/include/statistics/statistics.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <tbb/concurrent_unordered_map.h>
#include <string>
#include "common/macros.h"

#ifdef __APPLE__
#include <json/json.h>
#else
#include <jsoncpp/json/json.h>
#endif

namespace terrier::common {

/**
 *  Basic performance counter as statistics
 **/

class PerformanceCounters {
 public:
  PerformanceCounters() = default;

  /** @brief increment counter by counter name
   *  @param name  Counter name to be incremented
   */
  void IncrementCounter(const std::string &name) { counters_[name]++; }

  /** @brief decrement counter by counter name
   *  @param name  Counter name to be decremented
   */
  void DecrementCounter(const std::string &name) { counters_[name]--; }

  /** @brief Print the statistics in the Json value. */
  void PrintPerformanceCounters() {
    Json::Value json_value = GetStatsAsJson();
    std::cout << "The Json value about the statistics is shown below: " << std::endl;
    std::cout << json_value.toStyledString();
  }

 private:
  /** @brief Get the Json value about the statistics. */
  Json::Value GetStatsAsJson() {
    Json::Value json_value;
    for (auto counter : counters_) {
      json_value[counter.first] = Json::Value(counter.second);
    }
    return json_value;
  }

  /** Counter map as statistics */
  tbb::concurrent_unordered_map<std::string, int> counters_;
};

}  // namespace terrier::common
