//===----------------------------------------------------------------------===//
//
//                         Terrier
//
// performance_counters.h
//
// Identification: src/include/common/performance_counters.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>

#ifdef __APPLE__
#include <json/json.h>
#else
#include <jsoncpp/json/json.h>
#endif

namespace terrier {

//===--------------------------------------------------------------------===//
// Performance Counters
//===--------------------------------------------------------------------===//

/**
 * Stateless interface to allow printing of performance counters
 *
 * Most stateful classes should implement this interface and return useful
 * information about its performance.
 */
class PerformanceCounters {
 public:
  virtual ~PerformanceCounters(){};

  /** @brief Set the Json value about the performance counters. */
  Json::Value GetPerformanceCounters() {
    Json::Value performance_counters;
    performance_counters["status"] = "not implemented";
    return performance_counters;
  };

  void PrintPerformanceCounters() {
    std::cout << "The performance counters are shown below: " << std::endl
              << GetPerformanceCounters() << std::endl;
    return;
  }
};

}  // namespace terrier
