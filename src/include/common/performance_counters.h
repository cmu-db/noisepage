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

#include <iosfwd>
#include <string>

#ifdef __APPLE__
#include <json/json.h>
#else
#include <jsoncpp/json/json.h>
#endif

namespace terrier {

//===--------------------------------------------------------------------===//
// Statistics Object
//===--------------------------------------------------------------------===//

class PerformanceCounters {
 public:
  virtual ~PerformanceCounters(){};

  /** @brief Set the Json value about the performance counters. */
  virtual Json::Value GetPerformanceCounters() = 0;
};

}  // namespace terrier
