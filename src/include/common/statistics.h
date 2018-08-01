//===----------------------------------------------------------------------===//
//
//                         Terrier
//
// statistics.h
//
// Identification: src/include/common/statistics.h
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

/**
 * Contains statistics about database operations.
 */
class Statistics {
 public:
  virtual ~Statistics() {}

  /** @brief Set the Json value about the statistics. */
  virtual void SetStats() = 0;

  /** @brief Print the statistics in the Json value. */
  void PrintStats();

 protected:
  /** The Json value about the statistics */
  Json::Value json_value_;
};

}  // namespace terrier
