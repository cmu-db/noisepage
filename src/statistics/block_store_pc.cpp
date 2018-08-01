//===----------------------------------------------------------------------===//
//
//                         Terrier
//
// block_store_pc.cpp
//
// Identification: src/statistics/block_store_pc.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#ifndef NDEBUG

#include <sstream>
#include <iostream>

#include "statistics/block_store_pc.h"

namespace terrier {
namespace statistics {

void PerformanceCounters<storage::BlockStore>::PrintPerformanceCounters(){
  Json::Value json_value = GetStatsAsJson();
  std::cout << "The Json value about the statistics is shown below: " << std::endl;
  std::cout << json_value.toStyledString();
}

Json::Value PerformanceCounters<storage::BlockStore>::GetStatsAsJson(){
  Json::Value json_value;
  for (auto counter : counters_) {
    json_value[counter.first] = Json::Value(counter.second);
  }
  return json_value;
}

}  // namespace statistics
}  // namespace terrier

#endif
