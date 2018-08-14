//===----------------------------------------------------------------------===//
//
//                         Terrier
//
// abstract_stats.h
//
// Identification: src/include/common/stats/abstract_stats.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace terrier::common {

class StatsCollector;

class AbstractStats {
 public:
  AbstractStats() = delete;

  AbstractStats(StatsCollector *stats_collector);

  virtual ~AbstractStats();

  virtual void SyncAllCounters() = 0;

 protected:
  StatsCollector *stats_collector_;
};

}  // namespace terrier::common

