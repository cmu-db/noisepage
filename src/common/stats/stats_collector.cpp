//===----------------------------------------------------------------------===//
//
//                         Terrier
//
// stats_collector.h
//
// Identification: src/common/stats/stats_collector.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/stats/abstract_stats.h"

#include "common/stats/stats_collector.h"

namespace terrier::common {

  AbstractStats::AbstractStats(StatsCollector *stats_collector) : stats_collector_(stats_collector) {
    stats_collector->RegisterStats(this);
  }

  AbstractStats::~AbstractStats() {
    stats_collector_->DeregisterStats(this);
  }

}  // namespace terrier::common


