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

/**
 * AbstractStats is a virtual stats class for diverse statistics classes.
 *   e.g. for statistics of memory, network or transaction usage.
 * This has to register a StatsCollector with which synchronizes counters defined in a class
 * inheriting this. Besides, the class has to implement the synchronizing method.
 */

class AbstractStats {
 public:
  AbstractStats() = delete;

  /** @brief Register a stats collector as initialization. A inheritance class has to add
   *         registration of the counters collected.
   *  @param stats_collector  stats collector which this class send the counters to.
   */
  explicit AbstractStats(StatsCollector *stats_collector);

  /** @brief Deregister itself from the stats collector. A inheritance class should add
   *         synchronization with the stats collector before local counter values are disappered.
   */
  virtual ~AbstractStats();

  /** @brief synchronize all counters with the stats collector */
  virtual void SyncAllCounters() = 0;

 protected:
  StatsCollector *stats_collector_;
};

}  // namespace terrier::common
