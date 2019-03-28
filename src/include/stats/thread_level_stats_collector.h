#pragma once

#include <thread>
#include <unordered_map>
#include "catalog/catalog_defs.h"
#include "stats/abstract_metric.h"
#include "stats/abstract_raw_data.h"
#include "stats/statistic_defs.h"

namespace terrier {

namespace transaction {
class TransactionContext;
}  // namespace concurrency

namespace stats {
/**
 * @brief Class responsible for collecting raw data on a single thread.
 *
 * Each thread will be assigned one collector that is globally unique. This is
 * to ensure that we can collect raw data in an non-blocking way as the
 * collection code runs on critical query path. Periodically a dedicated aggregator thread
 * will put the data from all collectors together into a meaningful form.
 */
class ThreadLevelStatsCollector {
 public:
  using CollectorsMap =
      tbb::concurrent_unordered_map<std::thread::id, ThreadLevelStatsCollector, std::hash<std::thread::id>>;
  /**
   * @return the Collector for the calling thread
   */
  static ThreadLevelStatsCollector &GetCollectorForThread() {
    std::thread::id tid = std::this_thread::get_id();
    return collector_map_[tid];
  }

  /**
   * @return A mapping from each thread to their assigned Collector
   */
  static CollectorsMap &GetAllCollectors() { return collector_map_; };

  /**
   * @brief Constructor of collector
   */
  ThreadLevelStatsCollector();

  /**
   * @brief Destructor of collector
   */
  ~ThreadLevelStatsCollector();

  /* See Metric for documentation on the following methods. They should correspond
   * to the "OnXxx" methods one-to-one */
  void CollectTransactionBegin(const concurrency::TransactionContext *txn) {
    for (auto &metric : metric_dispatch_[StatsEventType::TXN_BEGIN]) metric->OnTransactionBegin(txn);
  };

  void CollectTransactionCommit(const concurrency::TransactionContext *txn, oid_t tile_group_id) {
    for (auto &metric : metric_dispatch_[StatsEventType::TXN_COMMIT]) metric->OnTransactionCommit(txn, tile_group_id);
  };

  void CollectTransactionAbort(const concurrency::TransactionContext *txn, oid_t tile_group_id) {
    for (auto &metric : metric_dispatch_[StatsEventType::TXN_ABORT]) metric->OnTransactionAbort(txn, tile_group_id);
  };

  void CollectTupleRead(const concurrency::TransactionContext *current_txn, oid_t tile_group_id) {
    for (auto &metric : metric_dispatch_[StatsEventType::TUPLE_READ]) metric->OnTupleRead(current_txn, tile_group_id);
  };

  void CollectTupleUpdate(const concurrency::TransactionContext *current_txn, oid_t tile_group_id) {
    for (auto &metric : metric_dispatch_[StatsEventType::TUPLE_UPDATE])
      metric->OnTupleUpdate(current_txn, tile_group_id);
  };

  void CollectTupleInsert(const concurrency::TransactionContext *current_txn, oid_t tile_group_id) {
    for (auto &metric : metric_dispatch_[StatsEventType::TUPLE_INSERT])
      metric->OnTupleInsert(current_txn, tile_group_id);
  };

  void CollectTupleDelete(const concurrency::TransactionContext *current_txn, oid_t tile_group_id) {
    for (auto &metric : metric_dispatch_[StatsEventType::TUPLE_DELETE])
      metric->OnTupleDelete(current_txn, tile_group_id);
  };

  void CollectTableMemoryAlloc(oid_t database_id, oid_t table_id, size_t bytes) {
    if (table_id == INVALID_OID || database_id == INVALID_OID) return;

    for (auto &metric : metric_dispatch_[StatsEventType::TABLE_MEMORY_ALLOC])
      metric->OnMemoryAlloc({database_id, table_id}, bytes);
  };

  void CollectTableMemoryFree(oid_t database_id, oid_t table_id, size_t bytes) {
    if (table_id == INVALID_OID || database_id == INVALID_OID) return;
    for (auto &metric : metric_dispatch_[StatsEventType::TABLE_MEMORY_FREE])
      metric->OnMemoryFree({database_id, table_id}, bytes);
  };

  void CollectIndexRead(oid_t database_id, oid_t index_id, size_t num_read) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_READ])
      metric->OnIndexRead({database_id, index_id}, num_read);
  };

  void CollectIndexUpdate(oid_t database_id, oid_t index_id) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_UPDATE]) metric->OnIndexUpdate({database_id, index_id});
  };

  void CollectIndexInsert(oid_t database_id, oid_t index_id) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_INSERT]) metric->OnIndexInsert({database_id, index_id});
  };

  void CollectIndexDelete(oid_t database_id, oid_t index_id) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_DELETE]) metric->OnIndexDelete({database_id, index_id});
  };

  void CollectIndexMemoryAlloc(oid_t database_id, oid_t index_id, size_t bytes) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_MEMORY_ALLOC])
      metric->OnMemoryAlloc({database_id, index_id}, bytes);
  };

  void CollectIndexMemoryUsage(oid_t database_id, oid_t index_id, size_t bytes) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_MEMORY_USAGE])
      metric->OnMemoryUsage({database_id, index_id}, bytes);
  };

  void CollectIndexMemoryFree(oid_t database_id, oid_t index_id, size_t bytes) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_MEMORY_FREE])
      metric->OnMemoryFree({database_id, index_id}, bytes);
  };

  void CollectIndexMemoryReclaim(oid_t database_id, oid_t index_id, size_t bytes) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_MEMORY_RECLAIM])
      metric->OnMemoryReclaim({database_id, index_id}, bytes);
  };

  void CollectQueryBegin() {
    for (auto &metric : metric_dispatch_[StatsEventType::QUERY_BEGIN]) metric->OnQueryBegin();
  };

  void CollectQueryEnd() {
    for (auto &metric : metric_dispatch_[StatsEventType::QUERY_END]) metric->OnQueryEnd();
  };

  void CollectTestNum(int number) {
    for (auto &metric : metric_dispatch_[StatsEventType::TEST]) metric->OnTest(number);
  }

  /**
   * @return A vector of raw data, for each registered metric. Each piece of
   * data is guaranteed to be safe to read and remove, and the same type of
   * metric is guaranteed to be in the same positopn in the returned vector
   * for different instances of Collector.
   */
  std::vector<std::shared_ptr<AbstractRawData>> GetDataToAggregate();

 private:
  /**
   * Registers a Metric so that its callbacks are invoked.
   * Use this only in the constructor.
   * @tparam metric type of Metric to register
   * @param types A list of event types to receive updates about.
   */
  template <typename metric>
  void RegisterMetric(std::vector<StatsEventType> types) {
    auto m = std::make_shared<metric>();
    metrics_.push_back(m);
    for (StatsEventType type : types) metric_dispatch_[type].push_back(m);
  }

  using MetricList = std::vector<std::shared_ptr<Metric>>;
  /**
   * List of all registered metrics
   */
  MetricList metrics_;
  /**
   * Mapping from each type of event to a list of metrics registered to
   * receive updates from that type of event.
   */
  std::unordered_map<StatsEventType, MetricList, EnumHash<StatsEventType>> metric_dispatch_;

  static CollectorsMap collector_map_;
};

}  // namespace stats
}  // namespace terrier
