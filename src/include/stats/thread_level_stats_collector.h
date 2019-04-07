#pragma once

#include <thread>
#include <unordered_map>
#include "catalog/catalog_defs.h"
#include "common/container/concurrent_map.h"
#include "stats/abstract_metric.h"
#include "stats/abstract_raw_data.h"
#include "stats/statistic_defs.h"
#include "transaction/transaction_manager.h"

namespace terrier {

namespace transaction {
class TransactionContext;
}  // namespace transaction

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
  using CollectorsMap = common::ConcurrentMap<std::thread::id, ThreadLevelStatsCollector *, std::hash<std::thread::id>>;
  /**
   * @return the Collector for the calling thread
   */
  static ThreadLevelStatsCollector *GetCollectorForThread() {
    std::thread::id tid = std::this_thread::get_id();
    auto const iter = collector_map_.Find(tid);
    if (iter == collector_map_.End()) {
      return nullptr;
    }
    return iter->second;
  }

  /**
   * @return A mapping from each thread to their assigned Collector
   */
  static CollectorsMap &GetAllCollectors() { return collector_map_; }

  /**
   * @return the txn_manager of the system
   */
  static transaction::TransactionManager *GetTxnManager() { return txn_manager_; }

  /**
   * @return the txn_manager of the system
   */
  static void SetTxnManager(transaction::TransactionManager *txn_manager) { txn_manager_ = txn_manager; }

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
  void CollectTransactionBegin(const transaction::TransactionContext *txn) {
    for (auto &metric : metric_dispatch_[StatsEventType::TXN_BEGIN]) metric->OnTransactionBegin(txn);
  }

  void CollectTransactionCommit(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid) {
    for (auto &metric : metric_dispatch_[StatsEventType::TXN_COMMIT]) metric->OnTransactionCommit(txn, database_oid);
  }

  void CollectTransactionAbort(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid) {
    for (auto &metric : metric_dispatch_[StatsEventType::TXN_ABORT]) metric->OnTransactionAbort(txn, database_oid);
  }

  void CollectTupleRead(const transaction::TransactionContext *current_txn, catalog::db_oid_t database_oid,
                        catalog::table_oid_t table_oid) {
    for (auto &metric : metric_dispatch_[StatsEventType::TUPLE_READ])
      metric->OnTupleRead(current_txn, {database_oid, table_oid});
  }

  void CollectTupleUpdate(const transaction::TransactionContext *current_txn, catalog::db_oid_t database_oid,
                          catalog::table_oid_t table_oid) {
    for (auto &metric : metric_dispatch_[StatsEventType::TUPLE_UPDATE])
      metric->OnTupleUpdate(current_txn, {database_oid, table_oid});
  }

  void CollectTupleInsert(const transaction::TransactionContext *current_txn, catalog::db_oid_t database_oid,
                          catalog::table_oid_t table_oid) {
    for (auto &metric : metric_dispatch_[StatsEventType::TUPLE_INSERT])
      metric->OnTupleInsert(current_txn, {database_oid, table_oid});
  }

  void CollectTupleDelete(const transaction::TransactionContext *current_txn, catalog::db_oid_t database_oid,
                          catalog::table_oid_t table_oid) {
    for (auto &metric : metric_dispatch_[StatsEventType::TUPLE_DELETE])
      metric->OnTupleDelete(current_txn, {database_oid, table_oid});
  }

  void CollectTableMemoryAlloc(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid, size_t bytes) {
    for (auto &metric : metric_dispatch_[StatsEventType::TABLE_MEMORY_ALLOC])
      metric->OnTableMemoryAlloc({database_oid, table_oid}, bytes);
  }

  void CollectTableMemoryFree(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid, size_t bytes) {
    for (auto &metric : metric_dispatch_[StatsEventType::TABLE_MEMORY_FREE])
      metric->OnTableMemoryFree({database_oid, table_oid}, bytes);
  }

  void CollectTableMemoryUsage(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid, size_t bytes) {
    for (auto &metric : metric_dispatch_[StatsEventType::TABLE_MEMORY_ALLOC])
      metric->OnTableMemoryUsage({database_oid, table_oid}, bytes);
  }

  void CollectTableMemoryReclaim(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid, size_t bytes) {
    for (auto &metric : metric_dispatch_[StatsEventType::TABLE_MEMORY_FREE])
      metric->OnTableMemoryReclaim({database_oid, table_oid}, bytes);
  }

  void CollectIndexRead(catalog::db_oid_t database_oid, catalog::index_oid_t index_oid, size_t num_read) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_READ])
      metric->OnIndexRead({database_oid, index_oid}, num_read);
  }

  void CollectIndexUpdate(catalog::db_oid_t database_oid, catalog::index_oid_t index_oid) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_UPDATE])
      metric->OnIndexUpdate({database_oid, index_oid});
  }

  void CollectIndexInsert(catalog::db_oid_t database_oid, catalog::index_oid_t index_oid) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_INSERT])
      metric->OnIndexInsert({database_oid, index_oid});
  }

  void CollectIndexDelete(catalog::db_oid_t database_oid, catalog::index_oid_t index_oid) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_DELETE])
      metric->OnIndexDelete({database_oid, index_oid});
  }

  void CollectIndexMemoryAlloc(catalog::db_oid_t database_oid, catalog::index_oid_t index_oid, size_t bytes) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_MEMORY_ALLOC])
      metric->OnIndexMemoryAlloc({database_oid, index_oid}, bytes);
  }

  void CollectIndexMemoryUsage(catalog::db_oid_t database_oid, catalog::index_oid_t index_oid, size_t bytes) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_MEMORY_USAGE])
      metric->OnIndexMemoryUsage({database_oid, index_oid}, bytes);
  }

  void CollectIndexMemoryFree(catalog::db_oid_t database_oid, catalog::index_oid_t index_oid, size_t bytes) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_MEMORY_FREE])
      metric->OnIndexMemoryFree({database_oid, index_oid}, bytes);
  }

  void CollectIndexMemoryReclaim(catalog::db_oid_t database_oid, catalog::index_oid_t index_oid, size_t bytes) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_MEMORY_RECLAIM])
      metric->OnIndexMemoryReclaim({database_oid, index_oid}, bytes);
  }

  void CollectQueryBegin() {
    for (auto &metric : metric_dispatch_[StatsEventType::QUERY_BEGIN]) metric->OnQueryBegin();
  }

  void CollectQueryEnd() {
    for (auto &metric : metric_dispatch_[StatsEventType::QUERY_END]) metric->OnQueryEnd();
  }

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
  /**
   * Mapping from thread ID to stats collector
   */
  static CollectorsMap collector_map_;
  /**
   * Transaction manager of the system
   */
  static transaction::TransactionManager *txn_manager_;
};

}  // namespace stats
}  // namespace terrier
