#pragma once

#include <memory>
#include <unordered_map>
#include <vector>
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
  /**
   * Concurrent unordered map between thread ID and pointer to an instance of this class
   */
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
   * @brief Constructor of collector
   */
  ThreadLevelStatsCollector();

  /**
   * @brief Destructor of collector
   */
  ~ThreadLevelStatsCollector();

  /**
   * Collector action on transaction begin
   * @param txn context of the transaction beginning
   */
  void CollectTransactionBegin(const transaction::TransactionContext *txn) {
    for (auto &metric : metric_dispatch_[StatsEventType::TXN_BEGIN]) metric->OnTransactionBegin(txn);
  }

  /**
   * Collector action on transaction commit
   * @param txn context of the transaction committing
   * @param database_oid OID fo the database where the txn happens.
   */
  void CollectTransactionCommit(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid) {
    for (auto &metric : metric_dispatch_[StatsEventType::TXN_COMMIT]) metric->OnTransactionCommit(txn, database_oid);
  }

  /**
   * Collector action on transaction abort
   * @param txn context of the transaction aborting
   * @param database_oid OID fo the database where the txn happens.
   */
  void CollectTransactionAbort(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid) {
    for (auto &metric : metric_dispatch_[StatsEventType::TXN_ABORT]) metric->OnTransactionAbort(txn, database_oid);
  }

  /**
   * Collector action on tuple read
   * @param txn context of the transaction performing read
   * @param database_oid OID of the database that the tuple read happens
   * @param table_oid OID of the table that the tuple read happens
   */
  void CollectTupleRead(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid,
                        catalog::table_oid_t table_oid) {
    for (auto &metric : metric_dispatch_[StatsEventType::TUPLE_READ])
      metric->OnTupleRead(txn, {database_oid, table_oid});
  }

  /**
   * Collector action on tuple update
   * @param txn context of the transaction performing update
   * @param database_oid OID of the database that the tuple update happens
   * @param table_oid OID of the table that the tuple update happens
   */
  void CollectTupleUpdate(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid,
                          catalog::table_oid_t table_oid) {
    for (auto &metric : metric_dispatch_[StatsEventType::TUPLE_UPDATE])
      metric->OnTupleUpdate(txn, {database_oid, table_oid});
  }

  /**
   * Collector action on tuple insert
   * @param txn context of the transaction performing insert
   * @param database_oid OID of the database that the tuple insert happens
   * @param table_oid OID of the table that the tuple insert happens
   */
  void CollectTupleInsert(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid,
                          catalog::table_oid_t table_oid) {
    for (auto &metric : metric_dispatch_[StatsEventType::TUPLE_INSERT])
      metric->OnTupleInsert(txn, {database_oid, table_oid});
  }

  /**
   * Collector action on tuple delete
   * @param txn Context of the transaction performing delete
   * @param database_oid OID of the database that the tuple delete happens
   * @param table_oid OID of the table that the tuple delete happens
   */
  void CollectTupleDelete(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid,
                          catalog::table_oid_t table_oid) {
    for (auto &metric : metric_dispatch_[StatsEventType::TUPLE_DELETE])
      metric->OnTupleDelete(txn, {database_oid, table_oid});
  }

  /**
   * Collector action on table memory allocation
   * @param database_oid OID of the database that the table memory allocation happens
   * @param table_oid OID of the table that the table memory allocation happens
   * @param size number of bytes being allocated
   */
  void CollectTableMemoryAlloc(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid, size_t size) {
    for (auto &metric : metric_dispatch_[StatsEventType::TABLE_MEMORY_ALLOC])
      metric->OnTableMemoryAlloc({database_oid, table_oid}, size);
  }

  /**
   * Collector action on table memory free
   * @param database_oid OID of the database that the table memory free happens
   * @param table_oid OID of the table that the table memory free happens
   * @param size number of bytes being freed
   */
  void CollectTableMemoryFree(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid, size_t size) {
    for (auto &metric : metric_dispatch_[StatsEventType::TABLE_MEMORY_FREE])
      metric->OnTableMemoryFree({database_oid, table_oid}, size);
  }

  /**
   * Collector action on table memory usage
   * @param database_oid OID of the database that the table memory usage happens
   * @param table_oid OID of the table that the table memory usage happens
   * @param size number of bytes being used
   */
  void CollectTableMemoryUsage(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid, size_t size) {
    for (auto &metric : metric_dispatch_[StatsEventType::TABLE_MEMORY_ALLOC])
      metric->OnTableMemoryUsage({database_oid, table_oid}, size);
  }

  /**
   * Collection action on table memory reclaim
   * @param database_oid OID of the database that the table memory reclaim happens
   * @param table_oid OID of the table that the table memory reclaim happens
   * @param size number of bytes being reclaim
   */
  void CollectTableMemoryReclaim(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid, size_t size) {
    for (auto &metric : metric_dispatch_[StatsEventType::TABLE_MEMORY_FREE])
      metric->OnTableMemoryReclaim({database_oid, table_oid}, size);
  }

  /**
   * Collection action on index read
   * @param database_oid OID of the database that the index read happens
   * @param index_oid OID of the index that the index read happens
   * @param num_read number of read happening
   */
  void CollectIndexRead(catalog::db_oid_t database_oid, catalog::index_oid_t index_oid, size_t num_read) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_READ])
      metric->OnIndexRead({database_oid, index_oid}, num_read);
  }

  /**
   * Collector action on index update
   * @param database_oid OID of the database that the index update happens
   * @param index_oid OID of the index that the index update happens
   */
  void CollectIndexUpdate(catalog::db_oid_t database_oid, catalog::index_oid_t index_oid) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_UPDATE])
      metric->OnIndexUpdate({database_oid, index_oid});
  }

  /**
   * Collector action on index insert
   * @param database_oid OID of the database that the index insert happens
   * @param index_oid OID of the index that the index insert happens
   */
  void CollectIndexInsert(catalog::db_oid_t database_oid, catalog::index_oid_t index_oid) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_INSERT])
      metric->OnIndexInsert({database_oid, index_oid});
  }

  /**
   * Collector action on index delete
   * @param database_oid OID of the database that the index delete happens
   * @param index_oid OID of the index that the index delete happens
   */
  void CollectIndexDelete(catalog::db_oid_t database_oid, catalog::index_oid_t index_oid) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_DELETE])
      metric->OnIndexDelete({database_oid, index_oid});
  }

  /**
   * Collector action on index memory allocation
   * @param database_oid OID of the database that the index memory alloc happens
   * @param index_oid OID of the index that the index memory alloc happens
   * @param size number of bytes being allocated
   */
  void CollectIndexMemoryAlloc(catalog::db_oid_t database_oid, catalog::index_oid_t index_oid, size_t size) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_MEMORY_ALLOC])
      metric->OnIndexMemoryAlloc({database_oid, index_oid}, size);
  }

  /**
   * Collector action on index memory usage
   * @param database_oid OID of the database that the index memory usage happens
   * @param index_oid OID of the index that the index memory usage happens
   * @param size number of bytes being used
   */
  void CollectIndexMemoryUsage(catalog::db_oid_t database_oid, catalog::index_oid_t index_oid, size_t size) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_MEMORY_USAGE])
      metric->OnIndexMemoryUsage({database_oid, index_oid}, size);
  }

  /**
   * Collector ation on index memory free
   * @param database_oid OID of the database that the index memory free happens
   * @param index_oid OID of the index that the index memory free happens
   * @param size number of bytes being freed
   */
  void CollectIndexMemoryFree(catalog::db_oid_t database_oid, catalog::index_oid_t index_oid, size_t size) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_MEMORY_FREE])
      metric->OnIndexMemoryFree({database_oid, index_oid}, size);
  }

  /**
   * Collector action on index memory reclaim
   * @param database_oid OID of the database that the index memory reclaim happens
   * @param index_oid OID of the index that the index memory reclaim happens
   * @param size number of bytes being reclaim
   */
  void CollectIndexMemoryReclaim(catalog::db_oid_t database_oid, catalog::index_oid_t index_oid, size_t size) {
    for (auto &metric : metric_dispatch_[StatsEventType::INDEX_MEMORY_RECLAIM])
      metric->OnIndexMemoryReclaim({database_oid, index_oid}, size);
  }

  /**
   * Collector action when a query begins
   */
  void CollectQueryBegin() {
    for (auto &metric : metric_dispatch_[StatsEventType::QUERY_BEGIN]) metric->OnQueryBegin();
  }

  /**
   * Collector action when a query ends
   */
  void CollectQueryEnd() {
    for (auto &metric : metric_dispatch_[StatsEventType::QUERY_END]) metric->OnQueryEnd();
  }

  /**
   * Collector action for a test event
   */
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
};

}  // namespace stats
}  // namespace terrier
