#pragma once

#include <memory>
#include <unordered_map>
#include <vector>
#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "metric/abstract_metric.h"
#include "metric/abstract_raw_data.h"
#include "metric/metric_defs.h"
#include "transaction/transaction_manager.h"

namespace terrier::metric {

class MetricsManager;

/**
 * @brief Class responsible for collecting raw data on a single thread.
 *
 * Each thread should be assigned one collector, at the time of system startup, that is globally
 * unique. This is to ensure that we can collect raw data in an non-blocking way as the
 * collection code runs on critical query path. Periodically a dedicated aggregator thread
 * will put the data from all collectors together into a meaningful form.
 */
class MetricsStore {
 public:
  /**
   * Destructor of collector
   */
  ~MetricsStore() = default;

  /**
   * Collector action on transaction begin
   * @param txn context of the transaction beginning
   */
  void RecordTransactionBegin(const transaction::TransactionContext *txn) {
    for (auto &metric : metric_dispatch_[MetricsEventType::TXN_BEGIN]) metric->OnTransactionBegin(txn);
  }

  /**
   * Collector action on transaction commit
   * @param txn context of the transaction committing
   * @param database_oid OID of the database where the txn happens.
   */
  void RecordTransactionCommit(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid) {
    for (auto &metric : metric_dispatch_[MetricsEventType::TXN_COMMIT]) metric->OnTransactionCommit(txn, database_oid);
  }

  /**
   * Collector action on transaction abort
   * @param txn context of the transaction aborting
   * @param database_oid OID of the database where the txn happens.
   */
  void RecordTransactionAbort(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid) {
    for (auto &metric : metric_dispatch_[MetricsEventType::TXN_ABORT]) metric->OnTransactionAbort(txn, database_oid);
  }

  /**
   * Collector action on tuple read
   * @param txn context of the transaction performing read
   * @param database_oid OID of the database that the tuple read happens
   * @param namespace_oid OID of the namespace that the tuple read happens
   * @param table_oid OID of the table that the tuple read happens
   */
  void RecordTupleRead(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid,
                       catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid) {
    for (auto &metric : metric_dispatch_[MetricsEventType::TUPLE_READ])
      metric->OnTupleRead(txn, database_oid, namespace_oid, table_oid);
  }

  /**
   * Collector action on tuple update
   * @param txn context of the transaction performing update
   * @param database_oid OID of the database that the tuple update happens
   * @param namespace_oid OID of the namespace that the tuple update happens
   * @param table_oid OID of the table that the tuple update happens
   */
  void RecordTupleUpdate(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid,
                         catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid) {
    for (auto &metric : metric_dispatch_[MetricsEventType::TUPLE_UPDATE])
      metric->OnTupleUpdate(txn, database_oid, namespace_oid, table_oid);
  }

  /**
   * Collector action on tuple insert
   * @param txn context of the transaction performing insert
   * @param database_oid OID of the database that the tuple insert happens
   * @param namespace_oid OID of the namespace that the tuple insert happens
   * @param table_oid OID of the table that the tuple insert happens
   */
  void RecordTupleInsert(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid,
                         catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid) {
    for (auto &metric : metric_dispatch_[MetricsEventType::TUPLE_INSERT])
      metric->OnTupleInsert(txn, database_oid, namespace_oid, table_oid);
  }

  /**
   * Collector action on tuple delete
   * @param txn Context of the transaction performing delete
   * @param database_oid OID of the database that the tuple delete happens
   * @param namespace_oid OID of the namespace that the tuple delete happens
   * @param table_oid OID of the table that the tuple delete happens
   */
  void RecordTupleDelete(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid,
                         catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid) {
    for (auto &metric : metric_dispatch_[MetricsEventType::TUPLE_DELETE])
      metric->OnTupleDelete(txn, database_oid, namespace_oid, table_oid);
  }

 private:
  friend class MetricsManager;

  /**
   * Constructor of collector
   */
  MetricsStore();

  /**
   * @return A vector of raw data, for each registered metric. Each piece of
   * data is guaranteed to be safe to read and remove, and the same type of
   * metric is guaranteed to be in the same position in the returned vector
   * for different instances of Collector.
   */
  std::vector<std::unique_ptr<AbstractRawData>> GetDataToAggregate();

  /**
   * Registers a Metric so that its callbacks are invoked.
   * Use this only in the constructor.
   * @tparam metric type of Metric to register
   * @param types A list of event types to receive updates about.
   */
  template <typename metric>
  void RegisterMetric(const std::vector<MetricsEventType> &types) {
    const auto &m = metrics_.emplace_back(new metric);
    for (MetricsEventType type : types) metric_dispatch_[type].emplace_back(m);
  }

  /**
   * Vector of all registered metrics, this owns the metric objects and frees them at object destruction
   */
  std::vector<std::unique_ptr<Metric>> metrics_;

  /**
   * Mapping from each type of event to a list of metrics registered to
   * receive updates from that type of event. This does NOT own the registered metrics
   */
  std::unordered_map<MetricsEventType, std::vector<common::ManagedPointer<Metric>>> metric_dispatch_;
};

}  // namespace terrier::metric
