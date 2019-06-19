#pragma once

#include <bitset>
#include <memory>
#include <unordered_map>
#include <vector>
#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "metrics/abstract_metric.h"
#include "metrics/abstract_raw_data.h"
#include "metrics/metric_defs.h"
#include "transaction/transaction_manager.h"

namespace terrier::metrics {

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
  void RecordTransactionBegin(const transaction::TransactionContext &txn) {
    for (uint8_t component = 0; component < NUM_COMPONENTS; component++) {
      if (MetricSupportsEvent(MetricsEventType::TXN_BEGIN, static_cast<MetricsComponent>(component))) {
        metrics_[component]->OnTransactionBegin(txn);
      }
    }
  }

  /**
   * Collector action on transaction commit
   * @param txn context of the transaction committing
   * @param database_oid OID of the database where the txn happens.
   */
  void RecordTransactionCommit(const transaction::TransactionContext &txn, catalog::db_oid_t database_oid) {
    for (uint8_t component = 0; component < NUM_COMPONENTS; component++) {
      if (MetricSupportsEvent(MetricsEventType::TXN_COMMIT, static_cast<MetricsComponent>(component))) {
        metrics_[component]->OnTransactionCommit(txn, database_oid);
      }
    }
  }

  /**
   * Collector action on transaction abort
   * @param txn context of the transaction aborting
   * @param database_oid OID of the database where the txn happens.
   */
  void RecordTransactionAbort(const transaction::TransactionContext &txn, catalog::db_oid_t database_oid) {
    for (uint8_t component = 0; component < NUM_COMPONENTS; component++) {
      if (MetricSupportsEvent(MetricsEventType::TXN_ABORT, static_cast<MetricsComponent>(component))) {
        metrics_[component]->OnTransactionAbort(txn, database_oid);
      }
    }
  }

  /**
   * Collector action on tuple read
   * @param txn context of the transaction performing read
   * @param database_oid OID of the database that the tuple read happens
   * @param namespace_oid OID of the namespace that the tuple read happens
   * @param table_oid OID of the table that the tuple read happens
   */
  void RecordTupleRead(const transaction::TransactionContext &txn, catalog::db_oid_t database_oid,
                       catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid) {
    for (uint8_t component = 0; component < NUM_COMPONENTS; component++) {
      if (MetricSupportsEvent(MetricsEventType::TUPLE_READ, static_cast<MetricsComponent>(component))) {
        metrics_[component]->OnTupleRead(txn, database_oid, namespace_oid, table_oid);
      }
    }
  }

  /**
   * Collector action on tuple update
   * @param txn context of the transaction performing update
   * @param database_oid OID of the database that the tuple update happens
   * @param namespace_oid OID of the namespace that the tuple update happens
   * @param table_oid OID of the table that the tuple update happens
   */
  void RecordTupleUpdate(const transaction::TransactionContext &txn, catalog::db_oid_t database_oid,
                         catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid) {
    for (uint8_t component = 0; component < NUM_COMPONENTS; component++) {
      if (MetricSupportsEvent(MetricsEventType::TUPLE_UPDATE, static_cast<MetricsComponent>(component))) {
        metrics_[component]->OnTupleUpdate(txn, database_oid, namespace_oid, table_oid);
      }
    }
  }

  /**
   * Collector action on tuple insert
   * @param txn context of the transaction performing insert
   * @param database_oid OID of the database that the tuple insert happens
   * @param namespace_oid OID of the namespace that the tuple insert happens
   * @param table_oid OID of the table that the tuple insert happens
   */
  void RecordTupleInsert(const transaction::TransactionContext &txn, catalog::db_oid_t database_oid,
                         catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid) {
    for (uint8_t component = 0; component < NUM_COMPONENTS; component++) {
      if (MetricSupportsEvent(MetricsEventType::TUPLE_INSERT, static_cast<MetricsComponent>(component))) {
        metrics_[component]->OnTupleInsert(txn, database_oid, namespace_oid, table_oid);
      }
    }
  }

  /**
   * Collector action on tuple delete
   * @param txn Context of the transaction performing delete
   * @param database_oid OID of the database that the tuple delete happens
   * @param namespace_oid OID of the namespace that the tuple delete happens
   * @param table_oid OID of the table that the tuple delete happens
   */
  void RecordTupleDelete(const transaction::TransactionContext &txn, catalog::db_oid_t database_oid,
                         catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid) {
    for (uint8_t component = 0; component < NUM_COMPONENTS; component++) {
      if (MetricSupportsEvent(MetricsEventType::TUPLE_DELETE, static_cast<MetricsComponent>(component))) {
        metrics_[component]->OnTupleDelete(txn, database_oid, namespace_oid, table_oid);
      }
    }
  }

 private:
  friend class MetricsManager;

  explicit MetricsStore(const std::bitset<NUM_COMPONENTS> &enabled_metrics);

  std::array<std::unique_ptr<AbstractRawData>, NUM_COMPONENTS> GetDataToAggregate();

  std::array<std::unique_ptr<Metric>, NUM_COMPONENTS> metrics_;

  const std::bitset<NUM_COMPONENTS> &enabled_metrics_;
};

}  // namespace terrier::metrics
