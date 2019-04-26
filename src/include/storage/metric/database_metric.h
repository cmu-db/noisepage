#pragma once

#include <string>
#include <unordered_map>
#include <utility>

#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "storage/metric/abstract_metric.h"
#include "type/transient_value_factory.h"
#include "type/transient_value_peeker.h"

namespace terrier {

namespace transaction {
class TransactionContext;
}  // namespace transaction

namespace storage::metric {

/**
 * Raw data object for holding stats collected at the database level
 */
class DatabaseMetricRawData : public AbstractRawData {
 public:
  /**
   * Increment the number of committed transaction by one
   * @param database_id OID of the database the transaction committed in
   */
  void IncrementTxnCommitted(catalog::db_oid_t database_id) { data_[database_id].commit_cnt_++; }

  /**
   * Increment the number of aborted transaction by one
   * @param database_id OID of the database the transaction aborted in
   */
  void IncrementTxnAborted(catalog::db_oid_t database_id) { data_[database_id].abort_cnt_++; }

  /**
   * Aggregate collected data from another raw data object into this raw data object
   * @param other
   */
  void Aggregate(AbstractRawData *other) override {
    auto other_db_metric = dynamic_cast<DatabaseMetricRawData *>(other);
    for (auto &entry : other_db_metric->data_) {
      auto &this_counter = data_[entry.first];
      auto &other_counter = entry.second;
      this_counter.commit_cnt_ += other_counter.commit_cnt_;
      this_counter.abort_cnt_ += other_counter.abort_cnt_;
    }
  }

  /**
   * Make necessary updates to the metric raw data and persist the content of
   * this RawData into the Catalog. Expect this object
   * to be garbage-collected after this method is called.
   * @param txn_manager transaction manager of the system
   * @param catalog catalog of the system
   */
void UpdateAndPersist(transaction::TransactionManager *txn_manager, catalog::Catalog *catalog, transaction::TransactionContext *txn) override;

  /**
   * @return the type of the metric this object is holding the data for
   */
  MetricType GetMetricType() const override { return MetricType::DATABASE; }

  /**
   * @return the number of committed transaction in a database
   */
  int32_t GetCommitCount(catalog::db_oid_t db_oid) { return data_[db_oid].commit_cnt_; }

  /**
   * @return the number of aborted transaction in a database
   */
  int32_t GetAbortCount(catalog::db_oid_t db_oid) { return data_[db_oid].abort_cnt_; }

  /**
   * Get the SQL table for persisting collected data, create a new table if necessary
   */
  catalog::SqlTableHelper *GetStatsTable(transaction::TransactionManager *txn_manager, catalog::Catalog *catalog, transaction::TransactionContext *txn);

 private:
  /**
   * Maps from database id to a pair of counters.
   *
   * First counter represents number of transactions committed and the second
   * one represents the number of transactions aborted.
   */
  struct Counter {
    int32_t commit_cnt_;
    int32_t abort_cnt_;
  };
  std::unordered_map<catalog::db_oid_t, struct Counter> data_;
};

/**
 * Interface that owns and manipulates DatabaseMetricRawData
 */
class DatabaseMetric : public AbstractMetric<DatabaseMetricRawData> {
 public:
  /**
   * Database level action on transaction commit
   * @param txn transaction context of the committing transaction
   * @param database_oid OID of the database the transaction is running in
   */
  void OnTransactionCommit(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid) override {
    GetRawData()->IncrementTxnCommitted(database_oid);
  }

  /**
   * Database level action on transaction abort
   * @param txn transaction context of the aborting transaction
   * @param database_oid OID of the database the transaction is running in
   */
  void OnTransactionAbort(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid) override {
    GetRawData()->IncrementTxnAborted(database_oid);
  }
};
}  // namespace storage::metric
}  // namespace terrier
