#pragma once

#include <chrono>  //NOLINT
#include <string>
#include <unordered_map>
#include <utility>

#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "common/scoped_timer.h"
#include "storage/metric/abstract_metric.h"
#include "transaction/transaction_defs.h"

namespace terrier {

namespace transaction {
class TransactionContext;
}  // namespace transaction

namespace storage::metric {

/**
 * Raw data object for holding stats collected at transaction level
 */
class TransactionMetricRawData : public AbstractRawData {
 public:
  /**
   * Set start time of transaction
   * @param txn transaction context of the relevant transaction
   */
  void SetTxnStart(const transaction::TransactionContext *txn) {
    data_[txn->TxnId().load()].start_ = std::chrono::high_resolution_clock::now();
  }

  /**
   * Calculate transaction latency
   * @param txn transaction context of the relevant transaction
   */
  void CalculateTxnLatency(const transaction::TransactionContext *txn) {
    auto end = std::chrono::high_resolution_clock::now();
    auto start = data_[txn->TxnId().load()].start_;
    data_[txn->TxnId().load()].latency_ =
        static_cast<int64_t>(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
  }

  /**
   * Increment the number of tuples read by one
   * @param txn transaction context of the relevant transaction
   */
  void IncrementTupleRead(const transaction::TransactionContext *txn) { data_[txn->TxnId().load()].tuple_read_++; }

  /**
   * Increment the number of tuples updated by one
   * @param txn transaction context of the relevant transaction
   */
  void IncrementTupleUpdate(const transaction::TransactionContext *txn) { data_[txn->TxnId().load()].tuple_update_++; }

  /**
   * Increment the number of tuples inserted by one
   * @param txn transaction context of the relevant transaction
   */
  void IncrementTupleInsert(const transaction::TransactionContext *txn) { data_[txn->TxnId().load()].tuple_insert_++; }

  /**
   * Increment the number of tuples deleted by one
   * @param txn transaction context of the relevant transaction
   */
  void IncrementTupleDelete(const transaction::TransactionContext *txn) { data_[txn->TxnId().load()].tuple_delete_++; }

  /**
   * Aggregate collected data from another raw data object into this raw data object
   * @param other
   */
  void Aggregate(AbstractRawData *other) override {
    auto other_db_metric = dynamic_cast<TransactionMetricRawData *>(other);
    for (auto &entry : other_db_metric->data_) {
      auto &other_counter = entry.second;
      data_[entry.first] = other_counter;
    }
  }

  /**
   * Make necessary updates to the metric raw data and persist the content of
   * this RawData into SqlTables. Expect this object
   * to be garbage-collected after this method is called.
   * @param txn_manager transaction manager of the system
   * @param catalog catalog of the system
   * @param txn transaction context used for accessing SqlTables
   */
  void UpdateAndPersist(transaction::TransactionManager *txn_manager, catalog::Catalog *catalog,
                        transaction::TransactionContext *txn) override;

  /**
   * @return the type of the metric this object is holding the data for
   */
  MetricType GetMetricType() const override { return MetricType::TRANSACTION; }

  /**
   * @return the latency of the given transaction
   */
  int64_t GetLatency(transaction::timestamp_t txn_id) { return data_[txn_id].latency_; }

  /**
   * @return the tuples read of the given transaction
   */
  int64_t GetTupleRead(transaction::timestamp_t txn_id) { return data_[txn_id].tuple_read_; }

  /**
   * @return the tuples updated of the given transaction
   */
  int64_t GetTupleUpdate(transaction::timestamp_t txn_id) { return data_[txn_id].tuple_update_; }

  /**
   * @return the tuples inserted of the given transaction
   */
  int64_t GetTupleInsert(transaction::timestamp_t txn_id) { return data_[txn_id].tuple_insert_; }

  /**
   * @return the tuples deleted of the given transaction
   */
  int64_t GetTupleDelete(transaction::timestamp_t txn_id) { return data_[txn_id].tuple_delete_; }

  /**
   * Get the SQL table for persisting collected data, create a new table if necessary
   * @param txn_manager transaction manager of the system
   * @param catalog catalog of the system
   * @param txn transaction context used for table lookup/creation
   */
  catalog::SqlTableHelper *GetStatsTable(transaction::TransactionManager *txn_manager, catalog::Catalog *catalog,
                                         transaction::TransactionContext *txn) override;

 private:
  /**
   * Collection of data related to a transaction
   */
  struct TransactionData {
    std::chrono::high_resolution_clock::time_point start_;
    int64_t latency_;
    int64_t tuple_read_;
    int64_t tuple_insert_;
    int64_t tuple_delete_;
    int64_t tuple_update_;
  };

  /**
   * Maps from transaction id to relevant data.
   */
  std::unordered_map<transaction::timestamp_t, struct TransactionData> data_;
};

/**
 * Interface that owns and manipulates TransactionMetricRawData
 */
class TransactionMetric : public AbstractMetric<TransactionMetricRawData> {
 public:
  /**
   * @param txn transaction context of the beginning transaction
   */
  void OnTransactionBegin(const transaction::TransactionContext *txn) override { GetRawData()->SetTxnStart(txn); }

  /**
   * @param txn transaction context of the committing transaction
   * @param database_oid OID of the database the transaction is running in
   */
  void OnTransactionCommit(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid) override {
    (void)database_oid;
    GetRawData()->CalculateTxnLatency(txn);
  }

  /**
   * @param txn transaction context of the aborting transaction
   * @param database_oid OID of the database the transaction is running in
   */
  void OnTransactionAbort(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid) override {
    (void)database_oid;
    GetRawData()->CalculateTxnLatency(txn);
  }

  /**
   * @param txn context of the transaction performing read
   * @param database_oid OID of the database that the tuple read happens
   * @param namespace_oid OID of the namespace that the tuple read happens
   * @param table_oid OID of the table that the tuple read happens
   */
  void OnTupleRead(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid,
                   catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid) override {
    (void)database_oid;
    (void)namespace_oid;
    (void)table_oid;
    GetRawData()->IncrementTupleRead(txn);
  }

  /**
   * @param txn context of the transaction performing update
   * @param database_oid OID of the database that the tuple delete happens
   * @param namespace_oid OID of the namespace that the tuple delete happens
   * @param table_oid OID of the table that the tuple delete happens
   */
  void OnTupleUpdate(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid,
                     catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid) override {
    (void)database_oid;
    (void)namespace_oid;
    (void)table_oid;
    GetRawData()->IncrementTupleUpdate(txn);
  }

  /**
   * @param txn context of the transaction performing insert
   * @param database_oid OID of the database that the tuple delete happens
   * @param namespace_oid OID of the namespace that the tuple delete happens
   * @param table_oid OID of the table that the tuple delete happens
   */
  void OnTupleInsert(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid,
                     catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid) override {
    (void)database_oid;
    (void)namespace_oid;
    (void)table_oid;
    GetRawData()->IncrementTupleInsert(txn);
  }

  /**
   * @param txn Context of the transaction performing delete
   * @param database_oid OID of the database that the tuple delete happens
   * @param namespace_oid OID of the namespace that the tuple delete happens
   * @param table_oid OID of the table that the tuple delete happens
   */
  void OnTupleDelete(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid,
                     catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid) override {
    (void)database_oid;
    (void)namespace_oid;
    (void)table_oid;
    GetRawData()->IncrementTupleDelete(txn);
  }
};
}  // namespace storage::metric
}  // namespace terrier
