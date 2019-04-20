#pragma once

#include <chrono>  //NOLINT
#include <string>
#include <unordered_map>
#include <utility>

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
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
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
   * this RawData into the Catalog. Expect this object
   * to be garbage-collected after this method is called.
   * @param txn_manager transaction manager of the system
   */
  void UpdateAndPersist(transaction::TransactionManager *txn_manager) override;

  /**
   * @return the type of the metric this object is holding the data for
   */
  MetricType GetMetricType() const override { return MetricType::TRANSACTION; }

 private:
  /**
   * Collection of data related to a transaction
   */
  struct TransactionData {
    std::chrono::high_resolution_clock::time_point start_;
    uint64_t latency_;
    uint64_t tuple_read_;
    uint64_t tuple_insert_;
    uint64_t tuple_delete_;
    uint64_t tuple_update_;
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
   * @param src database and table id pair that the tuple read happens
   */
  void OnTupleRead(const transaction::TransactionContext *txn,
                   std::pair<catalog::db_oid_t, catalog::table_oid_t> src) override {
    (void)src;
    GetRawData()->IncrementTupleRead(txn);
  }

  /**
   * @param txn context of the transaction performing update
   * @param src database and table id pair that the tuple update happens
   */
  void OnTupleUpdate(const transaction::TransactionContext *txn,
                     std::pair<catalog::db_oid_t, catalog::table_oid_t> src) override {
    (void)src;
    GetRawData()->IncrementTupleUpdate(txn);
  }

  /**
   * @param txn context of the transaction performing insert
   * @param src database and table id pair that the tuple insert happens
   */
  void OnTupleInsert(const transaction::TransactionContext *txn,
                     std::pair<catalog::db_oid_t, catalog::table_oid_t> src) override {
    (void)src;
    GetRawData()->IncrementTupleInsert(txn);
  }

  /**
   * @param txn Context of the transaction performing delete
   * @param src database and table id pair that the tuple delete happens
   */
  void OnTupleDelete(const transaction::TransactionContext *txn,
                     std::pair<catalog::db_oid_t, catalog::table_oid_t> src) override {
    (void)src;
    GetRawData()->IncrementTupleDelete(txn);
  }
};
}  // namespace storage::metric
}  // namespace terrier
