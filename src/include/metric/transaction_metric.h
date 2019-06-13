#pragma once

#include <chrono>  //NOLINT
#include <string>
#include <unordered_map>
#include <utility>

#include "catalog/catalog_defs.h"
#include "common/scoped_timer.h"
#include "metric/abstract_metric.h"
#include "transaction/transaction_defs.h"

namespace terrier::metric {

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
    TERRIER_ASSERT(txn != nullptr, "Transaction context cannot be nullptr");
    data_[txn->TxnId().load()].start_ = std::chrono::high_resolution_clock::now();
  }

  /**
   * Calculate transaction latency
   * @param txn transaction context of the relevant transaction
   */
  void CalculateTxnLatency(const transaction::TransactionContext *txn) {
    TERRIER_ASSERT(txn != nullptr, "Transaction context cannot be nullptr");
    auto end = std::chrono::high_resolution_clock::now();
    auto start = data_[txn->TxnId().load()].start_;
    data_[txn->TxnId().load()].latency_ =
        static_cast<int64_t>(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
  }

  /**
   * Increment the number of tuples read by one
   * @param txn transaction context of the relevant transaction
   */
  void IncrementTupleRead(const transaction::TransactionContext *txn) {
    TERRIER_ASSERT(txn != nullptr, "Transaction context cannot be nullptr");
    data_[txn->TxnId().load()].tuple_read_++;
  }

  /**
   * Increment the number of tuples updated by one
   * @param txn transaction context of the relevant transaction
   */
  void IncrementTupleUpdate(const transaction::TransactionContext *txn) {
    TERRIER_ASSERT(txn != nullptr, "Transaction context cannot be nullptr");
    data_[txn->TxnId().load()].tuple_update_++;
  }

  /**
   * Increment the number of tuples inserted by one
   * @param txn transaction context of the relevant transaction
   */
  void IncrementTupleInsert(const transaction::TransactionContext *txn) {
    TERRIER_ASSERT(txn != nullptr, "Transaction context cannot be nullptr");
    data_[txn->TxnId().load()].tuple_insert_++;
  }

  /**
   * Increment the number of tuples deleted by one
   * @param txn transaction context of the relevant transaction
   */
  void IncrementTupleDelete(const transaction::TransactionContext *txn) {
    TERRIER_ASSERT(txn != nullptr, "Transaction context cannot be nullptr");
    data_[txn->TxnId().load()].tuple_delete_++;
  }

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
   * @return the type of the metric this object is holding the data for
   */
  MetricsScope GetMetricType() const override { return MetricsScope::TRANSACTION; }

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

 private:
  /**
   * Collection of data related to a transaction
   */
  struct TransactionData {
    /**
     * Starting time of the transaction
     */
    std::chrono::high_resolution_clock::time_point start_;
    /**
     * Latency of the transaction
     */
    int64_t latency_;
    /**
     * Number of tuples read by the transaction
     */
    int64_t tuple_read_;
    /**
     * Number of tuples inserted by the transaction
     */
    int64_t tuple_insert_;
    /**
     * Number of tuples deleted by the transaction
     */
    int64_t tuple_delete_;
    /**
     * Number of tuples updated by the transaction
     */
    int64_t tuple_update_;
  };

  /**
   * Map of transaction id to relevant data.
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
  void OnTransactionCommit(const transaction::TransactionContext *txn,
                           UNUSED_ATTRIBUTE catalog::db_oid_t database_oid) override {
    GetRawData()->CalculateTxnLatency(txn);
  }

  /**
   * @param txn transaction context of the aborting transaction
   * @param database_oid OID of the database the transaction is running in
   */
  void OnTransactionAbort(const transaction::TransactionContext *txn,
                          UNUSED_ATTRIBUTE catalog::db_oid_t database_oid) override {
    GetRawData()->CalculateTxnLatency(txn);
  }

  /**
   *
   * @param txn context of the transaction performing read
   * @param database_oid OID of the database that the tuple read happens
   * @param namespace_oid OID of the namespace that the tuple read happens
   * @param table_oid OID of the table that the tuple read happens
   */
  void OnTupleRead(const transaction::TransactionContext *txn, UNUSED_ATTRIBUTE catalog::db_oid_t database_oid,
                   UNUSED_ATTRIBUTE catalog::namespace_oid_t namespace_oid,
                   UNUSED_ATTRIBUTE catalog::table_oid_t table_oid) override {
    GetRawData()->IncrementTupleRead(txn);
  }

  /**
   * @param txn context of the transaction performing update
   * @param database_oid OID of the database that the tuple update happens
   * @param namespace_oid OID of the namespace that the tuple update happens
   * @param table_oid OID of the table that the tuple update happens
   */
  void OnTupleUpdate(const transaction::TransactionContext *txn, UNUSED_ATTRIBUTE catalog::db_oid_t database_oid,
                     UNUSED_ATTRIBUTE catalog::namespace_oid_t namespace_oid,
                     UNUSED_ATTRIBUTE catalog::table_oid_t table_oid) override {
    GetRawData()->IncrementTupleUpdate(txn);
  }

  /**
   * @param txn context of the transaction performing insert
   * @param database_oid OID of the database that the tuple insert happens
   * @param namespace_oid OID of the namespace that the tuple insert happens
   * @param table_oid OID of the table that the tuple insert happens
   */
  void OnTupleInsert(const transaction::TransactionContext *txn, UNUSED_ATTRIBUTE catalog::db_oid_t database_oid,
                     UNUSED_ATTRIBUTE catalog::namespace_oid_t namespace_oid,
                     UNUSED_ATTRIBUTE catalog::table_oid_t table_oid) override {
    GetRawData()->IncrementTupleInsert(txn);
  }

  /**
   * @param txn Context of the transaction performing delete
   * @param database_oid OID of the database that the tuple delete happens
   * @param namespace_oid OID of the namespace that the tuple delete happens
   * @param table_oid OID of the table that the tuple delete happens
   */
  void OnTupleDelete(const transaction::TransactionContext *txn, UNUSED_ATTRIBUTE catalog::db_oid_t database_oid,
                     UNUSED_ATTRIBUTE catalog::namespace_oid_t namespace_oid,
                     UNUSED_ATTRIBUTE catalog::table_oid_t table_oid) override {
    GetRawData()->IncrementTupleDelete(txn);
  }
};
}  // namespace terrier::metric
