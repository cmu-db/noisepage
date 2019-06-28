#pragma once

#include <chrono>  //NOLINT
#include <fstream>
#include <string>
#include <unordered_map>
#include <utility>

#include "catalog/catalog_defs.h"
#include "common/scoped_timer.h"
#include "metrics/abstract_metric.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"

namespace terrier::metrics {

/**
 * Raw data object for holding stats collected at transaction level
 */
class TransactionMetricRawData : public AbstractRawData {
 public:
  /**
   * Set start time of transaction
   * @param txn_start start time (unique identifier) for txn
   */
  void SetTxnStart(const transaction::timestamp_t txn_start) {
    data_[txn_start].start_ = std::chrono::high_resolution_clock::now();
  }

  /**
   * Calculate transaction latency
   * @param txn_start start time (unique identifier) for txn
   */
  void CalculateTxnLatency(const transaction::timestamp_t txn_start) {
    auto end = std::chrono::high_resolution_clock::now();
    auto start = data_[txn_start].start_;
    data_[txn_start].latency_ =
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
  }

  /**
   * Increment the number of tuples read by one
   * @param txn_start start time (unique identifier) for txn
   */
  void IncrementTupleRead(const transaction::timestamp_t txn_start) { data_[txn_start].tuple_read_++; }

  /**
   * Increment the number of tuples updated by one
   * @param txn_start start time (unique identifier) for txn
   */
  void IncrementTupleUpdate(const transaction::timestamp_t txn_start) { data_[txn_start].tuple_update_++; }

  /**
   * Increment the number of tuples inserted by one
   * @param txn_start start time (unique identifier) for txn
   */
  void IncrementTupleInsert(const transaction::timestamp_t txn_start) { data_[txn_start].tuple_insert_++; }

  /**
   * Increment the number of tuples deleted by one
   * @param txn_start start time (unique identifier) for txn
   */
  void IncrementTupleDelete(const transaction::timestamp_t txn_start) { data_[txn_start].tuple_delete_++; }

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
  MetricsComponent GetMetricType() const override { return MetricsComponent::TRANSACTION; }

  /**
   * @return the latency of the given transaction
   */
  uint64_t GetLatency(const transaction::timestamp_t txn_start) { return data_[txn_start].latency_; }

  /**
   * @return the tuples read of the given transaction
   */
  uint64_t GetTupleRead(const transaction::timestamp_t txn_start) { return data_[txn_start].tuple_read_; }

  /**
   * @return the tuples updated of the given transaction
   */
  uint64_t GetTupleUpdate(const transaction::timestamp_t txn_start) { return data_[txn_start].tuple_update_; }

  /**
   * @return the tuples inserted of the given transaction
   */
  uint64_t GetTupleInsert(const transaction::timestamp_t txn_start) { return data_[txn_start].tuple_insert_; }

  /**
   * @return the tuples deleted of the given transaction
   */
  uint64_t GetTupleDelete(const transaction::timestamp_t txn_start) { return data_[txn_start].tuple_delete_; }

  void ToCSV(common::ManagedPointer<std::ofstream> outfile) const final {
    TERRIER_ASSERT(outfile->is_open(), "File not opened.");
  }

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
    uint64_t latency_;
    /**
     * Number of tuples read by the transaction
     */
    uint64_t tuple_read_;
    /**
     * Number of tuples inserted by the transaction
     */
    uint64_t tuple_insert_;
    /**
     * Number of tuples deleted by the transaction
     */
    uint64_t tuple_delete_;
    /**
     * Number of tuples updated by the transaction
     */
    uint64_t tuple_update_;
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
  void OnTransactionBegin(const transaction::TransactionContext &txn) override {
    GetRawData()->SetTxnStart(txn.StartTime());
  }

  /**
   * @param txn transaction context of the committing transaction
   * @param database_oid OID of the database the transaction is running in
   */
  void OnTransactionCommit(const transaction::TransactionContext &txn,
                           UNUSED_ATTRIBUTE catalog::db_oid_t database_oid) override {
    GetRawData()->CalculateTxnLatency(txn.StartTime());
  }

  /**
   * @param txn transaction context of the aborting transaction
   * @param database_oid OID of the database the transaction is running in
   */
  void OnTransactionAbort(const transaction::TransactionContext &txn,
                          UNUSED_ATTRIBUTE catalog::db_oid_t database_oid) override {
    GetRawData()->CalculateTxnLatency(txn.StartTime());
  }

  /**
   *
   * @param txn context of the transaction performing read
   * @param database_oid OID of the database that the tuple read happens
   * @param namespace_oid OID of the namespace that the tuple read happens
   * @param table_oid OID of the table that the tuple read happens
   */
  void OnTupleRead(const transaction::TransactionContext &txn, UNUSED_ATTRIBUTE catalog::db_oid_t database_oid,
                   UNUSED_ATTRIBUTE catalog::namespace_oid_t namespace_oid,
                   UNUSED_ATTRIBUTE catalog::table_oid_t table_oid) override {
    GetRawData()->IncrementTupleRead(txn.StartTime());
  }

  /**
   * @param txn context of the transaction performing update
   * @param database_oid OID of the database that the tuple update happens
   * @param namespace_oid OID of the namespace that the tuple update happens
   * @param table_oid OID of the table that the tuple update happens
   */
  void OnTupleUpdate(const transaction::TransactionContext &txn, UNUSED_ATTRIBUTE catalog::db_oid_t database_oid,
                     UNUSED_ATTRIBUTE catalog::namespace_oid_t namespace_oid,
                     UNUSED_ATTRIBUTE catalog::table_oid_t table_oid) override {
    GetRawData()->IncrementTupleUpdate(txn.StartTime());
  }

  /**
   * @param txn context of the transaction performing insert
   * @param database_oid OID of the database that the tuple insert happens
   * @param namespace_oid OID of the namespace that the tuple insert happens
   * @param table_oid OID of the table that the tuple insert happens
   */
  void OnTupleInsert(const transaction::TransactionContext &txn, UNUSED_ATTRIBUTE catalog::db_oid_t database_oid,
                     UNUSED_ATTRIBUTE catalog::namespace_oid_t namespace_oid,
                     UNUSED_ATTRIBUTE catalog::table_oid_t table_oid) override {
    GetRawData()->IncrementTupleInsert(txn.StartTime());
  }

  /**
   * @param txn Context of the transaction performing delete
   * @param database_oid OID of the database that the tuple delete happens
   * @param namespace_oid OID of the namespace that the tuple delete happens
   * @param table_oid OID of the table that the tuple delete happens
   */
  void OnTupleDelete(const transaction::TransactionContext &txn, UNUSED_ATTRIBUTE catalog::db_oid_t database_oid,
                     UNUSED_ATTRIBUTE catalog::namespace_oid_t namespace_oid,
                     UNUSED_ATTRIBUTE catalog::table_oid_t table_oid) override {
    GetRawData()->IncrementTupleDelete(txn.StartTime());
  }
};
}  // namespace terrier::metrics
