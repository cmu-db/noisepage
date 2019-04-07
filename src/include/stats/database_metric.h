#pragma once

#include <string>
#include <unordered_map>
#include <utility>

#include "catalog/catalog_defs.h"
#include "stats/abstract_metric.h"

namespace terrier {

namespace transaction {
class TransactionContext;
}  // namespace transaction

namespace stats {
class DatabaseMetricRawData : public AbstractRawData {
 public:
  void IncrementTxnCommited(catalog::db_oid_t database_id) { counters_[database_id].first++; }

  void IncrementTxnAborted(catalog::db_oid_t database_id) { counters_[database_id].second++; }

  void Aggregate(AbstractRawData *other) override {
    auto other_db_metric = dynamic_cast<DatabaseMetricRawData *>(other);
    for (auto &entry : other_db_metric->counters_) {
      auto &this_counter = counters_[entry.first];
      auto &other_counter = entry.second;
      this_counter.first += other_counter.first;
      this_counter.second += other_counter.second;
    }
  }

  void UpdateAndPersist() override;

  /**
   * @return the type of the metric this object is holding the data for
   */
  MetricType GetMetricType() const override { return MetricType::DATABASE; }

 private:
  /**
   * Maps from database id to a pair of counters.
   *
   * First counter represents number of transactions committed and the second
   * one represents the number of transactions aborted.
   */
  std::unordered_map<catalog::db_oid_t, std::pair<int64_t, int64_t>> counters_;
};

class DatabaseMetric : public AbstractMetric<DatabaseMetricRawData> {
 public:
  void OnTransactionCommit(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid) override {
    GetRawData()->IncrementTxnCommited(database_oid);
  }

  void OnTransactionAbort(const transaction::TransactionContext *txn, catalog::db_oid_t database_oid) override {
    GetRawData()->IncrementTxnAborted(database_oid);
  }
};

}  // namespace stats
}  // namespace terrier
