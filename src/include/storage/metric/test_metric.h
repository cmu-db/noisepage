#pragma once

#include <cstdio>
#include "catalog/catalog.h"
#include "storage/metric/abstract_metric.h"
#include "storage/metric/abstract_raw_data.h"

namespace terrier::storage::metric {

/**
 * @brief raw data type for testing purpose
 */
class TestMetricRawData : public AbstractRawData {
 public:
  /**
   * @brief integrate the count with the number specified
   * @param num number to be integrate
   */
  void Integrate(int num) { value_ += num; }

  /**
   * @brief aggregate the counts
   * @param other
   */
  void Aggregate(AbstractRawData *other) override {
    auto other_test = dynamic_cast<TestMetricRawData *>(other);
    value_ += other_test->GetCount();
  }

  /**
   * Make necessary updates to the metric raw data and persist the content of
   * this RawData into internal SQL tables. Expect this object
   * to be garbage-collected after this method is called.
   * @param txn_manager transaction manager of the system
   * @param catalog catalog of the system
   * @param txn transaction context used for table lookup/creation
   */
  void UpdateAndPersist(transaction::TransactionManager *txn_manager, catalog::Catalog *catalog,
                        transaction::TransactionContext *txn) override {}

  /**
   * Get the SQL table for persisting collected data, create a new table if necessary
   * @param txn_manager transaction manager of the system
   * @param catalog catalog of the system
   * @param txn transaction context used for table lookup/creation
   */
  catalog::SqlTableHelper *GetStatsTable(transaction::TransactionManager *txn_manager, catalog::Catalog *catalog,
                                         transaction::TransactionContext *txn) override {
    return nullptr;
  }

  /**
   * @return the type of the metric this object is holding the data for
   */
  MetricType GetMetricType() const override { return MetricType::TEST; }

  /**
   * @return value of the test raw data
   */
  int GetCount() { return value_; }

 private:
  /**
   * Representation of some data
   */
  int value_;
};

/**
 * @brief Interface around test raw data
 */
class TestMetric : public AbstractMetric<TestMetricRawData> {
 public:
  /**
   * @param num value to integrate with test raw data
   */
  void OnTest(int num) override { GetRawData()->Integrate(num); }
};

}  // namespace terrier::storage::metric
