#pragma once

#include <cstdio>
#include "metric/abstract_metric.h"
#include "metric/abstract_raw_data.h"

namespace terrier::metric {

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

}  // namespace terrier::metric
