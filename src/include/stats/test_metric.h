#pragma once

#include "abstract_metric.h"
#include "abstract_raw_data.h"

namespace terrier::stats {

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

  void UpdateAndPersist() override{};

  /**
   * @return the type of the metric this object is holding the data for
   */
  MetricType GetMetricType() const override { return MetricType::TEST; }

  /**
   * @return value of the test raw data
   */
  int GetCount() { return value_; }

 private:
  int value_;
};

class TestMetric : public AbstractMetric<TestMetricRawData> {
 public:
  void OnTest(int num) override { GetRawData()->Integrate(num); }
};

}  // namespace terrier::stats
