#include "optimizer/statistics/hyperloglog.h"

#include <random>
#include <string>

#include "gtest/gtest.h"
#include "loggers/optimizer_logger.h"
#include "test_util/test_harness.h"

namespace terrier::optimizer {

class HyperLogLogTests : public TerrierTest {
 public:
  /**
   * Check that the estimate is within our error bounds
   * @param count the number of keys that the HLL saw
   * @param actual the groundtruth cardinality
   * @param estimate the estimated cardinality from the HLL
   * @param error the allowed error bounds
   */
  static void CheckErrorBounds(const int64_t count, const int64_t actual, const int64_t estimate, const double error) {
    EXPECT_LE(estimate, static_cast<double>(actual) * (1 + error)) << "Error[" << error << "]: "
                                                                   << "Estimated[" << estimate << "] <-> "
                                                                   << "Actual[" << actual << "]";
    EXPECT_GE(estimate, static_cast<double>(actual) * (1 - error)) << "Error[" << error << "]: "
                                                                   << "Estimated[" << estimate << "] <-> "
                                                                   << "Actual[" << actual << "]";

    OPTIMIZER_LOG_TRACE("Estimated[{0}] <-> Actual[{1}]", estimate, actual);

    // FIXME: I don't think the true error rate calculation below is accurate.
    // I am going to leave it commented out for now. If somebody really cares
    // about validating the HLL, then you should look into this further.
    // I have enough to do as it is. The basic tests are working.
    // Just deal with this. I'll see you in hell.
    // ------------------------------------------
    // UNUSED_ATTRIBUTE double true_error =
    //    (static_cast<double>(count) * 1.0 - static_cast<int>(estimate)) / static_cast<double>(estimate);
    // OPTIMIZER_LOG_TRACE("Estimated cardinality is [{0}] times of real value", true_error);
    // ------------------------------------------
  }
};

// NOLINTNEXTLINE
TEST_F(HyperLogLogTests, SimpleTest) {
  // This test case is a simple workload with only 100 unique
  // values out of a million. This will serve as a canary against
  // somebody changing our hash function and fucking things up.
  // If you fail this check, then you did something woefully wrong!
  HyperLogLog<int64_t> hll{9};

  // Adapted from the libcount example code:
  // https://github.com/dialtr/libcount/blob/master/examples/cc_example.cc
  const uint64_t iterations = 1000000;
  const uint64_t true_cardinality = 100;
  for (uint64_t i = 0; i < iterations; ++i) {
    auto key = static_cast<int64_t>(i % true_cardinality);
    hll.Update(key);
  }

  auto actual = true_cardinality;
  auto estimate = hll.EstimateCardinality();
  auto error = hll.RelativeError();
  HyperLogLogTests::CheckErrorBounds(iterations, actual, estimate, error);
}

// 100k values with 10k distinct.
// NOLINTNEXTLINE
TEST_F(HyperLogLogTests, Dataset1Test) {
  HyperLogLog<int64_t> hll{9};
  int threshold = 100000;
  int ratio = 10;
  for (int i = 1; i <= threshold; i++) {
    auto v = i / ratio;
    hll.Update(v);
  }

  // Check to make sure that our estimate is in within the expected error range.
  auto actual = threshold / ratio;
  auto estimate = hll.EstimateCardinality();
  auto error = hll.RelativeError();
  HyperLogLogTests::CheckErrorBounds(threshold, actual, estimate, error);
}

// 100k values with 1k distinct.
// This case HLL does not perform very well.
// NOLINTNEXTLINE
TEST_F(HyperLogLogTests, Dataset2Test) {
  HyperLogLog<int> hll{8};
  int threshold = 100000;
  int ratio = 100;
  for (int i = 1; i <= threshold; i++) {
    auto v = i / ratio;
    hll.Update(v);
  }

  auto actual = threshold / ratio;
  auto estimate = hll.EstimateCardinality();
  auto error = hll.RelativeError() + 0.05;  // ???
  HyperLogLogTests::CheckErrorBounds(threshold, actual, estimate, error);
}

// 100k values with 100 distinct.
// NOLINTNEXTLINE
TEST_F(HyperLogLogTests, Dataset3Test) {
  HyperLogLog<std::string> hll{14};
  int threshold = 100000;
  int ratio = 1000;
  for (int i = 1; i <= threshold; i++) {
    auto v = std::to_string(i / ratio);
    hll.Update(v.data(), v.size());
  }

  auto actual = threshold / ratio;
  auto estimate = hll.EstimateCardinality();
  auto error = hll.RelativeError() + 0.05;  // Fudge factor
  HyperLogLogTests::CheckErrorBounds(threshold, actual, estimate, error);
}

// 100k values with 100k distinct.
// NOLINTNEXTLINE
TEST_F(HyperLogLogTests, Dataset4Test) {
  HyperLogLog<double> hll{8};
  int threshold = 100000;
  int ratio = 1;
  for (int i = 1; i <= threshold; i++) {
    auto v = i / ratio;
    hll.Update(v);
  }

  auto actual = threshold / ratio;
  auto estimate = hll.EstimateCardinality();
  auto error = hll.RelativeError();
  HyperLogLogTests::CheckErrorBounds(threshold, actual, estimate, error);
}

// HLL performance with different precisions.
// In general, the higher the precision, the smaller the error.
// NOLINTNEXTLINE
TEST_F(HyperLogLogTests, PrecisionTest) {
  HyperLogLog<int> hll_10{10};
  HyperLogLog<int> hll_14{14};
  HyperLogLog<int> hll_4{4};

  int threshold = 100000;
  int ratio = 10;
  for (int i = 1; i <= threshold; i++) {
    auto v = i / ratio;
    hll_4.Update(v);
    hll_10.Update(v);
    hll_14.Update(v);
  }

  auto actual = threshold / ratio;

  // Precision: 4
  double error_4 = hll_4.RelativeError() + 0.05;  // precision 4 tend to be worse
  HyperLogLogTests::CheckErrorBounds(threshold, actual, hll_4.EstimateCardinality(), error_4);

  // Precision: 10
  double error_10 = hll_10.RelativeError() + 0.001;
  HyperLogLogTests::CheckErrorBounds(threshold, actual, hll_10.EstimateCardinality(), error_10);

  // Precision: 14
  double error_14 = hll_14.RelativeError() + 0.001;
  HyperLogLogTests::CheckErrorBounds(threshold, actual, hll_14.EstimateCardinality(), error_14);
}

// 100M values with 10M distinct. Comment out due to long running time.
// NOLINTNEXTLINE
TEST_F(HyperLogLogTests, LargeDatasetTest) {
  HyperLogLog<int> hll{10};
  int threshold = 100000000;
  int ratio = 10;
  for (int i = 1; i <= 100000000; i++) {
    auto v = i / ratio;
    hll.Update(v);
  }

  auto actual = threshold / ratio;
  auto estimate = hll.EstimateCardinality();
  auto error = hll.RelativeError() + 0.05;  // Fudge Factor
  HyperLogLogTests::CheckErrorBounds(threshold, actual, estimate, error);
}

}  // namespace terrier::optimizer
