#include "optimizer/statistics/histogram.h"

#include <random>
#include <sstream>
#include <vector>

#include "gtest/gtest.h"
#include "test_util/test_harness.h"

namespace terrier::optimizer {

class HistogramTests : public TerrierTest {};

// 100k values with uniform distribution from 1 to 100.
// NOLINTNEXTLINE
TEST_F(HistogramTests, UniformDistTest) {
  Histogram<int> h{100};
  int n = 100000;
  std::default_random_engine generator;
  std::uniform_int_distribution<int> distribution(1, 100);
  for (int i = 0; i < n; i++) {
    auto number = static_cast<int>(distribution(generator));
    h.Increment(number);
  }
  std::vector<double> res = h.Uniform();
  for (int i = 1; i < 100; i++) {
    // Should have each value as histogram bound.
    EXPECT_EQ(i, std::floor(res[i - 1]));
  }
}

// Gaussian distribution with 100k values.
// NOLINTNEXTLINE
TEST_F(HistogramTests, GaussianDistTest) {
  Histogram<int> h{100};
  int n = 100000;
  std::random_device rd;
  std::mt19937 generator(rd());
  std::normal_distribution<> distribution(0, 10);
  for (int i = 0; i < n; i++) {
    auto number = static_cast<int>(distribution(generator));
    h.Increment(number);
  }
  std::vector<double> res = h.Uniform();
  // This should have around 68% data in one stdev [-10, 10]
  int count = 0;
  for (double x : res)
    if (x >= -10 && x <= 10) count++;
  EXPECT_GE(count, static_cast<int>(res.size()) * 0.68);
}

// Log-normal distribution with 100k values.
// NOLINTNEXTLINE
TEST_F(HistogramTests, LeftSkewedDistTest) {
  Histogram<int> h{100};
  int n = 100000;
  std::random_device rd;
  std::mt19937 generator(rd());
  std::lognormal_distribution<> distribution(0, 1);
  for (int i = 0; i < n; i++) {
    auto number = static_cast<int>(distribution(generator));
    h.Increment(number);
  }
  std::vector<double> res = h.Uniform();
}

// Exponential distribution.
// NOLINTNEXTLINE
TEST_F(HistogramTests, ExponentialDistTest) {
  Histogram<int> h{100};
  int n = 100000;
  double lambda = 1;
  std::random_device rd;
  std::mt19937 generator(rd());
  std::exponential_distribution<> distribution(lambda);
  for (int i = 0; i < n; i++) {
    auto number = static_cast<int>(distribution(generator));
    h.Increment(number);
  }
  std::vector<double> res = h.Uniform();
  // ln 2 / lambda is the mean
  double threshold = std::log(2) / lambda;
  int count = 0;
  for (double x : res)
    if (x < threshold) count++;
  EXPECT_GE(count, static_cast<int>(res.size()) * 0.5);
}

// Make sure that the histogram works for different value types
// NOLINTNEXTLINE
TEST_F(HistogramTests, ValueTypeTest) {
  uint8_t num_bins = 99;

  Histogram<int> int_h{num_bins};
  int_h.Increment(777);
  int_h.Increment(999);
  EXPECT_EQ(int_h.GetMaxBinSize(), num_bins);
  EXPECT_EQ(int_h.GetMinValue(), 777);
  EXPECT_EQ(int_h.GetMaxValue(), 999);
  EXPECT_EQ(int_h.GetTotalValueCount(), 2);

  Histogram<uint16_t> smallint_h{num_bins};
  smallint_h.Increment(777);
  smallint_h.Increment(999);
  EXPECT_EQ(smallint_h.GetMinValue(), 777);
  EXPECT_EQ(smallint_h.GetMaxValue(), 999);
  EXPECT_EQ(smallint_h.GetTotalValueCount(), 2);

  Histogram<float> float_h{num_bins};
  float_h.Increment(777.77F);
  float_h.Increment(999.99F);
  EXPECT_EQ(float_h.GetMinValue(), 777.77F);
  EXPECT_EQ(float_h.GetMaxValue(), 999.99F);
  EXPECT_EQ(float_h.GetTotalValueCount(), 2);

  Histogram<double> double_h{num_bins};
  double_h.Increment(777.77);
  double_h.Increment(999.99);
  EXPECT_EQ(double_h.GetMinValue(), 777.77);
  EXPECT_EQ(double_h.GetMaxValue(), 999.99);
  EXPECT_EQ(double_h.GetTotalValueCount(), 2);
}

// NOLINTNEXTLINE
TEST_F(HistogramTests, EstimateItemCountTest) {
  Histogram<int> h{100};
  auto boundaries = h.Uniform();
  EXPECT_EQ(boundaries.size(), 0);

  EXPECT_EQ(h.EstimateItemCount(0), 0);
  h.Increment(5);
  EXPECT_EQ(h.EstimateItemCount(3), 0);
  EXPECT_EQ(h.EstimateItemCount(4), 0);
  EXPECT_EQ(h.EstimateItemCount(5), 1);
  EXPECT_EQ(h.EstimateItemCount(6), 1);
}

// Just check to make sure that the output string is valid
// NOLINTNEXTLINE
TEST_F(HistogramTests, OutputTest) {
  Histogram<int> h{100};
  for (int i = 0; i < 1000; i++) {
    h.Increment(i);
  }
  std::ostringstream os;
  os << h;
  EXPECT_FALSE(os.str().empty());
}

}  // namespace terrier::optimizer
