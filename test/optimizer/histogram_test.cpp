#include <random>
#include <sstream>

#include "gtest/gtest.h"
#include "optimizer/statistics/histogram.h"

#include "util/test_harness.h"

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
    int number = static_cast<int>(distribution(generator));
    h.Update(number);
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
    int number = static_cast<int>(distribution(generator));
    h.Update(number);
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
    int number = static_cast<int>(distribution(generator));
    h.Update(number);
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
    int number = static_cast<int>(distribution(generator));
    h.Update(number);
  }
  std::vector<double> res = h.Uniform();
  // ln 2 / lambda is the mean
  double threshold = std::log(2) / lambda;
  int count = 0;
  for (double x : res)
    if (x < threshold) count++;
  EXPECT_GE(count, static_cast<int>(res.size()) * 0.5);
}

// Handle error cases correctly.
// NOLINTNEXTLINE
TEST_F(HistogramTests, ValueTypeTest) {
  Histogram<int> int_h{100};
  int_h.Update(999);

  Histogram<uint16_t> smallint_h{100};
  smallint_h.Update(999);

  Histogram<float> float_h{100};
  float_h.Update(999.99f);

  Histogram<double> double_h{100};
  double_h.Update(999.99);
}

// NOLINTNEXTLINE
TEST_F(HistogramTests, SumTest) {
  Histogram<int> h{100};
  auto boundaries = h.Uniform();
  EXPECT_EQ(boundaries.size(), 0);

  EXPECT_EQ(h.Sum(0), 0);
  h.Update(5);
  EXPECT_EQ(h.Sum(3), 0);
  EXPECT_EQ(h.Sum(4), 0);
  EXPECT_EQ(h.Sum(5), 1);
  EXPECT_EQ(h.Sum(6), 1);
}

// Just check to make sure that the output string is valid
// NOLINTNEXTLINE
TEST_F(HistogramTests, OutputTest) {
  Histogram<int> h{100};
  for (int i = 0; i < 1000; i++) {
    h.Update(i);
  }
  std::ostringstream os;
  os << h;
  EXPECT_FALSE(os.str().empty());
}

}  // namespace terrier::optimizer
