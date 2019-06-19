#include <random>
#include <unordered_set>
#include <unordered_map>

#include "gtest/gtest.h"
#include "optimizer/statistics/count_min_sketch.h"

#include "util/test_harness.h"

namespace terrier::optimizer {

class CountMinSketchTests : public TerrierTest {};

// Basic CM-Sketch testing with integer datatype.
// NOLINTNEXTLINE
TEST_F(CountMinSketchTests, SimpleCountMinSketchIntegerTest) {
  CountMinSketch<int> sketch(10, 20, 0);
  EXPECT_EQ(sketch.GetDepth(), 10);
  EXPECT_EQ(sketch.GetWidth(), 20);
  EXPECT_EQ(sketch.GetSize(), 0);

  sketch.Add(1, 10);
  sketch.Add(2, 5);
  sketch.Add(3, 1);
  sketch.Add(4, 1000000);

  // This case count min sketch should give exact counting
  EXPECT_EQ(sketch.EstimateItemCount(1), 10);
  EXPECT_EQ(sketch.EstimateItemCount(2), 5);
  EXPECT_EQ(sketch.EstimateItemCount(3), 1);
  EXPECT_EQ(sketch.EstimateItemCount(4), 1000000);
}

// Basic testing with string datatype.
// NOLINTNEXTLINE
TEST_F(CountMinSketchTests, SimpleCountMinSketchStringTest) {
  CountMinSketch<std::string> sketch(10, 5, 0);
  EXPECT_EQ(sketch.GetDepth(), 10);
  EXPECT_EQ(sketch.GetWidth(), 5);
  EXPECT_EQ(sketch.GetSize(), 0);

  sketch.Add("10", 10);
  sketch.Add("5", 5);
  sketch.Add("1", 1);
  sketch.Add("Million", 1000000);

  EXPECT_EQ(sketch.EstimateItemCount("10"), 10);
}

// Basic CM-Sketch testing with integer datatype.
// NOLINTNEXTLINE
TEST_F(CountMinSketchTests, ApproximateIntegerTest) {
  double eps = 0.5;
  double gamma = 0.5;
  CountMinSketch<int> sketch(eps, gamma, 0);
  EXPECT_EQ(sketch.GetSize(), 0);

  // Populate the sketch with a bunch of random values
  // We will keep track of their exact counts in
  // a map to see how far off we are.
  int n = 100000;
  int max = 1000;

  std::unordered_map<int, int> exact;
  for (int i = 0; i <= max; i++) {
    exact[i] = 0;
  }
  std::unordered_set<int> seen;

  std::random_device rd;
  std::mt19937 generator(rd());
  std::normal_distribution<> distribution(0, max);
  for (int i = 0; i < n; i++) {
    auto number = static_cast<int>(distribution(generator));
    sketch.Add(number, 1);
    exact[number]++;
    seen.insert(number);
  }
  EXPECT_EQ(sketch.GetSize(), seen.size());

}


}  // namespace terrier::optimizer
