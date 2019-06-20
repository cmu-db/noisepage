#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"
#include "optimizer/statistics/count_min_sketch.h"

#include "util/test_harness.h"

namespace terrier::optimizer {

class CountMinSketchTests : public TerrierTest {};

// Basic CM-Sketch testing with integer datatype.
// NOLINTNEXTLINE
TEST_F(CountMinSketchTests, BasicIntegerTest) {
  CountMinSketch<int> sketch(10, 20);
  EXPECT_EQ(sketch.GetDepth(), 10);
  EXPECT_EQ(sketch.GetWidth(), 20);
  EXPECT_EQ(sketch.GetApproximateSize(), 0);

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
TEST_F(CountMinSketchTests, BasicStringTest) {
  CountMinSketch<std::string> sketch(10, 5);
  EXPECT_EQ(sketch.GetDepth(), 10);
  EXPECT_EQ(sketch.GetWidth(), 5);
  EXPECT_EQ(sketch.GetApproximateSize(), 0);

  sketch.Add("10", 10);
  sketch.Add("5", 5);
  sketch.Add("1", 1);
  sketch.Add("Million", 1000000);

  EXPECT_EQ(sketch.EstimateItemCount("10"), 10);
}

// Basic test that checks that the approximation is reasonable
// It's hard to test this for real because
// NOLINTNEXTLINE
TEST_F(CountMinSketchTests, ApproximateIntegerTest) {
  // Create two sketches
  // The first is 'weak' meaning that it has less storage space
  // and there for its approximations will be more inaccurate.
  // The second sketch is 'strong' and should produce more accurate
  // approximations than the 'weak' one.

  double eps = 0.02;
  double gamma = 0.02;
  CountMinSketch<int> weak_sketch(eps, gamma);
  CountMinSketch<int> strong_sketch(100, 200);

  EXPECT_EQ(weak_sketch.GetApproximateSize(), 0);
  EXPECT_EQ(strong_sketch.GetApproximateSize(), 0);

  // Populate the sketch with a bunch of random values
  // We will keep track of their exact counts in
  // a map to see how far off we are.
  int n = 50000;
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
    weak_sketch.Add(number, 1);
    strong_sketch.Add(number, 1);
    exact[number]++;
    seen.insert(number);
  }

  // The total count should be accurate
  EXPECT_EQ(weak_sketch.GetTotalCount(), n);
  EXPECT_EQ(strong_sketch.GetTotalCount(), n);

  // The second sketch should have a higher approximate key count
  // because it has wider + deeper bins.
  EXPECT_LT(weak_sketch.GetApproximateSize(), strong_sketch.GetApproximateSize());

  // And both of them should be less than actual size
  EXPECT_LT(weak_sketch.GetApproximateSize(), seen.size());
  EXPECT_LT(strong_sketch.GetApproximateSize(), seen.size());

  // And there should be no false negatives
  for (auto number : seen) {
    EXPECT_GT(weak_sketch.EstimateItemCount(number), 0);
    EXPECT_GT(strong_sketch.EstimateItemCount(number), 0);
  }
}

// Load up the sketch with random values and then blast that mofo with
// the same value. Of all the values that we put in it, the heavy hitter
// one should have the highest approximate count
// NOLINTNEXTLINE
TEST_F(CountMinSketchTests, HeavyHitterTest) {
  CountMinSketch<int> sketch(100, 200);
  EXPECT_EQ(sketch.GetApproximateSize(), 0);

  // Populate the sketch with a bunch of random values
  // We will keep track of their exact counts in
  // a map to see how far off we are.
  int n = 50000;
  int max = 10000;

  std::unordered_set<int> seen;

  // Fill the sketch up with a bunch of random numbers
  std::random_device rd;
  std::mt19937 generator(rd());
  std::normal_distribution<> distribution(0, max);
  for (int i = 0; i < n; i++) {
    auto number = static_cast<int>(distribution(generator));
    sketch.Add(number, 1);
    seen.insert(number);
  }

  // Now we're going to generate a certain number of heavy hitters
  // We have a vector so that we know the order of the numbers as
  // they were generated. Then we have an unordered_set because I'm
  // lazy and I wanted a more simple find look-up below.
  int num_heavy = 4;
  std::vector<int> heavy_nums;
  std::unordered_set<int> heavy_nums_set;
  for (int i = 0; i < num_heavy; i++) {
    auto heavy_num = static_cast<int>(distribution(generator));

    // For each heavy hitter number, we're going increase its
    // count by increasingly larger deltas.
    // So the last number in 'heavy_nums' should have the largest
    // approximate count in the sketch
    int delta = (i + 1) * max;
    sketch.Add(heavy_num, delta);
    seen.insert(heavy_num);
    heavy_nums.push_back(heavy_num);
    heavy_nums_set.insert(heavy_num);
  }

  // After we've populated the sketch, let's go check
  for (int i = 0; i < num_heavy; i++) {
    auto heavy_num = heavy_nums[i];

    // Make sure that the approximate counts for the heavy hitters
    // are always greater than all other items in the sketch
    auto heavy_apprx = sketch.EstimateItemCount(heavy_num);
    EXPECT_GT(heavy_apprx, 0);
    for (auto number : seen) {
      if (heavy_nums_set.find(number) != heavy_nums_set.end()) continue;
      EXPECT_LT(sketch.EstimateItemCount(number), heavy_apprx) << "number=" << number;
    }

    // The count for this heavy hitter should also be greater
    // than all previous heavy hitters
    for (int j = 0; j < i; j++) {
      EXPECT_NE(heavy_num, heavy_nums[j]);
      EXPECT_LT(sketch.EstimateItemCount(heavy_nums[j]), heavy_apprx) << "other=" << heavy_nums[j];
    }
  }
}

}  // namespace terrier::optimizer
