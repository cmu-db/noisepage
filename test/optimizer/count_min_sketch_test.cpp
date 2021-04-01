#include "optimizer/statistics/count_min_sketch.h"

#include <cmath>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"
#include "test_util/test_harness.h"

namespace noisepage::optimizer {

class CountMinSketchTests : public TerrierTest {};

// Basic CM-Sketch testing with integer datatype.
// NOLINTNEXTLINE
TEST_F(CountMinSketchTests, BasicIntegerTest) {
  CountMinSketch<int> sketch(1000);
  EXPECT_EQ(sketch.GetWidth(), 1000);

  sketch.Increment(1, 10);
  sketch.Increment(2, 5);
  sketch.Increment(3, 1);
  sketch.Increment(4, 100000);

  // These smaller values should give exact counts
  EXPECT_EQ(sketch.EstimateItemCount(1), 10);
  EXPECT_EQ(sketch.EstimateItemCount(2), 5);
  EXPECT_EQ(sketch.EstimateItemCount(3), 1);

  // The last one can be larger but *not* smaller
  EXPECT_GE(sketch.EstimateItemCount(4), 100000);

  // Try clearing sketch
  sketch.Clear();
  EXPECT_EQ(sketch.GetTotalCount(), 0);
  EXPECT_EQ(sketch.EstimateItemCount(1), 0);
  EXPECT_EQ(sketch.EstimateItemCount(2), 0);
  EXPECT_EQ(sketch.EstimateItemCount(3), 0);
  EXPECT_EQ(sketch.EstimateItemCount(4), 0);
}

// Basic testing with string datatype.
// NOLINTNEXTLINE
TEST_F(CountMinSketchTests, BasicStringTest) {
  CountMinSketch<const char *> sketch(1000);
  EXPECT_EQ(sketch.GetWidth(), 1000);

  sketch.Increment("10", 10);
  sketch.Increment("5", 5);
  sketch.Increment("1", 1);
  sketch.Increment("Million", 1000000);

  EXPECT_EQ(sketch.EstimateItemCount("10"), 10);
  EXPECT_EQ(sketch.EstimateItemCount("5"), 5);
  EXPECT_EQ(sketch.EstimateItemCount("1"), 1);

  // The last one can be larger but *not* smaller
  EXPECT_GE(sketch.EstimateItemCount("Million"), 100000);

  // Checking to see whether we get a false positive
  // This is probably a bad idea but it might be okay for a small
  // sketch like this.
  //
  // DEAR FUTURE TRAVELLER:
  // If this line below is failing, then you can just remove it.
  // EXPECT_EQ(sketch.EstimateItemCount("WuTang"), 0);

  // Try clearing sketch
  sketch.Clear();
  EXPECT_EQ(sketch.GetTotalCount(), 0);
  EXPECT_EQ(sketch.EstimateItemCount("10"), 0);
  EXPECT_EQ(sketch.EstimateItemCount("5"), 0);
  EXPECT_EQ(sketch.EstimateItemCount("1"), 0);
  EXPECT_EQ(sketch.EstimateItemCount("Million"), 0);
}

// NOLINTNEXTLINE
TEST_F(CountMinSketchTests, MergeTest) {
  CountMinSketch<int> sketch1(1000);
  EXPECT_EQ(sketch1.GetWidth(), 1000);
  CountMinSketch<int> sketch2(1000);
  EXPECT_EQ(sketch1.GetWidth(), 1000);

  sketch1.Increment(1, 10);
  sketch1.Increment(2, 5);
  sketch2.Increment(3, 1);
  sketch2.Increment(4, 100000);

  // These smaller values should give exact counts
  EXPECT_EQ(sketch1.EstimateItemCount(1), 10);
  EXPECT_EQ(sketch1.EstimateItemCount(2), 5);
  EXPECT_EQ(sketch2.EstimateItemCount(3), 1);

  // The last one can be larger but *not* smaller
  EXPECT_GE(sketch2.EstimateItemCount(4), 100000);

  sketch1.Merge(sketch2);
  // These smaller values should give exact counts
  EXPECT_EQ(sketch1.EstimateItemCount(1), 10);
  EXPECT_EQ(sketch1.EstimateItemCount(2), 5);
  EXPECT_EQ(sketch1.EstimateItemCount(3), 1);

  // The last one can be larger but *not* smaller
  EXPECT_GE(sketch1.EstimateItemCount(4), 100000);
}

// NOLINTNEXTLINE
TEST_F(CountMinSketchTests, IntSerializationTest) {
  CountMinSketch<int> sketch(1000);
  EXPECT_EQ(sketch.GetWidth(), 1000);

  sketch.Increment(1, 10);
  sketch.Increment(2, 5);
  sketch.Increment(3, 1);
  sketch.Increment(4, 100000);

  // These smaller values should give exact counts
  EXPECT_EQ(sketch.EstimateItemCount(1), 10);
  EXPECT_EQ(sketch.EstimateItemCount(2), 5);
  EXPECT_EQ(sketch.EstimateItemCount(3), 1);

  // The last one can be larger but *not* smaller
  EXPECT_GE(sketch.EstimateItemCount(4), 100000);

  size_t size;
  auto serialized_sketch = sketch.Serialize(&size);
  auto deserialized_sketch = CountMinSketch<int>::Deserialize(serialized_sketch.get(), size);

  // These smaller values should give exact counts
  EXPECT_EQ(deserialized_sketch.EstimateItemCount(1), 10);
  EXPECT_EQ(deserialized_sketch.EstimateItemCount(2), 5);
  EXPECT_EQ(deserialized_sketch.EstimateItemCount(3), 1);

  // The last one can be larger but *not* smaller
  EXPECT_GE(deserialized_sketch.EstimateItemCount(4), 100000);
}

// NOLINTNEXTLINE
TEST_F(CountMinSketchTests, StringSerializationTest) {
  CountMinSketch<const char *> sketch(1000);
  EXPECT_EQ(sketch.GetWidth(), 1000);

  sketch.Increment("10", 10);
  sketch.Increment("5", 5);
  sketch.Increment("1", 1);
  sketch.Increment("Million", 1000000);

  EXPECT_EQ(sketch.EstimateItemCount("10"), 10);
  EXPECT_EQ(sketch.EstimateItemCount("5"), 5);
  EXPECT_EQ(sketch.EstimateItemCount("1"), 1);

  // The last one can be larger but *not* smaller
  EXPECT_GE(sketch.EstimateItemCount("Million"), 100000);

  size_t size;
  auto serialized_sketch = sketch.Serialize(&size);
  auto deserialized_sketch = CountMinSketch<const char *>::Deserialize(serialized_sketch.get(), size);

  EXPECT_EQ(deserialized_sketch.EstimateItemCount("10"), 10);
  EXPECT_EQ(deserialized_sketch.EstimateItemCount("5"), 5);
  EXPECT_EQ(deserialized_sketch.EstimateItemCount("1"), 1);

  // The last one can be larger but *not* smaller
  EXPECT_GE(deserialized_sketch.EstimateItemCount("Million"), 100000);
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
  CountMinSketch<int> weak_sketch(10);
  CountMinSketch<int> strong_sketch(1000);

  EXPECT_EQ(weak_sketch.GetTotalCount(), 0);
  EXPECT_EQ(strong_sketch.GetTotalCount(), 0);
  EXPECT_GT(strong_sketch.GetSize(), weak_sketch.GetSize());

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
    weak_sketch.Increment(number, 1);
    strong_sketch.Increment(number, 1);
    exact[number]++;
    seen.insert(number);
  }

  // The total count should be accurate
  EXPECT_EQ(weak_sketch.GetTotalCount(), n);
  EXPECT_EQ(strong_sketch.GetTotalCount(), n);

  // The weak sketch should produce counts that are always greater
  // because it has less storage space. That means all of the random
  // numbers we insert into are going to get falsely incremented
  for (auto number : seen) {
    auto weak_cnt = weak_sketch.EstimateItemCount(number);
    auto strong_cnt = strong_sketch.EstimateItemCount(number);
    // Kill ourselves after the first one goes bad so that we don't
    // flood the terminal with garbage. Protect your neck!
    ASSERT_GT(weak_cnt, strong_cnt) << "exact[" << number << "]=" << exact[number];
  }
}

// Load up the sketch with random values and then blast that mofo with
// the same value. Of all the values that we put in it, the heavy hitter
// one should have the highest approximate count
// NOLINTNEXTLINE
TEST_F(CountMinSketchTests, HeavyHitterTest) {
  CountMinSketch<int> sketch(10000);

  // Populate the sketch with a bunch of random values
  // We will keep track of their exact counts in
  // a map to see how far off we are.
  int n = 5000;
  int max = 1000;

  std::unordered_set<int> seen;

  // Fill the sketch up with a bunch of random numbers
  std::random_device rd;
  std::mt19937 generator(rd());
  std::normal_distribution<> distribution(0, max);
  for (int i = 0; i < n; i++) {
    int number;
    do {
      number = static_cast<int>(distribution(generator));
    } while (seen.find(number) != seen.end());
    sketch.Increment(number, 1);
    seen.insert(number);
  }

  // Now we're going to generate a certain number of heavy hitters
  // We have a vector so that we know the order of the numbers as
  // they were generated. Then we have an unordered_set because I'm
  // lazy and I wanted a more simple find look-up below.
  int num_heavy = 3;
  std::vector<int> heavy_nums;
  std::unordered_set<int> heavy_nums_set;
  for (int i = 0; i < num_heavy; i++) {
    // Pick a random # and make sure that we haven't already picked it before!
    int heavy_num;
    do {
      heavy_num = static_cast<int>(distribution(generator));
    } while (heavy_nums_set.find(heavy_num) != heavy_nums_set.end());

    // For each heavy hitter number, we're going increase its
    // count by increasingly larger deltas.
    // So the last number in 'heavy_nums' should have the largest
    // approximate count in the sketch
    int delta = static_cast<int>(pow(max, i + 1)) * 3;
    sketch.Increment(heavy_num, delta);
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
      // Skip heavy hitter numbers
      if (heavy_nums_set.find(number) != heavy_nums_set.end()) continue;
      EXPECT_LE(sketch.EstimateItemCount(number), heavy_apprx) << "number=" << number;
    }

    // The count for this heavy hitter should also be greater
    // than all previous heavy hitters
    for (int j = 0; j < i; j++) {
      EXPECT_NE(heavy_num, heavy_nums[j]);
      EXPECT_LE(sketch.EstimateItemCount(heavy_nums[j]), heavy_apprx) << "other=" << heavy_nums[j];
    }
  }
}

// Check that the remove method works
// NOLINTNEXTLINE
TEST_F(CountMinSketchTests, RemoveTest) {
  CountMinSketch<int> sketch(1000);
  int num_keys = 1000;
  int max_count = 100;

  for (int key = 0; key < num_keys; key++) {
    sketch.Increment(key, max_count);
    ASSERT_GE(sketch.EstimateItemCount(key), max_count) << "Key[" << key << "]";
  }

  // Then remove the keys one-by-one
  for (int key = 0; key < num_keys; key++) {
    sketch.Remove(key);

    // Since this an approximate data structure, we can't guarantee
    // that the count for this particular key will be zero after
    // we remove it.
    // ASSERT_LE(sketch.EstimateItemCount(key), max_count) << "Key[" << key << "]";
  }

  // The total count should now be zero now
  EXPECT_EQ(sketch.GetTotalCount(), 0);
}

}  // namespace noisepage::optimizer
