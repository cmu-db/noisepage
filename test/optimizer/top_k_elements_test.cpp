#include "optimizer/statistics/top_k_elements.h"

#include <stack>
#include <string>
#include <unordered_map>
#include <vector>

#include "gtest/gtest.h"
#include "loggers/optimizer_logger.h"
#include "optimizer/statistics/count_min_sketch.h"
#include "test_util/test_harness.h"

namespace terrier::optimizer {

class TopKElementsTests : public TerrierTest {
  void SetUp() override {
#if NOISEPAGE_USE_LOGGER
    optimizer::optimizer_logger->set_level(spdlog::level::info);
#endif
  }
};

// Check that we can do simple increments to the top-k trackre
// NOLINTNEXTLINE
TEST_F(TopKElementsTests, SimpleIncrementTest) {
  const int k = 5;
  TopKElements<int> top_k(k, 1000);
  EXPECT_EQ(top_k.GetK(), k);
  EXPECT_EQ(top_k.GetSize(), 0);

  top_k.Increment(1, 10);
  top_k.Increment(2, 5);
  top_k.Increment(3, 1);
  top_k.Increment(4, 1000000);

  // Since we set the top-k to track 5 keys,
  // all of these keys should return exact results
  // because we only have four keys in there now.
  EXPECT_EQ(top_k.EstimateItemCount(1), 10);
  EXPECT_EQ(top_k.EstimateItemCount(2), 5);
  EXPECT_EQ(top_k.EstimateItemCount(3), 1);
  EXPECT_EQ(top_k.EstimateItemCount(4), 1000000);

  // Make sure the size matches exactly the number
  // of keys we have thrown at it.
  EXPECT_EQ(top_k.GetSize(), 4);

  // Add another value
  top_k.Increment(5, 15);
  EXPECT_EQ(top_k.GetSize(), 5);

  OPTIMIZER_LOG_TRACE(top_k);
}

// Check that if incrementally increase the count of a key that
// it will eventually get promoted to be in the top-k list
// NOLINTNEXTLINE
TEST_F(TopKElementsTests, PromotionTest) {
  const int k = 10;
  TopKElements<int> top_k(k, 1000);

  int num_keys = k * 2;
  int large_count = 1000;
  for (int key = 1; key <= num_keys; key++) {
    // If the key is below the half way point for
    // the number of keys we're inserted, then make
    // it's count super large
    if (key <= k) {
      top_k.Increment(key, large_count);

      // Otherwise just set it a small number
    } else {
      top_k.Increment(key, 99);
    }
  }

  // Now pick the largest key and keep incrementing
  // it until it is larger than 5x the large_count
  // At this point it should be in our top-k list
  auto target_key = num_keys;
  for (int i = 0; i < large_count * 5; i++) {
    top_k.Increment(target_key, 1);
  }
  auto sorted_keys = top_k.GetSortedTopKeys();
  auto found = std::find(sorted_keys.begin(), sorted_keys.end(), target_key);
  EXPECT_NE(found, sorted_keys.end());

  // Now do the same thing but instead of incrementally updating
  // the target key's count, just hit it once with a single update.
  target_key = num_keys - 1;
  top_k.Increment(target_key, large_count * 15);
  sorted_keys = top_k.GetSortedTopKeys();
  found = std::find(sorted_keys.begin(), sorted_keys.end(), target_key);
  EXPECT_NE(found, sorted_keys.end());
}

// Check that that we can get a proper list of sorted keys back
// of the top-k elements.
// NOLINTNEXTLINE
TEST_F(TopKElementsTests, SortedKeyTest) {
  // test TopKElements
  const int k = 10;
  TopKElements<std::string> top_k(k, 1000);
  std::stack<std::string> expected_keys;

  int num_keys = 500;
  for (int i = 1; i <= num_keys; i++) {
    auto key = std::to_string(i) + "!";
    top_k.Increment(key, key.size(), i * 1000);

    // If this key is within the last k entries that we are
    // putting into the top-k tracker, then add it to our
    // stack. This will the order of the keys that we
    // expected to get back when we ask for them in sorted order.
    if (i >= (num_keys - k)) expected_keys.push(key);

    // If we have inserted less than k keys into the top-k
    // tracker, then the number of keys added should be equal
    // to the size of the top-k tracker.
    if (i < k) {
      EXPECT_EQ(top_k.GetSize(), i);
    } else {
      EXPECT_EQ(top_k.GetSize(), k);
    }
  }

  // The top-k elements should be the last k numbers
  // that we added into the object
  auto sorted_keys = top_k.GetSortedTopKeys();
  EXPECT_EQ(sorted_keys.size(), k);
  int i = 0;
  for (UNUSED_ATTRIBUTE const auto &key : sorted_keys) {
    // Pop off the keys from our expected stack each time.
    // It should match the current key in our sorted key list
    auto expected_key = expected_keys.top();
    expected_keys.pop();

    // TODO(pavlo): This text case is in correct because we can't
    // guarantee that the keys we shove in will have the exact amount
    // that we originally set them to.
    OPTIMIZER_LOG_TRACE("Top-{0}: {1} <-> {2}", i, key, expected_key);
    // EXPECT_EQ(key, expected_key) << "Iteration #" << i;
    i++;
  }
}

// Check that we can increment and decrement correctly
// NOLINTNEXTLINE
TEST_F(TopKElementsTests, SimpleIncrementDecrementTest) {
  // test TopKElements
  const int k = 5;
  TopKElements<int> top_k(k, 1000);

  std::unordered_map<int, int> expected = {
      {10, 10}, {5, 5}, {99, 99}, {999, 999}, {1, 1},
      // {10000, 1000000}
  };

  for (const auto &entry : expected) {
    top_k.Increment(entry.first, entry.second);
  }
  for (const auto &entry : expected) {
    EXPECT_EQ(top_k.EstimateItemCount(entry.first), entry.second);
  }

  // Add 5 to the existing keys
  for (const auto &entry : expected) {
    top_k.Increment(entry.first, 5);
    expected[entry.first] += 5;
  }
  for (const auto &entry : expected) {
    EXPECT_EQ(top_k.EstimateItemCount(entry.first), entry.second);
  }
  EXPECT_EQ(top_k.GetSize(), k);
  // top_k.PrintTopKQueueOrderedMaxFirst(10);

  // Subtract 5 from all of the keys
  for (const auto &entry : expected) {
    top_k.Decrement(entry.first, 5);
    expected[entry.first] -= 5;
  }
  for (const auto &entry : expected) {
    EXPECT_EQ(top_k.EstimateItemCount(entry.first), entry.second);
  }
  // top_k.PrintTopKQueueOrderedMaxFirst(10);
}

// This checks that our top-k thingy does not mess up its
// internal data structures if we try to decrement keys
// that it has never seen before.
// NOLINTNEXTLINE
TEST_F(TopKElementsTests, DecrementNonExistingKeyTest) {
  const int k = 5;
  TopKElements<int> top_k(k, 1000);

  // Add some real keys
  for (int key = 0; key < k; key++) {
    top_k.Increment(key, 1);
  }
  EXPECT_EQ(top_k.GetSize(), k);
  auto sorted_keys = top_k.GetSortedTopKeys();
  EXPECT_EQ(sorted_keys.size(), k);

  // Decrement random keys that don't exist
  for (int key = k + 1; key < 10; key++) {
    // It's count should be less than or equal to zero.
    auto count = top_k.EstimateItemCount(key);
    EXPECT_LE(count, 0);

    top_k.Decrement(key, 1);
    top_k.Decrement(key, 1);
  }
  EXPECT_EQ(top_k.GetSize(), k);

  // Make sure that we only have keys that we expect to have
  sorted_keys = top_k.GetSortedTopKeys();
  EXPECT_EQ(sorted_keys.size(), k);
  for (int key = 0; key < k; key++) {
    auto found = std::find(sorted_keys.begin(), sorted_keys.end(), key);
    EXPECT_NE(found, sorted_keys.end());
  }
}

// Check that if we decrement a key enough that it goes
// negative that it gets removed from our top-k entries
// NOLINTNEXTLINE
TEST_F(TopKElementsTests, NegativeCountTest) {
  const int k = 5;
  TopKElements<int> top_k(k, 1000);
  const int max_count = 222;

  std::unordered_map<int, int> expected;
  for (int i = 1; i <= k; i++) {
    top_k.Increment(i, max_count);
    expected[i] = max_count;
  }
  EXPECT_EQ(top_k.GetSize(), k);

  // Throw in an extra key just to show that we aren't able
  // to promote a key from the sketch if one key's count
  // goes negative
  top_k.Increment(k + 1, 1);
  EXPECT_EQ(top_k.GetSize(), k);

  // Now take the last key and decrement it until it goes negative
  for (int i = max_count; i > 0; i--) {
    top_k.Decrement(k, 1);
    expected[i] -= 1;
  }
  EXPECT_EQ(top_k.GetSize(), k - 1);

  // Make sure that the last key does not exist in our
  // list of sorted keys. No other key should get promoted
  // because the top-k class doesn't know about them.
  auto sorted_keys = top_k.GetSortedTopKeys();
  EXPECT_EQ(sorted_keys.size(), k - 1);
  auto found = std::find(sorted_keys.begin(), sorted_keys.end(), k - 1);
  EXPECT_NE(found, sorted_keys.end());
}

// Another simple check for incrementing
// NOLINTNEXTLINE
TEST_F(TopKElementsTests, IncrementOnlyTest) {
  const int k = 20;
  UNUSED_ATTRIBUTE const int num0 = 10;
  TopKElements<int> top_k(k, 1000);

  top_k.Increment(10, 10);
  top_k.Increment(5, 5);
  top_k.Increment(1, 1);
  top_k.Increment(1000000, 1000000);

  top_k.Increment(7777, 2333);
  top_k.Increment(8888, 2334);
  top_k.Increment(9999, 2335);
  for (int i = 0; i < 30; ++i) {
    top_k.Increment(i, i);
  }

  auto sorted_keys = top_k.GetSortedTopKeys();
  EXPECT_EQ(top_k.GetSize(), k);
  EXPECT_EQ(sorted_keys.size(), k);

  // I don't know what this test was really trying to do here...
  // EXPECT_EQ(sorted_keys[0], num0);
  // EXPECT_EQ(top_k.GetOrderedMaxFirst(num0).size(), num0);
  // EXPECT_EQ(top_k.GetAllOrderedMaxFirst().size(), k);

  for (int i = 1000; i < 2000; ++i) {
    top_k.Increment(i, i);
  }
  sorted_keys = top_k.GetSortedTopKeys();
  EXPECT_EQ(top_k.GetSize(), k);
  EXPECT_EQ(sorted_keys.size(), k);
  // top_k.PrintAllOrderedMaxFirst();
}

// Test that we can put doubles in our top-k tracker
// NOLINTNEXTLINE
TEST_F(TopKElementsTests, DoubleTest) {
  const int k = 5;
  TopKElements<double> top_k(k, 1000);

  for (int i = 0; i < 1000; i++) {
    auto v = 7.12 + i;
    top_k.Increment(v, 1);
  }

  auto sorted_keys = top_k.GetSortedTopKeys();
  EXPECT_EQ(sorted_keys.size(), k);
}

// Test that our remove method works correctly
// NOLINTNEXTLINE
TEST_F(TopKElementsTests, RemoveTest) {
  const int k = 5;
  const int max_count = 100;
  TopKElements<int> top_k(k, 1000);

  // First add some keys with large counts
  for (int key = 1; key <= k; key++) {
    top_k.Increment(key, max_count * key);
  }
  // Then add some smaller keys
  for (int key = k; key <= k * 2; key++) {
    top_k.Increment(key, 1);
  }
  // We should still only have 'k' keys tracked
  EXPECT_EQ(top_k.GetSize(), k);

  // Now remove all of the large keys
  for (int key = 1; key <= k; key++) {
    top_k.Remove(key);
  }

  // The size should now be zero
  EXPECT_EQ(top_k.GetSize(), 0);

  // But if increment one of the smaller keys, then
  // the size should now be one
  top_k.Increment(k + 1, 1);
  EXPECT_EQ(top_k.GetSize(), 1);
}

// Just check to make sure that the output string is valid
// NOLINTNEXTLINE
TEST_F(TopKElementsTests, OutputTest) {
  const int k = 10;
  TopKElements<std::string> top_k(k, 1000);

  std::vector<std::string> keys = {
      "RZA", "GZA", "ODB", "Ghostface Killa", "Method Man", "Inspectah Deck", "Raekwon", "UGod", "Masta Killa",
  };
  EXPECT_LT(keys.size(), k);

  for (const auto &key : keys) {
    top_k.Increment(key, 1000);
  }

  std::ostringstream os;
  os << top_k;
  std::string output = os.str();
  EXPECT_FALSE(output.empty());

  // Make sure that all of our expected keys are in there
  for (const auto &key : keys) {
    std::size_t found = output.find(key);
    EXPECT_NE(found, std::string::npos);
  }
}

}  // namespace terrier::optimizer
