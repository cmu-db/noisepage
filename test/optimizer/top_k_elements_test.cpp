#include <string>

#include "optimizer/statistics/count_min_sketch.h"
#include "optimizer/statistics/top_k_elements.h"
#include "loggers/optimizer_logger.h"
#include "gtest/gtest.h"

#include "util/test_harness.h"

namespace terrier::optimizer {

class TopKElementsTests : public TerrierTest {};

// NOLINTNEXTLINE
TEST_F(TopKElementsTests, SimpleIncrementTest) {
  // test TopKElements
  const int k = 5;
  TopKElements<int> topK(k, 1000);
  EXPECT_EQ(topK.GetK(), k);
  EXPECT_EQ(topK.GetSize(), 0);

  topK.Increment(1, 10);
  topK.Increment(2, 5);
  topK.Increment(3, 1);
  topK.Increment(4, 1000000);

  // Since we set the top-k to track 5 keys,
  // all of these keys should return exact results
  // because we only have four keys in there now.
  EXPECT_EQ(topK.EstimateItemCount(1), 10);
  EXPECT_EQ(topK.EstimateItemCount(2), 5);
  EXPECT_EQ(topK.EstimateItemCount(3), 1);
  EXPECT_EQ(topK.EstimateItemCount(4), 1000000);

  // Make sure the size matches exactly the number
  // of keys we have thrown at it.
  EXPECT_EQ(topK.GetSize(), 4);

  // Add another value
  topK.Increment(5, 15);
  EXPECT_EQ(topK.GetSize(), 5);

  std::cout << topK;
}

// NOLINTNEXTLINE
TEST_F(TopKElementsTests, PromotionTest) {
  // Check that if incrementally increase the count of a
  // key that it will eventually get promoted to be in
  // the top-k list

  const int k = 10;
  TopKElements<int> topK(k, 1000);

  int num_keys = k * 2;
  int large_count = 1000;
  for (int key = 1; key <= num_keys; key++) {
    // If the key is below the half way point for
    // the number of keys we're inserted, then make
    // it's count super large
    if (key <= k) {
      topK.Increment(key, large_count);
    }
    // Otherwise just set it a small number
    else {
      topK.Increment(key, 99);
    }
  }

  // Now pick the largest key and keep incrementing
  // it until it is larger than 5x the large_count
  // At this point it should be in our top-k list
  auto target_key = num_keys;
  for (int i = 0; i < large_count*5; i++) {
    topK.Increment(target_key, 1);
  }
  auto sorted_keys = topK.GetSortedTopKeys();
  // FIXME
  bool found = false;
  for (auto key : sorted_keys) {
    if (key == target_key) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);

  // Now do the same thing but instead of incrementally updating
  // the target key's count, just hit it once with a single update.
  target_key = num_keys - 1;
  topK.Increment(target_key, large_count * 15);
  sorted_keys = topK.GetSortedTopKeys();
  // FIXME
  found = false;
  for (auto key : sorted_keys) {
    if (key == target_key) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);

}


// NOLINTNEXTLINE
//TEST_F(TopKElementsTests, SortedKeyTest) {
//  // test TopKElements
//  const int k = 10;
//  TopKElements<std::string> topK(k, 1000);
//  std::stack<std::string> expected_keys;
//
//  int num_keys = 500;
//  for (int i = 1; i <= num_keys; i++) {
//    auto key = std::to_string(i) + "!";
//    topK.Increment(key.data(), key.size(), i * 1000);
//
//    // If this key is within the last k entries that we are
//    // putting into the top-k tracker, then add it to our
//    // stack. This will the order of the keys that we
//    // expected to get back when we ask for them in sorted order.
//    if (i >= (num_keys-k)) expected_keys.push(key);
//
//    // If we have inserted less than k keys into the top-k
//    // tracker, then the number of keys added should be equal
//    // to the size of the top-k tracker.
//    if (i < k) {
//      EXPECT_EQ(topK.GetSize(), i);
//    } else {
//      EXPECT_EQ(topK.GetSize(), k);
//    }
//  }
//
//  // The top-k elements should be the last k numbers
//  // that we added into the object
//  auto sorted_keys = topK.GetSortedTopKeys();
//  EXPECT_EQ(sorted_keys.size(), k);
//  int i = 0;
//  for (auto key : sorted_keys) {
//    // Pop off the keys from our expected stack each time.
//    // It should match the current key in our sorted key list
//    auto expected_key = expected_keys.top();
//    expected_keys.pop();
//
//    OPTIMIZER_LOG_TRACE("Top-{0}: {1} <-> {2}", i, key, expected_key);
//    EXPECT_EQ(key, expected_key) << "Iteration #" << i;
//    i++;
//  }
//}

//// NOLINTNEXTLINE
//TEST_F(TopKElementsTests, SimpleArrivalAndDepartureTest) {
//  // test TopKElements
//  const int k = 5;
//  TopKElements<int> topK(k);
//
//  topK.Increment(10, 10);
//  topK.Increment(5, 5);
//  topK.Increment(1, 1);
//  topK.Increment(1000000, 1000000);
//
//  EXPECT_EQ(topK.EstimateItemCount(=10), 10);
//
//  topK.Increment(5, 15);
//  topK.Increment(6, 1);
//  topK.Increment(7, 2);
//  topK.Increment(8, 1);
//
//  EXPECT_EQ(topK.GetSize(), k);
//  topK.PrintTopKQueueOrderedMaxFirst(10);
//
//  topK.Decrement(5, 14);
//  topK.Decrement(10, 20);
//  topK.Decrement(100, 10000);
//  topK.PrintTopKQueueOrderedMaxFirst(10);
//}
//
//// NOLINTNEXTLINE
//TEST_F(TopKElementsTests, LargeArrivalOnlyTest) {
//  const int k = 20;
//  const int num0 = 10;
//  TopKElements<int> topK(k);
//
//  topK.Increment(10, 10);
//  topK.Increment(5, 5);
//  topK.Increment(1, 1);
//  topK.Increment(1000000, 1000000);
//
//  topK.Increment(7777, 2333);
//  topK.Increment(8888, 2334);
//  topK.Increment(9999, 2335);
//  for (int i = 0; i < 30; ++i) {
//    topK.Increment(i, i);
//  }
//
//  topK.PrintOrderedMaxFirst(num0);
//  EXPECT_EQ(topK.GetSize(), k);
//  EXPECT_EQ(topK.GetOrderedMaxFirst(num0).size(), num0);
//  EXPECT_EQ(topK.GetAllOrderedMaxFirst().size(), k);
//
//  for (int i = 1000; i < 2000; ++i) {
//    topK.Increment(i, i);
//  }
//  topK.PrintAllOrderedMaxFirst();
//}
//
////// NOLINTNEXTLINE
////TEST_F(TopKElementsTests, WrapperTest) {
////  CountMinSketch sketch(0.01, 0.1, 0);
////
////  const int k = 5;
////  TopKElements topK(sketch, k);
////
////  type::Value v1 = type::ValueFactory::GetDecimalValue(7.12);
////  type::Value v2 = type::ValueFactory::GetDecimalValue(10.25);
////  topK.Increment(v1);
////  topK.Increment(v2);
////  EXPECT_EQ(topK.GetAllOrderedMaxFirst().size(), 2);
////
////  for (int i = 0; i < 1000; i++) {
////    type::Value v = type::ValueFactory::GetDecimalValue(4.1525);
////    topK.Increment(v);
////  }
////  EXPECT_EQ(topK.GetAllOrderedMaxFirst().size(), 3);
////
////  type::Value v3 = type::ValueFactory::GetVarcharValue("luffy");
////  type::Value v4 = type::ValueFactory::GetVarcharValue(std::string("monkey"));
////  for (int i = 0; i < 500; i++) {
////    topK.Increment(v3, 1);
////    topK.Increment(v4, 1);
////  }
////  topK.PrintAllOrderedMaxFirst();
////}
//
//// NOLINTNEXTLINE
//TEST_F(TopKElementsTests, UniformTest) {
//  const int k = 5;
//  TopKElements<double> topK(k);
//
//  for (int i = 0; i < 1000; i++) {
//    auto v = 7.12 + i;
//    topK.Increment(v, 1);
//  }
//  EXPECT_EQ(topK.GetAllOrderedMaxFirst().size(), 5);
//
//  topK.PrintAllOrderedMaxFirst();
//}

}  // namespace terrier::optimizer
