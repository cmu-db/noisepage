#include <string>

#include "optimizer/statistics/count_min_sketch.h"
#include "optimizer/statistics/top_k_elements.h"
#include "loggers/optimizer_logger.h"
#include "gtest/gtest.h"

#include "util/test_harness.h"

namespace terrier::optimizer {

class TopKElementsTests : public TerrierTest {};

// NOLINTNEXTLINE
TEST_F(TopKElementsTests, SimpleArrivalOnlyTest) {
  // test TopKElements
  const int k = 5;
  TopKElements<int> topK(k);
  EXPECT_EQ(topK.GetK(), k);
  EXPECT_EQ(topK.GetSize(), 0);

  topK.Add(1, 10);
  topK.Add(2, 5);
  topK.Add(3, 1);
  topK.Add(4, 1000000);

  // This case count min sketch should give exact counting
  EXPECT_EQ(topK.EstimateItemCount(1), 10);
  EXPECT_EQ(topK.EstimateItemCount(2), 5);
  EXPECT_EQ(topK.EstimateItemCount(3), 1);
  EXPECT_EQ(topK.EstimateItemCount(4), 1000000);

  // EXPECT_EQ(topK.cmsketch.size, 4);
  EXPECT_EQ(topK.GetSize(), 4);

  topK.Add(5, 15);

  EXPECT_EQ(topK.GetSize(), 5);
  topK.PrintTopKQueueOrderedMaxFirst(10);
}

// NOLINTNEXTLINE
TEST_F(TopKElementsTests, SimpleArrivalAndDepartureTest) {
  CountMinSketch sketch(10, 5, 0);
  EXPECT_EQ(sketch.depth, 10);
  EXPECT_EQ(sketch.width, 5);
  EXPECT_EQ(sketch.size, 0);

  // test TopKElements
  const int k = 5;
  TopKElements topK(sketch, k);

  topK.Add("10", 10);
  topK.Add("5", 5);
  topK.Add("1", 1);
  topK.Add("Million", 1000000);

  EXPECT_EQ(topK.EstimateItemCount("10"), 10);

  topK.Add(5, 15);
  topK.Add("6", 1);
  topK.Add("7", 2);
  topK.Add("8", 1);

  EXPECT_EQ(topK.GetSize(), k);
  topK.PrintTopKQueueOrderedMaxFirst(10);

  topK.Remove(5, 14);
  topK.Remove("10", 20);
  topK.Remove(100, 10000);
  topK.PrintTopKQueueOrderedMaxFirst(10);
}

// NOLINTNEXTLINE
TEST_F(TopKElementsTests, LargeArrivalOnlyTest) {
  CountMinSketch sketch(1000, 1000, 0);

  const int k = 20, num0 = 10;
  TopKElements topK(sketch, k);

  topK.Add("10", 10);
  topK.Add("5", 5);
  topK.Add("1", 1);
  topK.Add("Million", 1000000);

  topK.Add(std::string{"Cowboy"}, 2333);
  topK.Add(std::string{"Bebop"}, 2334);
  topK.Add(std::string{"RayCharles"}, 2335);
  int i;
  for (i = 0; i < 30; ++i) {
    topK.Add(i, i);
  }

  topK.PrintOrderedMaxFirst(num0);
  EXPECT_EQ(topK.GetSize(), k);
  EXPECT_EQ(topK.GetOrderedMaxFirst(num0).size(), num0);
  EXPECT_EQ(topK.GetAllOrderedMaxFirst().size(), k);

  for (i = 1000; i < 2000; ++i) {
    topK.Add(i, i);
  }
  topK.PrintAllOrderedMaxFirst();
}

// NOLINTNEXTLINE
TEST_F(TopKElementsTests, WrapperTest) {
  CountMinSketch sketch(0.01, 0.1, 0);

  const int k = 5;
  TopKElements topK(sketch, k);

  type::Value v1 = type::ValueFactory::GetDecimalValue(7.12);
  type::Value v2 = type::ValueFactory::GetDecimalValue(10.25);
  topK.Add(v1);
  topK.Add(v2);
  EXPECT_EQ(topK.GetAllOrderedMaxFirst().size(), 2);

  for (int i = 0; i < 1000; i++) {
    type::Value v = type::ValueFactory::GetDecimalValue(4.1525);
    topK.Add(v);
  }
  EXPECT_EQ(topK.GetAllOrderedMaxFirst().size(), 3);

  type::Value v3 = type::ValueFactory::GetVarcharValue("luffy");
  type::Value v4 = type::ValueFactory::GetVarcharValue(std::string("monkey"));
  for (int i = 0; i < 500; i++) {
    topK.Add(v3);
    topK.Add(v4);
  }
  topK.PrintAllOrderedMaxFirst();
}

// NOLINTNEXTLINE
TEST_F(TopKElementsTests, UniformTest) {
  CountMinSketch sketch(0.01, 0.1, 0);

  const int k = 5;
  TopKElements topK(sketch, k);

  for (int i = 0; i < 1000; i++) {
    type::Value v1 = type::ValueFactory::GetDecimalValue(7.12 + i);
    topK.Add(v1);
  }
  EXPECT_EQ(topK.GetAllOrderedMaxFirst().size(), 5);

  topK.PrintAllOrderedMaxFirst();
}

}  // namespace terrier::optimizer
