#include "execution/sql/aggregators.h"
#include "execution/sql/value.h"
#include "execution/sql_test.h"
#include "optimizer/statistics/top_k_elements.h"
#include "storage/storage_defs.h"

namespace noisepage::execution::sql::test {

class AggregatorsTest : public SqlBasedTest {
 public:
  AggregatorsTest() = default;

  void SetUp() override {
    SqlBasedTest::SetUp();
    exec_ctx_ = MakeExecCtx();
  }

 protected:
  /**
   * Execution context to use for the test
   */
  std::unique_ptr<exec::ExecutionContext> exec_ctx_;
};

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, Count) {
  //
  // Count on empty input
  //

  {
    CountAggregate count;
    EXPECT_EQ(0, count.GetCountResult().val_);
  }

  //
  // Count on mixed NULL and non-NULL input
  //

  {
    // Even inputs are NULL
    CountAggregate count;
    for (uint32_t i = 0; i < 10; i++) {
      Integer val = (i % 2 == 0 ? Integer::Null() : Integer(i));
      count.Advance(val);
    }
    EXPECT_EQ(5, count.GetCountResult().val_);
  }
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, CountMerge) {
  // Even inputs are NULL
  CountAggregate count_1, count_2;

  // Insert into count_1
  for (uint32_t i = 0; i < 100; i++) {
    Integer val = (i % 2 == 0 ? Integer::Null() : Integer(i));
    count_1.Advance(val);
  }
  for (uint32_t i = 0; i < 100; i++) {
    Integer val(i);
    count_2.Advance(val);
  }

  auto merged = count_1.GetCountResult().val_ + count_2.GetCountResult().val_;

  count_1.Merge(count_2);

  EXPECT_EQ(merged, count_1.GetCountResult().val_);
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, SumInteger) {
  //
  // SUM on empty input is null
  //

  {
    IntegerSumAggregate sum;
    EXPECT_TRUE(sum.GetResultSum().is_null_);
  }

  //
  // Sum on mixed NULL and non-NULL input
  //

  {
    // [1, 3, 5, 7, 9]
    IntegerSumAggregate sum;
    for (uint32_t i = 0; i < 10; i++) {
      Integer val = (i % 2 == 0 ? Integer::Null() : Integer(i));
      sum.Advance(val);
    }

    EXPECT_FALSE(sum.GetResultSum().is_null_);
    EXPECT_EQ(25, sum.GetResultSum().val_);
  }

  //
  // Sum on non-NULL input
  //

  {
    // [0..9]
    IntegerSumAggregate sum;
    for (uint32_t i = 0; i < 10; i++) {
      Integer val(i);
      sum.Advance(val);
    }

    EXPECT_FALSE(sum.GetResultSum().is_null_);
    EXPECT_EQ(45, sum.GetResultSum().val_);
  }
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, MergeSumIntegers) {
  IntegerSumAggregate sum1;
  EXPECT_TRUE(sum1.GetResultSum().is_null_);
  for (uint64_t i = 0; i < 10; i++) {
    auto val = Integer(i);
    sum1.Advance(val);
  }

  IntegerSumAggregate sum2;
  EXPECT_TRUE(sum2.GetResultSum().is_null_);
  for (uint64_t i = 10; i < 20; i++) {
    auto val = Integer(i);
    sum2.Advance(val);
  }
  sum1.Merge(sum2);
  EXPECT_FALSE(sum1.GetResultSum().is_null_);
  EXPECT_EQ(190, sum1.GetResultSum().val_);

  IntegerSumAggregate sum3;
  EXPECT_TRUE(sum3.GetResultSum().is_null_);
  sum1.Merge(sum3);
  EXPECT_EQ(190, sum1.GetResultSum().val_);

  for (int64_t i = 0; i < 20; i++) {
    auto val = Integer(-i);
    sum3.Advance(val);
  }
  sum1.Merge(sum3);
  EXPECT_EQ(0, sum1.GetResultSum().val_);
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, SumReal) {
  // SUM on empty input is null
  {
    RealSumAggregate sum;
    EXPECT_TRUE(sum.GetResultSum().is_null_);
  }

  RealSumAggregate sum, sum2;

  // Push in some sums (with NULLs mixed in)
  {
    // [1, 3, 5, 7, 9]
    EXPECT_TRUE(sum.GetResultSum().is_null_);
    for (uint32_t i = 0; i < 10; i++) {
      Real val = (i % 2 == 0 ? Real::Null() : Real(static_cast<double>(i)));
      sum.Advance(val);
    }

    EXPECT_FALSE(sum.GetResultSum().is_null_);
    EXPECT_DOUBLE_EQ(25.0, sum.GetResultSum().val_);
  }

  // Push non-NULL values into sum2
  {
    // [1, 2, 3, 4, 5, 6, 7, 8, 9]
    EXPECT_TRUE(sum2.GetResultSum().is_null_);
    for (uint32_t i = 0; i < 10; i++) {
      Real val(static_cast<double>(i));
      sum2.Advance(val);
    }

    EXPECT_FALSE(sum2.GetResultSum().is_null_);
    EXPECT_DOUBLE_EQ(45.0, sum2.GetResultSum().val_);
  }

  // Try to merge both sums. Result should be total sum.
  sum.Merge(sum2);
  EXPECT_FALSE(sum.GetResultSum().is_null_);
  EXPECT_DOUBLE_EQ(70.0, sum.GetResultSum().val_);

  // Try to merge in NULL sum. Original sum should be unchanged.
  RealSumAggregate null_sum;
  sum.Merge(null_sum);
  EXPECT_FALSE(sum.GetResultSum().is_null_);
  EXPECT_DOUBLE_EQ(70.0, sum.GetResultSum().val_);
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, MaxInteger) {
  // NULL check and merging NULL check
  {
    IntegerMaxAggregate max;
    EXPECT_TRUE(max.GetResultMax().is_null_);

    IntegerMaxAggregate null_max;
    EXPECT_TRUE(null_max.GetResultMax().is_null_);

    max.Merge(null_max);
    EXPECT_TRUE(max.GetResultMax().is_null_);
  }

  IntegerMaxAggregate max1, max2;

  // Proper max calculation
  {
    for (int64_t i = 0; i < 25; i++) {
      int64_t j = i;

      // mix in some low numbers
      if (i % 2 == 0) {
        j = -i;
      }

      auto val = Integer(j);
      max1.Advance(val);
    }

    EXPECT_FALSE(max1.GetResultMax().is_null_);
    EXPECT_EQ(23, max1.GetResultMax().val_);
  }

  // Ditto for the second max
  {
    EXPECT_TRUE(max2.GetResultMax().is_null_);
    for (int64_t i = 23; i < 45; i++) {
      int64_t j = i;

      // mix in some low numbers
      if (i % 2 == 0) {
        j = -i;
      }
      auto val = Integer(j);
      max2.Advance(val);
    }

    EXPECT_FALSE(max2.GetResultMax().is_null_);
    EXPECT_EQ(43, max2.GetResultMax().val_);
  }

  // Try to merge the two maxs. Result should capture global (non-NULL) max.
  max1.Merge(max2);
  EXPECT_FALSE(max1.GetResultMax().is_null_);
  EXPECT_EQ(43, max1.GetResultMax().val_);

  IntegerMaxAggregate null_max;
  max1.Merge(null_max);
  EXPECT_FALSE(max1.GetResultMax().is_null_);
  EXPECT_EQ(43, max1.GetResultMax().val_);
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, MinInteger) {
  // NULL check and merging NULL check
  {
    IntegerMinAggregate min;
    EXPECT_TRUE(min.GetResultMin().is_null_);

    IntegerMinAggregate null_min;
    EXPECT_TRUE(null_min.GetResultMin().is_null_);

    min.Merge(null_min);
    EXPECT_TRUE(min.GetResultMin().is_null_);
  }

  IntegerMinAggregate min, min2;

  // Proper min calculation using min1
  {
    for (int64_t i = 0; i < 25; i++) {
      int64_t j = i;

      // mix in some low numbers
      if (i % 2 == 0) {
        j = -i;
      }

      auto val = Integer(j);
      min.Advance(val);
    }

    EXPECT_FALSE(min.GetResultMin().is_null_);
    EXPECT_EQ(-24, min.GetResultMin().val_);
  }

  // Proper min calculation, separately, using min2
  {
    EXPECT_TRUE(min2.GetResultMin().is_null_);
    for (int64_t i = 23; i < 45; i++) {
      int64_t j = i;

      // mix in some low numbers
      if (i % 2 == 0) {
        j = -i;
      }
      auto val = Integer(j);
      min2.Advance(val);
    }

    EXPECT_FALSE(min2.GetResultMin().is_null_);
    EXPECT_EQ(-44, min2.GetResultMin().val_);
  }

  // Try to merge the two mins. Result should capture global (non-NULL) min.
  min.Merge(min2);
  EXPECT_FALSE(min.GetResultMin().is_null_);
  EXPECT_EQ(-44, min.GetResultMin().val_);

  IntegerMinAggregate null_min;
  min.Merge(null_min);
  EXPECT_FALSE(min.GetResultMin().is_null_);
  EXPECT_EQ(-44, min.GetResultMin().val_);
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, MaxReal) {
  // NULL check and merging NULL check
  {
    RealMaxAggregate max;
    EXPECT_TRUE(max.GetResultMax().is_null_);

    RealMaxAggregate null_max;
    EXPECT_TRUE(null_max.GetResultMax().is_null_);

    max.Merge(null_max);
    EXPECT_TRUE(max.GetResultMax().is_null_);
  }

  RealMaxAggregate max1, max2;

  // Proper max calculation
  {
    for (int64_t i = 0; i < 25; i++) {
      double j = i;

      // mix in some low numbers
      if (i % 2 == 0) {
        j = -i;
      }

      auto val = Real(j);
      max1.Advance(val);
    }

    EXPECT_FALSE(max1.GetResultMax().is_null_);
    EXPECT_DOUBLE_EQ(23.0, max1.GetResultMax().val_);
  }

  // Ditto for the second max
  {
    EXPECT_TRUE(max2.GetResultMax().is_null_);
    for (int64_t i = 23; i < 45; i++) {
      double j = i;

      // mix in some low numbers
      if (i % 2 == 0) {
        j = -i;
      }
      auto val = Real(j);
      max2.Advance(val);
    }

    EXPECT_FALSE(max2.GetResultMax().is_null_);
    EXPECT_DOUBLE_EQ(43.0, max2.GetResultMax().val_);
  }

  // Try to merge the two maxs. Result should capture global (non-NULL) max.
  max1.Merge(max2);
  EXPECT_FALSE(max1.GetResultMax().is_null_);
  EXPECT_DOUBLE_EQ(43.0, max1.GetResultMax().val_);

  RealMaxAggregate null_max;
  max1.Merge(null_max);
  EXPECT_FALSE(max1.GetResultMax().is_null_);
  EXPECT_DOUBLE_EQ(43.0, max1.GetResultMax().val_);
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, MinReal) {
  // NULL check and merging NULL check
  {
    RealMinAggregate min;
    EXPECT_TRUE(min.GetResultMin().is_null_);

    RealMinAggregate null_min;
    EXPECT_TRUE(null_min.GetResultMin().is_null_);

    min.Merge(null_min);
    EXPECT_TRUE(min.GetResultMin().is_null_);
  }

  RealMinAggregate min, min2;

  // Proper min calculation using min1
  {
    for (int64_t i = 0; i < 25; i++) {
      double j = i;

      // mix in some low numbers
      if (i % 2 == 0) {
        j = -i;
      }

      auto val = Real(j);
      min.Advance(val);
    }

    EXPECT_FALSE(min.GetResultMin().is_null_);
    EXPECT_DOUBLE_EQ(-24.0, min.GetResultMin().val_);
  }

  // Proper min calculation, separately, using min2
  {
    EXPECT_TRUE(min2.GetResultMin().is_null_);
    for (int64_t i = 23; i < 45; i++) {
      double j = i;

      // mix in some low numbers
      if (i % 2 == 0) {
        j = -i;
      }
      auto val = Real(j);
      min2.Advance(val);
    }

    EXPECT_FALSE(min2.GetResultMin().is_null_);
    EXPECT_DOUBLE_EQ(-44.0, min2.GetResultMin().val_);
  }

  // Try to merge the two mins. Result should capture global (non-NULL) min.
  min.Merge(min2);
  EXPECT_FALSE(min.GetResultMin().is_null_);
  EXPECT_DOUBLE_EQ(-44.0, min.GetResultMin().val_);

  RealMinAggregate null_min;
  min.Merge(null_min);
  EXPECT_FALSE(min.GetResultMin().is_null_);
  EXPECT_DOUBLE_EQ(-44.0, min.GetResultMin().val_);
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, Avg) {
  // NULL check and merging NULL check
  {
    AvgAggregate avg;
    EXPECT_TRUE(avg.GetResultAvg().is_null_);

    AvgAggregate null_avg;
    EXPECT_TRUE(null_avg.GetResultAvg().is_null_);

    avg.Merge(null_avg);
    EXPECT_TRUE(avg.GetResultAvg().is_null_);
  }

  AvgAggregate avg1, avg2;

  // Create first average aggregate
  {
    EXPECT_TRUE(avg1.GetResultAvg().is_null_);
    double sum = 0.0, count = 0.0;
    for (uint64_t i = 0; i < 25; i++) {
      sum += i;
      count++;
      auto val = Integer(i);
      avg1.Advance(val);
    }

    EXPECT_FALSE(avg1.GetResultAvg().is_null_);
    EXPECT_DOUBLE_EQ((sum / count), avg1.GetResultAvg().val_);
  }

  // Create second average aggregate
  {
    EXPECT_TRUE(avg2.GetResultAvg().is_null_);
    double sum = 0.0, count = 0.0;
    for (int64_t i = 0; i < 25; i++) {
      sum += -i;
      count++;
      auto val = Integer(-i);
      avg2.Advance(val);
    }

    EXPECT_FALSE(avg2.GetResultAvg().is_null_);
    EXPECT_DOUBLE_EQ((sum / count), avg2.GetResultAvg().val_);
  }

  // Merge the two averages. Result should capture global (non-NULL) average.
  avg1.Merge(avg2);
  EXPECT_FALSE(avg1.GetResultAvg().is_null_);
  EXPECT_DOUBLE_EQ(0.0, avg1.GetResultAvg().val_);

  // Try to merge in a NULL average. Result should be unchanged.
  AvgAggregate null_avg;
  avg1.Merge(null_avg);
  EXPECT_FALSE(avg1.GetResultAvg().is_null_);
  EXPECT_DOUBLE_EQ(0.0, avg1.GetResultAvg().val_);
}

// ---------------------------------------------------------
// TOP K Test
// ---------------------------------------------------------

/*
 * When testing TOP K it's better to not exceed k entries because then the results become unpredictable
 */

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, TopKBool) {
  // NULL check and merging NULL check
  {
    BooleanTopKAggregate top_k;
    EXPECT_TRUE(top_k.GetResult(exec_ctx_.get()).is_null_);

    BooleanTopKAggregate null_top_k;
    EXPECT_TRUE(null_top_k.GetResult(exec_ctx_.get()).is_null_);

    top_k.Merge(null_top_k);
    EXPECT_TRUE(top_k.GetResult(exec_ctx_.get()).is_null_);
  }

  BooleanTopKAggregate top_k, top_k2;

  // Proper top_k calculation using top_k
  {
    for (int64_t i = 0; i < 25; i++) {
      auto val = BoolVal(i % 2 == 0);
      top_k.Advance(val);
    }

    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<bool>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 2);
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(true), 13);
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(false), 12);
  }

  // Proper top_k calculation, separately, using top_k2
  {
    EXPECT_TRUE(top_k2.GetResult(exec_ctx_.get()).is_null_);
    for (int64_t i = 23; i < 45; i++) {
      auto val = BoolVal(i % 3 == 0);
      top_k2.Advance(val);
    }

    EXPECT_FALSE(top_k2.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k2.GetResult(exec_ctx_.get());
    auto deserialized_top_k2 = optimizer::TopKElements<bool>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k2.GetSize(), 2);
    EXPECT_EQ(deserialized_top_k2.EstimateItemCount(true), 7);
    EXPECT_EQ(deserialized_top_k2.EstimateItemCount(false), 15);
  }

  // Try to merge the two topk. Result should capture global (non-NULL) top_k.
  {
    top_k.Merge(top_k2);
    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<bool>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 2);
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(true), 13 + 7);
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(false), 12 + 15);
  }

  // Try to merge in a NULL topk. Result should be unchanged.
  {
    BooleanTopKAggregate null_top_k;
    top_k.Merge(null_top_k);
    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<bool>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 2);
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(true), 13 + 7);
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(false), 12 + 15);
  }
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, TopKInt) {
  // NULL check and merging NULL check
  {
    IntegerTopKAggregate top_k;
    EXPECT_TRUE(top_k.GetResult(exec_ctx_.get()).is_null_);

    IntegerTopKAggregate null_top_k;
    EXPECT_TRUE(null_top_k.GetResult(exec_ctx_.get()).is_null_);

    top_k.Merge(null_top_k);
    EXPECT_TRUE(top_k.GetResult(exec_ctx_.get()).is_null_);
  }

  IntegerTopKAggregate top_k, top_k2;

  // Proper top_k calculation using top_k
  {
    for (int64_t i = 0; i < 15; i++) {
      auto val = Integer(i / 2);
      top_k.Advance(val);
    }

    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<decltype(Integer::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 8);
    for (int i = 0; i < 7; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(i), 2);
    }
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(7), 1);
    for (int i = 8; i < 15; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(i), 0);
    }
  }

  // Proper top_k calculation, separately, using top_k2
  {
    EXPECT_TRUE(top_k2.GetResult(exec_ctx_.get()).is_null_);
    for (int64_t i = 15; i < 31; i++) {
      auto val = Integer(i / 2);
      top_k2.Advance(val);
    }

    EXPECT_FALSE(top_k2.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k2.GetResult(exec_ctx_.get());
    auto deserialized_top_k2 = optimizer::TopKElements<decltype(Integer::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k2.GetSize(), 9);
    for (int i = 0; i < 7; i++) {
      EXPECT_EQ(deserialized_top_k2.EstimateItemCount(i), 0);
    }
    EXPECT_EQ(deserialized_top_k2.EstimateItemCount(7), 1);
    for (int i = 8; i < 15; i++) {
      EXPECT_EQ(deserialized_top_k2.EstimateItemCount(i), 2);
    }
    EXPECT_EQ(deserialized_top_k2.EstimateItemCount(15), 1);
    for (int i = 16; i < 31; i++) {
      EXPECT_EQ(deserialized_top_k2.EstimateItemCount(i), 0);
    }
  }

  // Try to merge the two topk. Result should capture global (non-NULL) top_k.
  {
    top_k.Merge(top_k2);
    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<decltype(Integer::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 16);
    for (int i = 0; i < 15; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(i), 2);
    }
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(15), 1);
    for (int i = 16; i < 31; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(i), 0);
    }
  }

  // Try to merge in a NULL topk. Result should be unchanged.
  {
    IntegerTopKAggregate null_top_k;
    top_k.Merge(null_top_k);
    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<decltype(Integer::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 16);
    for (int i = 0; i < 15; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(i), 2);
    }
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(15), 1);
    for (int i = 16; i < 31; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(i), 0);
    }
  }
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, TopKReal) {
  // NULL check and merging NULL check
  {
    RealTopKAggregate top_k;
    EXPECT_TRUE(top_k.GetResult(exec_ctx_.get()).is_null_);

    RealTopKAggregate null_top_k;
    EXPECT_TRUE(null_top_k.GetResult(exec_ctx_.get()).is_null_);

    top_k.Merge(null_top_k);
    EXPECT_TRUE(top_k.GetResult(exec_ctx_.get()).is_null_);
  }

  RealTopKAggregate top_k, top_k2;

  // Proper top_k calculation using top_k
  {
    for (size_t i = 0; i < 8; i++) {
      auto val = Real(static_cast<double>(i) / 2);
      top_k.Advance(val);
    }

    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<decltype(Real::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 8);
    for (size_t i = 0; i < 8; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(static_cast<double>(i) / 2), 1);
    }
  }

  // Proper top_k calculation, separately, using top_k2
  {
    EXPECT_TRUE(top_k2.GetResult(exec_ctx_.get()).is_null_);
    for (size_t i = 8; i < 16; i++) {
      auto val = Real(static_cast<double>(i) / 2);
      top_k2.Advance(val);
    }

    EXPECT_FALSE(top_k2.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k2.GetResult(exec_ctx_.get());
    auto deserialized_top_k2 = optimizer::TopKElements<decltype(Real::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k2.GetSize(), 8);
    for (size_t i = 8; i < 16; i++) {
      EXPECT_EQ(deserialized_top_k2.EstimateItemCount(static_cast<double>(i) / 2), 1);
    }
  }

  // Try to merge the two topk. Result should capture global (non-NULL) top_k.
  {
    top_k.Merge(top_k2);
    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<decltype(Real::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 16);
    for (size_t i = 0; i < 15; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(static_cast<double>(i) / 2), 1);
    }
  }

  // Try to merge in a NULL topk. Result should be unchanged.
  {
    RealTopKAggregate null_top_k;
    top_k.Merge(null_top_k);
    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<decltype(Real::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 16);
    for (size_t i = 0; i < 15; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(static_cast<double>(i) / 2), 1);
    }
  }
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, TopKDecimal) {
  // NULL check and merging NULL check
  {
    DecimalTopKAggregate top_k;
    EXPECT_TRUE(top_k.GetResult(exec_ctx_.get()).is_null_);

    DecimalTopKAggregate null_top_k;
    EXPECT_TRUE(null_top_k.GetResult(exec_ctx_.get()).is_null_);

    top_k.Merge(null_top_k);
    EXPECT_TRUE(top_k.GetResult(exec_ctx_.get()).is_null_);
  }

  DecimalTopKAggregate top_k, top_k2;

  // Proper top_k calculation using top_k
  {
    for (Decimal64 i(0); i < 15; i += 1) {
      auto val = DecimalVal(i / 2);
      top_k.Advance(val);
    }

    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<decltype(DecimalVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 8);
    for (Decimal64 i(0); i < 7; i += 1) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(Decimal64(i)), 2);
    }
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(Decimal64(7)), 1);
  }

  // Proper top_k calculation, separately, using top_k2
  {
    EXPECT_TRUE(top_k2.GetResult(exec_ctx_.get()).is_null_);
    for (Decimal64 i(15); i < 31; i += 1) {
      auto val = DecimalVal(i / 2);
      top_k2.Advance(val);
    }

    EXPECT_FALSE(top_k2.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k2.GetResult(exec_ctx_.get());
    auto deserialized_top_k2 = optimizer::TopKElements<decltype(DecimalVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k2.GetSize(), 9);
    EXPECT_EQ(deserialized_top_k2.EstimateItemCount(Decimal64(7)), 1);
    for (Decimal64 i(8); i < 15; i += 1) {
      EXPECT_EQ(deserialized_top_k2.EstimateItemCount(Decimal64(i)), 2);
    }
    EXPECT_EQ(deserialized_top_k2.EstimateItemCount(Decimal64(15)), 1);
  }

  // Try to merge the two topk. Result should capture global (non-NULL) top_k.
  {
    top_k.Merge(top_k2);
    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<decltype(DecimalVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 16);
    for (Decimal64 i(0); i < 15; i += 1) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(Decimal64(i / 2)), 2);
    }
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(Decimal64(15)), 1);
  }

  // Try to merge in a NULL topk. Result should be unchanged.
  {
    DecimalTopKAggregate null_top_k;
    top_k.Merge(null_top_k);
    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<decltype(DecimalVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 16);
    for (Decimal64 i(0); i < 15; i += 1) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(Decimal64(i / 2)), 2);
    }
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(Decimal64(15)), 1);
  }
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, TopKString) {
  // NULL check and merging NULL check
  {
    StringTopKAggregate top_k;
    EXPECT_TRUE(top_k.GetResult(exec_ctx_.get()).is_null_);

    StringTopKAggregate null_top_k;
    EXPECT_TRUE(null_top_k.GetResult(exec_ctx_.get()).is_null_);

    top_k.Merge(null_top_k);
    EXPECT_TRUE(top_k.GetResult(exec_ctx_.get()).is_null_);
  }

  StringTopKAggregate top_k, top_k2;

  // Proper top_k calculation using top_k
  {
    auto val = StringVal(storage::VarlenEntry::Create("Foo"));
    top_k.Advance(val);
    top_k.Advance(val);
    val = StringVal(storage::VarlenEntry::Create("Bar"));
    top_k.Advance(val);

    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<decltype(StringVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 2);
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(storage::VarlenEntry::Create("Foo")), 2);
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(storage::VarlenEntry::Create("Bar")), 1);
  }

  // Proper top_k calculation, separately, using top_k2
  {
    EXPECT_TRUE(top_k2.GetResult(exec_ctx_.get()).is_null_);
    auto val = StringVal(storage::VarlenEntry::Create("Ice Climbers"));
    top_k2.Advance(val);
    val = StringVal(storage::VarlenEntry::Create("Marth"));
    top_k2.Advance(val);

    EXPECT_FALSE(top_k2.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k2.GetResult(exec_ctx_.get());
    auto deserialized_top_k2 = optimizer::TopKElements<decltype(StringVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k2.GetSize(), 2);
    EXPECT_EQ(deserialized_top_k2.EstimateItemCount(storage::VarlenEntry::Create("Ice Climbers")), 1);
    EXPECT_EQ(deserialized_top_k2.EstimateItemCount(storage::VarlenEntry::Create("Marth")), 1);
  }

  // Try to merge the two topk. Result should capture global (non-NULL) top_k.
  {
    top_k.Merge(top_k2);
    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<decltype(StringVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 4);
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(storage::VarlenEntry::Create("Foo")), 2);
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(storage::VarlenEntry::Create("Bar")), 1);
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(storage::VarlenEntry::Create("Ice Climbers")), 1);
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(storage::VarlenEntry::Create("Marth")), 1);
  }

  // Try to merge in a NULL topk. Result should be unchanged.
  {
    StringTopKAggregate null_top_k;
    top_k.Merge(null_top_k);
    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<decltype(StringVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 4);
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(storage::VarlenEntry::Create("Foo")), 2);
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(storage::VarlenEntry::Create("Bar")), 1);
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(storage::VarlenEntry::Create("Ice Climbers")), 1);
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(storage::VarlenEntry::Create("Marth")), 1);
  }
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, TopKDate) {
  // NULL check and merging NULL check
  {
    DateTopKAggregate top_k;
    EXPECT_TRUE(top_k.GetResult(exec_ctx_.get()).is_null_);

    DateTopKAggregate null_top_k;
    EXPECT_TRUE(null_top_k.GetResult(exec_ctx_.get()).is_null_);

    top_k.Merge(null_top_k);
    EXPECT_TRUE(top_k.GetResult(exec_ctx_.get()).is_null_);
  }

  DateTopKAggregate top_k, top_k2;

  // Proper top_k calculation using top_k
  {
    for (int64_t i = 0; i < 15; i++) {
      auto val = DateVal(i / 2);
      top_k.Advance(val);
    }

    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<decltype(DateVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 8);
    for (int i = 0; i < 7; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(Date::FromNative(i)), 2);
    }
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(Date::FromNative(7)), 1);
    for (int i = 8; i < 15; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(Date::FromNative(i)), 0);
    }
  }

  // Proper top_k calculation, separately, using top_k2
  {
    EXPECT_TRUE(top_k2.GetResult(exec_ctx_.get()).is_null_);
    for (int64_t i = 15; i < 31; i++) {
      auto val = DateVal(i / 2);
      top_k2.Advance(val);
    }

    EXPECT_FALSE(top_k2.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k2.GetResult(exec_ctx_.get());
    auto deserialized_top_k2 = optimizer::TopKElements<decltype(DateVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k2.GetSize(), 9);
    for (int i = 0; i < 7; i++) {
      EXPECT_EQ(deserialized_top_k2.EstimateItemCount(Date::FromNative(i)), 0);
    }
    EXPECT_EQ(deserialized_top_k2.EstimateItemCount(Date::FromNative(7)), 1);
    for (int i = 8; i < 15; i++) {
      EXPECT_EQ(deserialized_top_k2.EstimateItemCount(Date::FromNative(i)), 2);
    }
    EXPECT_EQ(deserialized_top_k2.EstimateItemCount(Date::FromNative(15)), 1);
    for (int i = 16; i < 31; i++) {
      EXPECT_EQ(deserialized_top_k2.EstimateItemCount(Date::FromNative(i)), 0);
    }
  }

  // Try to merge the two topk. Result should capture global (non-NULL) top_k.
  {
    top_k.Merge(top_k2);
    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<decltype(DateVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 16);
    for (int i = 0; i < 15; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(Date::FromNative(i)), 2);
    }
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(Date::FromNative(15)), 1);
    for (int i = 16; i < 31; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(Date::FromNative(i)), 0);
    }
  }

  // Try to merge in a NULL topk. Result should be unchanged.
  {
    DateTopKAggregate null_top_k;
    top_k.Merge(null_top_k);
    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<decltype(DateVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 16);
    for (int i = 0; i < 15; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(Date::FromNative(i)), 2);
    }
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(Date::FromNative(15)), 1);
    for (int i = 16; i < 31; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(Date::FromNative(i)), 0);
    }
  }
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, TopKTimestamp) {
  // NULL check and merging NULL check
  {
    TimestampTopKAggregate top_k;
    EXPECT_TRUE(top_k.GetResult(exec_ctx_.get()).is_null_);

    TimestampTopKAggregate null_top_k;
    EXPECT_TRUE(null_top_k.GetResult(exec_ctx_.get()).is_null_);

    top_k.Merge(null_top_k);
    EXPECT_TRUE(top_k.GetResult(exec_ctx_.get()).is_null_);
  }

  TimestampTopKAggregate top_k, top_k2;

  // Proper top_k calculation using top_k
  {
    for (int64_t i = 0; i < 15; i++) {
      auto val = TimestampVal(i / 2);
      top_k.Advance(val);
    }

    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<decltype(TimestampVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 8);
    for (int i = 0; i < 7; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(Timestamp::FromNative(i)), 2);
    }
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(Timestamp::FromNative(7)), 1);
    for (int i = 8; i < 15; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(Timestamp::FromNative(i)), 0);
    }
  }

  // Proper top_k calculation, separately, using top_k2
  {
    EXPECT_TRUE(top_k2.GetResult(exec_ctx_.get()).is_null_);
    for (int64_t i = 15; i < 31; i++) {
      auto val = TimestampVal(i / 2);
      top_k2.Advance(val);
    }

    EXPECT_FALSE(top_k2.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k2.GetResult(exec_ctx_.get());
    auto deserialized_top_k2 = optimizer::TopKElements<decltype(TimestampVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k2.GetSize(), 9);
    for (int i = 0; i < 7; i++) {
      EXPECT_EQ(deserialized_top_k2.EstimateItemCount(Timestamp::FromNative(i)), 0);
    }
    EXPECT_EQ(deserialized_top_k2.EstimateItemCount(Timestamp::FromNative(7)), 1);
    for (int i = 8; i < 15; i++) {
      EXPECT_EQ(deserialized_top_k2.EstimateItemCount(Timestamp::FromNative(i)), 2);
    }
    EXPECT_EQ(deserialized_top_k2.EstimateItemCount(Timestamp::FromNative(15)), 1);
    for (int i = 16; i < 31; i++) {
      EXPECT_EQ(deserialized_top_k2.EstimateItemCount(Timestamp::FromNative(i)), 0);
    }
  }

  // Try to merge the two topk. Result should capture global (non-NULL) top_k.
  {
    top_k.Merge(top_k2);
    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<decltype(TimestampVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 16);
    for (int i = 0; i < 15; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(Timestamp::FromNative(i)), 2);
    }
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(Timestamp::FromNative(15)), 1);
    for (int i = 16; i < 31; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(Timestamp::FromNative(i)), 0);
    }
  }

  // Try to merge in a NULL topk. Result should be unchanged.
  {
    TimestampTopKAggregate null_top_k;
    top_k.Merge(null_top_k);
    EXPECT_FALSE(top_k.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = top_k.GetResult(exec_ctx_.get());
    auto deserialized_top_k = optimizer::TopKElements<decltype(TimestampVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_top_k.GetSize(), 16);
    for (int i = 0; i < 15; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(Timestamp::FromNative(i)), 2);
    }
    EXPECT_EQ(deserialized_top_k.EstimateItemCount(Timestamp::FromNative(15)), 1);
    for (int i = 16; i < 31; i++) {
      EXPECT_EQ(deserialized_top_k.EstimateItemCount(Timestamp::FromNative(i)), 0);
    }
  }
}

// ---------------------------------------------------------
// HISTOGRAM Test
// ---------------------------------------------------------

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, HistogramBool) {
  // NULL check and merging NULL check
  {
    BooleanHistogramAggregate histogram;
    EXPECT_TRUE(histogram.GetResult(exec_ctx_.get()).is_null_);

    BooleanHistogramAggregate null_histogram;
    EXPECT_TRUE(null_histogram.GetResult(exec_ctx_.get()).is_null_);

    histogram.Merge(null_histogram);
    EXPECT_TRUE(histogram.GetResult(exec_ctx_.get()).is_null_);
  }

  BooleanHistogramAggregate histogram, histogram2;

  // Proper histogram calculation using histogram
  {
    for (int64_t i = 0; i < 25; i++) {
      auto val = BoolVal(i % 2 == 0);
      histogram.Advance(val);
    }

    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<bool>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 25);
  }

  // Proper histogram calculation, separately, using histogram2
  {
    EXPECT_TRUE(histogram2.GetResult(exec_ctx_.get()).is_null_);
    for (int64_t i = 23; i < 45; i++) {
      auto val = BoolVal(i % 3 == 0);
      histogram2.Advance(val);
    }

    EXPECT_FALSE(histogram2.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram2.GetResult(exec_ctx_.get());
    auto deserialized_histogram2 = optimizer::Histogram<bool>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram2.GetTotalValueCount(), 22);
  }

  // Try to merge the two histograms. Result should capture global (non-NULL) histogram.
  {
    histogram.Merge(histogram2);
    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<bool>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 47);
  }

  // Try to merge in a NULL histogram. Result should be unchanged.
  {
    BooleanHistogramAggregate null_top_k;
    histogram.Merge(null_top_k);
    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<bool>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 47);
  }
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, HistogramInt) {
  // NULL check and merging NULL check
  {
    IntegerHistogramAggregate histogram;
    EXPECT_TRUE(histogram.GetResult(exec_ctx_.get()).is_null_);

    IntegerHistogramAggregate null_histogram;
    EXPECT_TRUE(null_histogram.GetResult(exec_ctx_.get()).is_null_);

    histogram.Merge(null_histogram);
    EXPECT_TRUE(histogram.GetResult(exec_ctx_.get()).is_null_);
  }

  IntegerHistogramAggregate histogram, histogram2;

  // Proper histogram calculation using histogram
  {
    for (int64_t i = 0; i < 15; i++) {
      auto val = Integer(i / 2);
      histogram.Advance(val);
    }

    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<decltype(Integer::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 15);
    EXPECT_EQ(deserialized_histogram.GetMinValue(), 0);
    EXPECT_EQ(deserialized_histogram.GetMaxValue(), 7);
  }

  // Proper histogram calculation, separately, using histogram2
  {
    EXPECT_TRUE(histogram2.GetResult(exec_ctx_.get()).is_null_);
    for (int64_t i = 15; i < 31; i++) {
      auto val = Integer(i / 2);
      histogram2.Advance(val);
    }

    EXPECT_FALSE(histogram2.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram2.GetResult(exec_ctx_.get());
    auto deserialized_histogram2 = optimizer::Histogram<decltype(Integer::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram2.GetTotalValueCount(), 16);
    EXPECT_EQ(deserialized_histogram2.GetMinValue(), 7);
    EXPECT_EQ(deserialized_histogram2.GetMaxValue(), 15);
  }

  // Try to merge the two histogram. Result should capture global (non-NULL) histogram.
  {
    histogram.Merge(histogram2);
    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<decltype(Integer::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 31);
    EXPECT_EQ(deserialized_histogram.GetMinValue(), 0);
    EXPECT_EQ(deserialized_histogram.GetMaxValue(), 15);
  }

  // Try to merge in a NULL histogram. Result should be unchanged.
  {
    IntegerHistogramAggregate null_top_k;
    histogram.Merge(null_top_k);
    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<decltype(Integer::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 31);
    EXPECT_EQ(deserialized_histogram.GetMinValue(), 0);
    EXPECT_EQ(deserialized_histogram.GetMaxValue(), 15);
  }
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, HistogramReal) {
  // NULL check and merging NULL check
  {
    RealHistogramAggregate histogram;
    EXPECT_TRUE(histogram.GetResult(exec_ctx_.get()).is_null_);

    RealHistogramAggregate null_histogram;
    EXPECT_TRUE(null_histogram.GetResult(exec_ctx_.get()).is_null_);

    histogram.Merge(null_histogram);
    EXPECT_TRUE(histogram.GetResult(exec_ctx_.get()).is_null_);
  }

  RealHistogramAggregate histogram, histogram2;

  // Proper histogram calculation using histogram
  {
    for (size_t i = 0; i < 8; i++) {
      auto val = Real(static_cast<double>(i) / 2);
      histogram.Advance(val);
    }

    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<decltype(Real::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 8);
    EXPECT_EQ(deserialized_histogram.GetMinValue(), 0);
    EXPECT_EQ(deserialized_histogram.GetMaxValue(), 3.5);
  }

  // Proper histogram calculation, separately, using histogram2
  {
    EXPECT_TRUE(histogram2.GetResult(exec_ctx_.get()).is_null_);
    for (size_t i = 8; i < 16; i++) {
      auto val = Real(static_cast<double>(i) / 2);
      histogram2.Advance(val);
    }

    EXPECT_FALSE(histogram2.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram2.GetResult(exec_ctx_.get());
    auto deserialized_histogram2 = optimizer::Histogram<decltype(Real::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram2.GetTotalValueCount(), 8);
    EXPECT_EQ(deserialized_histogram2.GetMinValue(), 4);
    EXPECT_EQ(deserialized_histogram2.GetMaxValue(), 7.5);
  }

  // Try to merge the two histograms. Result should capture global (non-NULL) histogram.
  {
    histogram.Merge(histogram2);
    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<decltype(Real::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 16);
    EXPECT_EQ(deserialized_histogram.GetMinValue(), 0);
    EXPECT_EQ(deserialized_histogram.GetMaxValue(), 7.5);
  }

  // Try to merge in a NULL histogram. Result should be unchanged.
  {
    RealHistogramAggregate null_histogram;
    histogram.Merge(null_histogram);
    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<decltype(Real::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 16);
    EXPECT_EQ(deserialized_histogram.GetMinValue(), 0);
    EXPECT_EQ(deserialized_histogram.GetMaxValue(), 7.5);
  }
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, HistogramDecimal) {
  // NULL check and merging NULL check
  {
    DecimalHistogramAggregate histogram;
    EXPECT_TRUE(histogram.GetResult(exec_ctx_.get()).is_null_);

    DecimalHistogramAggregate null_histogram;
    EXPECT_TRUE(null_histogram.GetResult(exec_ctx_.get()).is_null_);

    histogram.Merge(null_histogram);
    EXPECT_TRUE(histogram.GetResult(exec_ctx_.get()).is_null_);
  }

  DecimalHistogramAggregate histogram, histogram2;

  // Proper histogram calculation using histogram
  {
    for (Decimal64 i(0); i < 15; i += 1) {
      auto val = DecimalVal(i / 2);
      histogram.Advance(val);
    }

    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<decltype(DecimalVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 15);
    EXPECT_EQ(deserialized_histogram.GetMinValue(), 0);
    EXPECT_EQ(deserialized_histogram.GetMaxValue(), 7);
  }

  // Proper histogram calculation, separately, using histogram2
  {
    EXPECT_TRUE(histogram2.GetResult(exec_ctx_.get()).is_null_);
    for (Decimal64 i(15); i < 31; i += 1) {
      auto val = DecimalVal(i / 2);
      histogram2.Advance(val);
    }

    EXPECT_FALSE(histogram2.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram2.GetResult(exec_ctx_.get());
    auto deserialized_histogram2 = optimizer::Histogram<decltype(DecimalVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram2.GetTotalValueCount(), 16);
    EXPECT_EQ(deserialized_histogram2.GetMinValue(), 7);
    EXPECT_EQ(deserialized_histogram2.GetMaxValue(), 15);
  }

  // Try to merge the two histogram. Result should capture global (non-NULL) histogram.
  {
    histogram.Merge(histogram2);
    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<decltype(DecimalVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 31);
    EXPECT_EQ(deserialized_histogram.GetMinValue(), 0);
    EXPECT_EQ(deserialized_histogram.GetMaxValue(), 15);
  }

  // Try to merge in a NULL histogram. Result should be unchanged.
  {
    DecimalHistogramAggregate null_top_k;
    histogram.Merge(null_top_k);
    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<decltype(DecimalVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 31);
    EXPECT_EQ(deserialized_histogram.GetMinValue(), 0);
    EXPECT_EQ(deserialized_histogram.GetMaxValue(), 15);
  }
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, HistogramString) {
  // NULL check and merging NULL check
  {
    StringHistogramAggregate histogram;
    EXPECT_TRUE(histogram.GetResult(exec_ctx_.get()).is_null_);

    StringHistogramAggregate null_histogram;
    EXPECT_TRUE(null_histogram.GetResult(exec_ctx_.get()).is_null_);

    histogram.Merge(null_histogram);
    EXPECT_TRUE(histogram.GetResult(exec_ctx_.get()).is_null_);
  }

  StringHistogramAggregate histogram, histogram2;

  // Proper histogram calculation using histogram
  {
    auto val = StringVal(storage::VarlenEntry::Create("Foo"));
    histogram.Advance(val);
    histogram.Advance(val);
    val = StringVal(storage::VarlenEntry::Create("Bar"));
    histogram.Advance(val);

    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<decltype(StringVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 3);
  }

  // Proper histogram calculation, separately, using histogram2
  {
    EXPECT_TRUE(histogram2.GetResult(exec_ctx_.get()).is_null_);
    auto val = StringVal(storage::VarlenEntry::Create("Ice Climbers"));
    histogram2.Advance(val);
    val = StringVal(storage::VarlenEntry::Create("Marth"));
    histogram2.Advance(val);

    EXPECT_FALSE(histogram2.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram2.GetResult(exec_ctx_.get());
    auto deserialized_histogram2 = optimizer::Histogram<decltype(StringVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram2.GetTotalValueCount(), 2);
  }

  // Try to merge the two histogram. Result should capture global (non-NULL) histogram.
  {
    histogram.Merge(histogram2);
    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<decltype(StringVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 5);
  }

  // Try to merge in a NULL histogram. Result should be unchanged.
  {
    StringHistogramAggregate null_histogram;
    histogram.Merge(null_histogram);
    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<decltype(StringVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 5);
  }
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, HistogramDate) {
  // NULL check and merging NULL check
  {
    DateHistogramAggregate histogram;
    EXPECT_TRUE(histogram.GetResult(exec_ctx_.get()).is_null_);

    DateHistogramAggregate null_histogram;
    EXPECT_TRUE(null_histogram.GetResult(exec_ctx_.get()).is_null_);

    histogram.Merge(null_histogram);
    EXPECT_TRUE(histogram.GetResult(exec_ctx_.get()).is_null_);
  }

  DateHistogramAggregate histogram, histogram2;

  // Proper histogram calculation using histogram
  {
    for (int64_t i = 0; i < 15; i++) {
      auto val = DateVal(i / 2);
      histogram.Advance(val);
    }

    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<decltype(DateVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 15);
    EXPECT_EQ(deserialized_histogram.GetMinValue(), 0);
    EXPECT_EQ(deserialized_histogram.GetMaxValue(), 7);
  }

  // Proper histogram calculation, separately, using histogram2
  {
    EXPECT_TRUE(histogram2.GetResult(exec_ctx_.get()).is_null_);
    for (int64_t i = 15; i < 31; i++) {
      auto val = DateVal(i / 2);
      histogram2.Advance(val);
    }

    EXPECT_FALSE(histogram2.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram2.GetResult(exec_ctx_.get());
    auto deserialized_histogram2 = optimizer::Histogram<decltype(DateVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram2.GetTotalValueCount(), 16);
    EXPECT_EQ(deserialized_histogram2.GetMinValue(), 7);
    EXPECT_EQ(deserialized_histogram2.GetMaxValue(), 15);
  }

  // Try to merge the two histogram. Result should capture global (non-NULL) histogram.
  {
    histogram.Merge(histogram2);
    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<decltype(DateVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 31);
    EXPECT_EQ(deserialized_histogram.GetMinValue(), 0);
    EXPECT_EQ(deserialized_histogram.GetMaxValue(), 15);
  }

  // Try to merge in a NULL histogram. Result should be unchanged.
  {
    DateHistogramAggregate null_top_k;
    histogram.Merge(null_top_k);
    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<decltype(DateVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 31);
    EXPECT_EQ(deserialized_histogram.GetMinValue(), 0);
    EXPECT_EQ(deserialized_histogram.GetMaxValue(), 15);
  }
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, HistogramTimestamp) {
  // NULL check and merging NULL check
  {
    TimestampHistogramAggregate histogram;
    EXPECT_TRUE(histogram.GetResult(exec_ctx_.get()).is_null_);

    TimestampHistogramAggregate null_histogram;
    EXPECT_TRUE(null_histogram.GetResult(exec_ctx_.get()).is_null_);

    histogram.Merge(null_histogram);
    EXPECT_TRUE(histogram.GetResult(exec_ctx_.get()).is_null_);
  }

  TimestampHistogramAggregate histogram, histogram2;

  // Proper histogram calculation using histogram
  {
    for (int64_t i = 0; i < 15; i++) {
      auto val = TimestampVal(i / 2);
      histogram.Advance(val);
    }

    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<decltype(TimestampVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 15);
    EXPECT_EQ(deserialized_histogram.GetMinValue(), 0);
    EXPECT_EQ(deserialized_histogram.GetMaxValue(), 7);
  }

  // Proper histogram calculation, separately, using histogram2
  {
    EXPECT_TRUE(histogram2.GetResult(exec_ctx_.get()).is_null_);
    for (int64_t i = 15; i < 31; i++) {
      auto val = TimestampVal(i / 2);
      histogram2.Advance(val);
    }

    EXPECT_FALSE(histogram2.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram2.GetResult(exec_ctx_.get());
    auto deserialized_histogram2 = optimizer::Histogram<decltype(TimestampVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram2.GetTotalValueCount(), 16);
    EXPECT_EQ(deserialized_histogram2.GetMinValue(), 7);
    EXPECT_EQ(deserialized_histogram2.GetMaxValue(), 15);
  }

  // Try to merge the two histogram. Result should capture global (non-NULL) histogram.
  {
    histogram.Merge(histogram2);
    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<decltype(TimestampVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 31);
    EXPECT_EQ(deserialized_histogram.GetMinValue(), 0);
    EXPECT_EQ(deserialized_histogram.GetMaxValue(), 15);
  }

  // Try to merge in a NULL histogram. Result should be unchanged.
  {
    TimestampHistogramAggregate null_top_k;
    histogram.Merge(null_top_k);
    EXPECT_FALSE(histogram.GetResult(exec_ctx_.get()).is_null_);
    auto json_res = histogram.GetResult(exec_ctx_.get());
    auto deserialized_histogram = optimizer::Histogram<decltype(TimestampVal::val_)>::Deserialize(
        reinterpret_cast<const std::byte *>(json_res.GetContent()), json_res.GetLength());
    EXPECT_EQ(deserialized_histogram.GetTotalValueCount(), 31);
    EXPECT_EQ(deserialized_histogram.GetMinValue(), 0);
    EXPECT_EQ(deserialized_histogram.GetMaxValue(), 15);
  }
}

}  // namespace noisepage::execution::sql::test
