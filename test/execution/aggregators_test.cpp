#include "execution/tpl_test.h"  // NOLINT

#include "execution/sql/aggregators.h"
#include "execution/sql/value.h"

namespace terrier::sql::test {

class AggregatorsTest : public TplTest {};

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, Count) {
  //
  // Count on empty input
  //

  {
    CountAggregate count;
    EXPECT_EQ(0, count.GetCountResult().val);
  }

  //
  // Count on mixed NULL and non-NULL input
  //

  {
    // Even inputs are NULL
    CountAggregate count;
    for (u32 i = 0; i < 10; i++) {
      Integer val = (i % 2 == 0 ? Integer::Null() : Integer(i));
      count.Advance(val);
    }
    EXPECT_EQ(5, count.GetCountResult().val);
  }
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, CountMerge) {
  // Even inputs are NULL
  CountAggregate count_1, count_2;

  // Insert into count_1
  for (u32 i = 0; i < 100; i++) {
    Integer val = (i % 2 == 0 ? Integer::Null() : Integer(i));
    count_1.Advance(val);
  }
  for (u32 i = 0; i < 100; i++) {
    Integer val(i);
    count_2.Advance(val);
  }

  auto merged = count_1.GetCountResult().val + count_2.GetCountResult().val;

  count_1.Merge(count_2);

  EXPECT_EQ(merged, count_1.GetCountResult().val);
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, SumInteger) {
  //
  // SUM on empty input is null
  //

  {
    IntegerSumAggregate sum;
    EXPECT_TRUE(sum.GetResultSum().is_null);
  }

  //
  // Sum on mixed NULL and non-NULL input
  //

  {
    // [1, 3, 5, 7, 9]
    IntegerSumAggregate sum;
    for (u32 i = 0; i < 10; i++) {
      Integer val = (i % 2 == 0 ? Integer::Null() : Integer(i));
      sum.Advance(val);
    }

    EXPECT_FALSE(sum.GetResultSum().is_null);
    EXPECT_EQ(25, sum.GetResultSum().val);
  }

  //
  // Sum on non-NULL input
  //

  {
    // [0..9]
    IntegerSumAggregate sum;
    for (u32 i = 0; i < 10; i++) {
      Integer val(i);
      sum.Advance(val);
    }

    EXPECT_FALSE(sum.GetResultSum().is_null);
    EXPECT_EQ(45, sum.GetResultSum().val);
  }
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, MergeSumIntegers) {
  IntegerSumAggregate sum1;
  EXPECT_TRUE(sum1.GetResultSum().is_null);
  for (u64 i = 0; i < 10; i++) {
    auto val = Integer(i);
    sum1.Advance(val);
  }

  IntegerSumAggregate sum2;
  EXPECT_TRUE(sum2.GetResultSum().is_null);
  for (u64 i = 10; i < 20; i++) {
    auto val = Integer(i);
    sum2.Advance(val);
  }
  sum1.Merge(sum2);
  EXPECT_FALSE(sum1.GetResultSum().is_null);
  EXPECT_EQ(190, sum1.GetResultSum().val);

  IntegerSumAggregate sum3;
  EXPECT_TRUE(sum3.GetResultSum().is_null);
  sum1.Merge(sum3);
  EXPECT_EQ(190, sum1.GetResultSum().val);

  for (i64 i = 0; i < 20; i++) {
    auto val = Integer(-i);
    sum3.Advance(val);
  }
  sum1.Merge(sum3);
  EXPECT_EQ(0, sum1.GetResultSum().val);
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, SumReal) {
  // SUM on empty input is null
  {
    RealSumAggregate sum;
    EXPECT_TRUE(sum.GetResultSum().is_null);
  }

  RealSumAggregate sum, sum2;

  // Push in some sums (with NULLs mixed in)
  {
    // [1, 3, 5, 7, 9]
    EXPECT_TRUE(sum.GetResultSum().is_null);
    for (u32 i = 0; i < 10; i++) {
      Real val = (i % 2 == 0 ? Real::Null() : Real(static_cast<double>(i)));
      sum.Advance(val);
    }

    EXPECT_FALSE(sum.GetResultSum().is_null);
    EXPECT_DOUBLE_EQ(25.0, sum.GetResultSum().val);
  }

  // Push non-NULL values into sum2
  {
    // [1, 2, 3, 4, 5, 6, 7, 8, 9]
    EXPECT_TRUE(sum2.GetResultSum().is_null);
    for (u32 i = 0; i < 10; i++) {
      Real val(static_cast<double>(i));
      sum2.Advance(val);
    }

    EXPECT_FALSE(sum2.GetResultSum().is_null);
    EXPECT_DOUBLE_EQ(45.0, sum2.GetResultSum().val);
  }

  // Try to merge both sums. Result should be total sum.
  sum.Merge(sum2);
  EXPECT_FALSE(sum.GetResultSum().is_null);
  EXPECT_DOUBLE_EQ(70.0, sum.GetResultSum().val);

  // Try to merge in NULL sum. Original sum should be unchanged.
  RealSumAggregate null_sum;
  sum.Merge(null_sum);
  EXPECT_FALSE(sum.GetResultSum().is_null);
  EXPECT_DOUBLE_EQ(70.0, sum.GetResultSum().val);
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, MaxInteger) {
  // NULL check and merging NULL check
  {
    IntegerMaxAggregate max;
    EXPECT_TRUE(max.GetResultMax().is_null);

    IntegerMaxAggregate null_max;
    EXPECT_TRUE(null_max.GetResultMax().is_null);

    max.Merge(null_max);
    EXPECT_TRUE(max.GetResultMax().is_null);
  }

  IntegerMaxAggregate max1, max2;

  // Proper max calculation
  {
    for (i64 i = 0; i < 25; i++) {
      i64 j = i;

      // mix in some low numbers
      if (i % 2 == 0) {
        j = -i;
      }

      auto val = Integer(j);
      max1.Advance(val);
    }

    EXPECT_FALSE(max1.GetResultMax().is_null);
    EXPECT_EQ(23, max1.GetResultMax().val);
  }

  // Ditto for the second max
  {
    EXPECT_TRUE(max2.GetResultMax().is_null);
    for (i64 i = 23; i < 45; i++) {
      i64 j = i;

      // mix in some low numbers
      if (i % 2 == 0) {
        j = -i;
      }
      auto val = Integer(j);
      max2.Advance(val);
    }

    EXPECT_FALSE(max2.GetResultMax().is_null);
    EXPECT_EQ(43, max2.GetResultMax().val);
  }

  // Try to merge the two maxs. Result should capture global (non-NULL) max.
  max1.Merge(max2);
  EXPECT_FALSE(max1.GetResultMax().is_null);
  EXPECT_EQ(43, max1.GetResultMax().val);

  IntegerMaxAggregate null_max;
  max1.Merge(null_max);
  EXPECT_FALSE(max1.GetResultMax().is_null);
  EXPECT_EQ(43, max1.GetResultMax().val);
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, MinInteger) {
  // NULL check and merging NULL check
  {
    IntegerMinAggregate min;
    EXPECT_TRUE(min.GetResultMin().is_null);

    IntegerMinAggregate null_min;
    EXPECT_TRUE(null_min.GetResultMin().is_null);

    min.Merge(null_min);
    EXPECT_TRUE(min.GetResultMin().is_null);
  }

  IntegerMinAggregate min, min2;

  // Proper min calculation using min1
  {
    for (i64 i = 0; i < 25; i++) {
      i64 j = i;

      // mix in some low numbers
      if (i % 2 == 0) {
        j = -i;
      }

      auto val = Integer(j);
      min.Advance(val);
    }

    EXPECT_FALSE(min.GetResultMin().is_null);
    EXPECT_EQ(-24, min.GetResultMin().val);
  }

  // Proper min calculation, separately, using min2
  {
    EXPECT_TRUE(min2.GetResultMin().is_null);
    for (i64 i = 23; i < 45; i++) {
      i64 j = i;

      // mix in some low numbers
      if (i % 2 == 0) {
        j = -i;
      }
      auto val = Integer(j);
      min2.Advance(val);
    }

    EXPECT_FALSE(min2.GetResultMin().is_null);
    EXPECT_EQ(-44, min2.GetResultMin().val);
  }

  // Try to merge the two mins. Result should capture global (non-NULL) min.
  min.Merge(min2);
  EXPECT_FALSE(min.GetResultMin().is_null);
  EXPECT_EQ(-44, min.GetResultMin().val);

  IntegerMinAggregate null_min;
  min.Merge(null_min);
  EXPECT_FALSE(min.GetResultMin().is_null);
  EXPECT_EQ(-44, min.GetResultMin().val);
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, MaxReal) {
  // NULL check and merging NULL check
  {
    RealMaxAggregate max;
    EXPECT_TRUE(max.GetResultMax().is_null);

    RealMaxAggregate null_max;
    EXPECT_TRUE(null_max.GetResultMax().is_null);

    max.Merge(null_max);
    EXPECT_TRUE(max.GetResultMax().is_null);
  }

  RealMaxAggregate max1, max2;

  // Proper max calculation
  {
    for (i64 i = 0; i < 25; i++) {
      auto j = static_cast<double>(i);

      // mix in some low numbers
      if (i % 2 == 0) {
        j = static_cast<double>(-i);
      }

      auto val = Real(j);
      max1.Advance(val);
    }

    EXPECT_FALSE(max1.GetResultMax().is_null);
    EXPECT_DOUBLE_EQ(23.0, max1.GetResultMax().val);
  }

  // Ditto for the second max
  {
    EXPECT_TRUE(max2.GetResultMax().is_null);
    for (i64 i = 23; i < 45; i++) {
      auto j = static_cast<double>(i);

      // mix in some low numbers
      if (i % 2 == 0) {
        j = static_cast<double>(-i);
      }
      auto val = Real(j);
      max2.Advance(val);
    }

    EXPECT_FALSE(max2.GetResultMax().is_null);
    EXPECT_DOUBLE_EQ(43.0, max2.GetResultMax().val);
  }

  // Try to merge the two maxs. Result should capture global (non-NULL) max.
  max1.Merge(max2);
  EXPECT_FALSE(max1.GetResultMax().is_null);
  EXPECT_DOUBLE_EQ(43.0, max1.GetResultMax().val);

  RealMaxAggregate null_max;
  max1.Merge(null_max);
  EXPECT_FALSE(max1.GetResultMax().is_null);
  EXPECT_DOUBLE_EQ(43.0, max1.GetResultMax().val);
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, MinReal) {
  // NULL check and merging NULL check
  {
    RealMinAggregate min;
    EXPECT_TRUE(min.GetResultMin().is_null);

    RealMinAggregate null_min;
    EXPECT_TRUE(null_min.GetResultMin().is_null);

    min.Merge(null_min);
    EXPECT_TRUE(min.GetResultMin().is_null);
  }

  RealMinAggregate min, min2;

  // Proper min calculation using min1
  {
    for (i64 i = 0; i < 25; i++) {
      auto j = static_cast<double>(i);

      // mix in some low numbers
      if (i % 2 == 0) {
        j = static_cast<double>(-i);
      }

      auto val = Real(j);
      min.Advance(val);
    }

    EXPECT_FALSE(min.GetResultMin().is_null);
    EXPECT_DOUBLE_EQ(-24.0, min.GetResultMin().val);
  }

  // Proper min calculation, separately, using min2
  {
    EXPECT_TRUE(min2.GetResultMin().is_null);
    for (i64 i = 23; i < 45; i++) {
      auto j = static_cast<double>(i);

      // mix in some low numbers
      if (i % 2 == 0) {
        j = static_cast<double>(-i);
      }
      auto val = Real(j);
      min2.Advance(val);
    }

    EXPECT_FALSE(min2.GetResultMin().is_null);
    EXPECT_DOUBLE_EQ(-44.0, min2.GetResultMin().val);
  }

  // Try to merge the two mins. Result should capture global (non-NULL) min.
  min.Merge(min2);
  EXPECT_FALSE(min.GetResultMin().is_null);
  EXPECT_DOUBLE_EQ(-44.0, min.GetResultMin().val);

  RealMinAggregate null_min;
  min.Merge(null_min);
  EXPECT_FALSE(min.GetResultMin().is_null);
  EXPECT_DOUBLE_EQ(-44.0, min.GetResultMin().val);
}

// NOLINTNEXTLINE
TEST_F(AggregatorsTest, Avg) {
  // NULL check and merging NULL check
  {
    AvgAggregate avg;
    EXPECT_TRUE(avg.GetResultAvg().is_null);

    AvgAggregate null_avg;
    EXPECT_TRUE(null_avg.GetResultAvg().is_null);

    avg.Merge(null_avg);
    EXPECT_TRUE(avg.GetResultAvg().is_null);
  }

  AvgAggregate avg1, avg2;

  // Create first average aggregate
  {
    EXPECT_TRUE(avg1.GetResultAvg().is_null);
    double sum = 0.0, count = 0.0;
    for (u64 i = 0; i < 25; i++) {
      sum += static_cast<double>(i);
      count++;
      auto val = Integer(i);
      avg1.Advance(val);
    }

    EXPECT_FALSE(avg1.GetResultAvg().is_null);
    EXPECT_DOUBLE_EQ((sum / count), avg1.GetResultAvg().val);
  }

  // Create second average aggregate
  {
    EXPECT_TRUE(avg2.GetResultAvg().is_null);
    double sum = 0.0, count = 0.0;
    for (i64 i = 0; i < 25; i++) {
      sum += static_cast<double>(-i);
      count++;
      auto val = Integer(-i);
      avg2.Advance(val);
    }

    EXPECT_FALSE(avg2.GetResultAvg().is_null);
    EXPECT_DOUBLE_EQ((sum / count), avg2.GetResultAvg().val);
  }

  // Merge the two averages. Result should capture global (non-NULL) average.
  avg1.Merge(avg2);
  EXPECT_FALSE(avg1.GetResultAvg().is_null);
  EXPECT_DOUBLE_EQ(0.0, avg1.GetResultAvg().val);

  // Try to merge in a NULL average. Result should be unchanged.
  AvgAggregate null_avg;
  avg1.Merge(null_avg);
  EXPECT_FALSE(avg1.GetResultAvg().is_null);
  EXPECT_DOUBLE_EQ(0.0, avg1.GetResultAvg().val);
}

}  // namespace terrier::sql::test
