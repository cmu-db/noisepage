#include "execution/tpl_test.h"  // NOLINT

#include "execution/sql/aggregators.h"
#include "execution/sql/value.h"

namespace tpl::sql::test {

class AggregatorsTest : public TplTest {};

TEST_F(AggregatorsTest, CountTest) {
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
      count.Advance(&val);
    }
    EXPECT_EQ(5, count.GetCountResult().val);
  }
}

TEST_F(AggregatorsTest, CountMergeTest) {
  // Even inputs are NULL
  CountAggregate count_1, count_2;

  // Insert into count_1
  for (u32 i = 0; i < 100; i++) {
    Integer val = (i % 2 == 0 ? Integer::Null() : Integer(i));
    count_1.Advance(&val);
  }
  for (u32 i = 0; i < 100; i++) {
    Integer val(i);
    count_2.Advance(&val);
  }

  auto merged = count_1.GetCountResult().val + count_2.GetCountResult().val;

  count_1.Merge(count_2);

  EXPECT_EQ(merged, count_1.GetCountResult().val);
}

TEST_F(AggregatorsTest, SumIntegerTest) {
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
      sum.AdvanceNullable(&val);
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
      sum.Advance(&val);
    }

    EXPECT_FALSE(sum.GetResultSum().is_null);
    EXPECT_EQ(45, sum.GetResultSum().val);
  }
}

TEST_F(AggregatorsTest, MergeSumIntegersTest) {
  IntegerSumAggregate sum1;
  EXPECT_TRUE(sum1.GetResultSum().is_null);
  for (u64 i = 0; i < 10; i++) {
    auto val = Integer(i);
    sum1.Advance(&val);
  }

  IntegerSumAggregate sum2;
  EXPECT_TRUE(sum2.GetResultSum().is_null);
  for (u64 i = 10; i < 20; i++) {
    auto val = Integer(i);
    sum2.Advance(&val);
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
    sum3.Advance(&val);
  }
  sum1.Merge(sum3);
  EXPECT_EQ(0, sum1.GetResultSum().val);
}

TEST_F(AggregatorsTest, MaxIntegerTest) {
  IntegerMaxAggregate max;
  EXPECT_TRUE(max.GetResultMax().is_null);

  IntegerMaxAggregate nullMax;
  EXPECT_TRUE(nullMax.GetResultMax().is_null);

  max.Merge(nullMax);
  EXPECT_TRUE(max.GetResultMax().is_null);

  for (i64 i = 0; i < 25; i++) {
    i64 j = i;

    // mix in some low numbers
    if (i % 2 == 0) {
      j = -i;
    }
    auto val = Integer(j);
    max.Advance(&val);
  }

  EXPECT_FALSE(max.GetResultMax().is_null);
  EXPECT_EQ(23, max.GetResultMax().val);

  IntegerMaxAggregate max2;
  EXPECT_TRUE(max2.GetResultMax().is_null);
  for (i64 i = 23; i < 45; i++) {
    i64 j = i;

    // mix in some low numbers
    if (i % 2 == 0) {
      j = -i;
    }
    auto val = Integer(j);
    max2.Advance(&val);
  }

  EXPECT_FALSE(max2.GetResultMax().is_null);
  EXPECT_EQ(43, max2.GetResultMax().val);

  max.Merge(max2);
  EXPECT_FALSE(max.GetResultMax().is_null);
  EXPECT_EQ(43, max.GetResultMax().val);

  max.Merge(nullMax);
  EXPECT_FALSE(max.GetResultMax().is_null);
  EXPECT_EQ(43, max.GetResultMax().val);
}

TEST_F(AggregatorsTest, MinIntegerTest) {
  IntegerMinAggregate min;
  EXPECT_TRUE(min.GetResultMin().is_null);

  IntegerMinAggregate nullMin;
  EXPECT_TRUE(nullMin.GetResultMin().is_null);

  min.Merge(nullMin);
  EXPECT_TRUE(min.GetResultMin().is_null);

  for (i64 i = 0; i < 25; i++) {
    i64 j = i;

    // mix in some low numbers
    if (i % 2 == 0) {
      j = -i;
    }
    auto val = Integer(j);
    min.Advance(&val);
  }

  EXPECT_FALSE(min.GetResultMin().is_null);
  EXPECT_EQ(-24, min.GetResultMin().val);

  IntegerMinAggregate min2;
  EXPECT_TRUE(min2.GetResultMin().is_null);
  for (i64 i = 23; i < 45; i++) {
    i64 j = i;

    // mix in some low numbers
    if (i % 2 == 0) {
      j = -i;
    }
    auto val = Integer(j);
    min2.Advance(&val);
  }

  EXPECT_FALSE(min2.GetResultMin().is_null);
  EXPECT_EQ(-44, min2.GetResultMin().val);

  min.Merge(min2);
  EXPECT_FALSE(min.GetResultMin().is_null);
  EXPECT_EQ(-44, min.GetResultMin().val);

  min.Merge(nullMin);
  EXPECT_FALSE(min.GetResultMin().is_null);
  EXPECT_EQ(-44, min.GetResultMin().val);
}

TEST_F(AggregatorsTest, AvgIntegerTest) {
  IntegerAvgAggregate avg;
  EXPECT_TRUE(avg.GetResultAvg().is_null);

  IntegerAvgAggregate nullAvg;
  EXPECT_TRUE(nullAvg.GetResultAvg().is_null);

  avg.Merge(nullAvg);
  EXPECT_TRUE(avg.GetResultAvg().is_null);

  for (i64 i = 0; i < 25; i++) {
    i64 j = i;

    auto val = Integer(j);
    avg.Advance(&val);
  }

  EXPECT_FALSE(avg.GetResultAvg().is_null);
  EXPECT_EQ(12, avg.GetResultAvg().val);

  IntegerAvgAggregate avg2;
  EXPECT_TRUE(avg2.GetResultAvg().is_null);
  for (i64 i = 0; i > -25; i--) {
    auto val = Integer(i);
    avg2.Advance(&val);
  }

  EXPECT_FALSE(avg2.GetResultAvg().is_null);
  EXPECT_EQ(-12, avg2.GetResultAvg().val);

  avg.Merge(avg2);
  EXPECT_FALSE(avg.GetResultAvg().is_null);
  EXPECT_EQ(0, avg.GetResultAvg().val);

  avg.Merge(nullAvg);
  EXPECT_FALSE(avg.GetResultAvg().is_null);
  EXPECT_EQ(0, avg.GetResultAvg().val);
}

}  // namespace tpl::sql::test
