#include "common/exception.h"
#include "execution/sql/runtime_types.h"
#include "execution/tpl_test.h"

namespace terrier::execution::sql::test {

class RuntimeTypesTest : public TplTest {};

TEST_F(RuntimeTypesTest, ExtractDateParts) {
  // Valid date
  Date d;
  EXPECT_NO_THROW({ d = Date::FromYMD(2016, 12, 19); });
  EXPECT_EQ(2016, d.ExtractYear());
  EXPECT_EQ(12, d.ExtractMonth());
  EXPECT_EQ(19, d.ExtractDay());

  // BC date.
  EXPECT_NO_THROW({ d = Date::FromYMD(-4000, 1, 2); });
  EXPECT_EQ(-4000, d.ExtractYear());
  EXPECT_EQ(1, d.ExtractMonth());
  EXPECT_EQ(2, d.ExtractDay());

  // Invalid
  EXPECT_THROW({ d = Date::FromYMD(1234, 3, 1111); }, ConversionException);
  EXPECT_THROW({ d = Date::FromYMD(1234, 93874, 11); }, ConversionException);
  EXPECT_THROW({ d = Date::FromYMD(1234, 7283, 192873); }, ConversionException);
  EXPECT_THROW({ d = Date::FromYMD(-40000, 12, 12); }, ConversionException);
  EXPECT_THROW({ d = Date::FromYMD(50000000, 12, 987); }, ConversionException);
  EXPECT_THROW({ d = Date::FromYMD(50000000, 921873, 1); }, ConversionException);
  EXPECT_THROW({ d = Date::FromYMD(-50000000, 921873, 21938); }, ConversionException);
}

TEST_F(RuntimeTypesTest, DateFromString) {
  // Valid date
  Date d;
  EXPECT_NO_THROW({ d = Date::FromString("1990-01-11"); });
  EXPECT_EQ(1990u, d.ExtractYear());
  EXPECT_EQ(1u, d.ExtractMonth());
  EXPECT_EQ(11u, d.ExtractDay());

  EXPECT_NO_THROW({ d = Date::FromString("2015-3-1"); });
  EXPECT_EQ(2015, d.ExtractYear());
  EXPECT_EQ(3u, d.ExtractMonth());
  EXPECT_EQ(1u, d.ExtractDay());

  EXPECT_NO_THROW({ d = Date::FromString("   1999-12-31    "); });
  EXPECT_EQ(1999, d.ExtractYear());
  EXPECT_EQ(12u, d.ExtractMonth());
  EXPECT_EQ(31u, d.ExtractDay());

  // Invalid
  EXPECT_THROW({ d = Date::FromString("1000-11-23123"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("1000-12323-19"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("1000-12323-199"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("50000000-12-20"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("50000000-12-120"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("50000000-1289217-12"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("da fuk?"); }, ConversionException);
  EXPECT_THROW({ d = Date::FromString("-1-1-23"); }, ConversionException);
}

TEST_F(RuntimeTypesTest, DateComparisons) {
  Date d1 = Date::FromString("2000-01-01");
  Date d2 = Date::FromString("2016-02-19");
  Date d3 = d1;
  Date d4 = Date::FromString("2017-10-10");
  EXPECT_NE(d1, d2);
  EXPECT_LT(d1, d2);
  EXPECT_EQ(d1, d3);
  EXPECT_GT(d4, d3);
  EXPECT_GT(d4, d2);
  EXPECT_GT(d4, d1);

  d1 = Date::FromYMD(-4000, 1, 1);
  d2 = Date::FromYMD(-4000, 1, 2);
  EXPECT_NE(d1, d2);
  EXPECT_LT(d1, d2);
}

TEST_F(RuntimeTypesTest, DateToString) {
  Date d1 = Date::FromString("2016-01-27");
  EXPECT_EQ("2016-01-27", d1.ToString());

  // Make sure we pad months and days
  d1 = Date::FromString("2000-1-1");
  EXPECT_EQ("2000-01-01", d1.ToString());
}

TEST_F(RuntimeTypesTest, ExtractTimestampParts) {
  // Valid timestamp.
  Timestamp t;
  EXPECT_NO_THROW({ t = Timestamp::FromHMSu(2016, 12, 19, 10, 20, 30, 0); });
  EXPECT_EQ(2016, t.ExtractYear());
  EXPECT_EQ(12, t.ExtractMonth());
  EXPECT_EQ(19, t.ExtractDay());
  EXPECT_EQ(10, t.ExtractHour());
  EXPECT_EQ(20, t.ExtractMinute());
  EXPECT_EQ(30, t.ExtractSecond());

  // BC timestamp.
  EXPECT_NO_THROW({ t = Timestamp::FromHMSu(-4000, 1, 2, 12, 24, 48, 0); });
  EXPECT_EQ(-4000, t.ExtractYear());
  EXPECT_EQ(1, t.ExtractMonth());
  EXPECT_EQ(2, t.ExtractDay());
  EXPECT_EQ(12, t.ExtractHour());
  EXPECT_EQ(24, t.ExtractMinute());
  EXPECT_EQ(48, t.ExtractSecond());

  // Invalid
  EXPECT_THROW({ t = Timestamp::FromHMSu(1234, 3, 4, 1, 1, 100, 0); }, ConversionException);
  EXPECT_THROW({ t = Timestamp::FromHMSu(1234, 3, 4, 1, 100, 1, 0); }, ConversionException);
  EXPECT_THROW({ t = Timestamp::FromHMSu(1234, 3, 4, 1, 100, 100, 0); }, ConversionException);
  EXPECT_THROW({ t = Timestamp::FromHMSu(1234, 3, 4, 25, 1, 1, 0); }, ConversionException);
  EXPECT_THROW({ t = Timestamp::FromHMSu(1234, 3, 4, 25, 1, 100, 0); }, ConversionException);
  EXPECT_THROW({ t = Timestamp::FromHMSu(1234, 3, 4, 25, 100, 1, 0); }, ConversionException);
  EXPECT_THROW({ t = Timestamp::FromHMSu(1234, 3, 4, 25, 100, 100, 0); }, ConversionException);
  EXPECT_THROW({ t = Timestamp::FromHMSu(50000000, 12, 9, 100, 1, 1, 0); }, ConversionException);
  EXPECT_THROW({ t = Timestamp::FromHMSu(50000000, 92187, 1, 13, 59, 60, 0); }, ConversionException);
}

TEST_F(RuntimeTypesTest, TimestampComparisons) {
  Timestamp t1 = Timestamp::FromHMSu(2000, 1, 1, 12, 0, 0, 0);
  Timestamp t2 = Timestamp::FromHMSu(2000, 1, 1, 16, 0, 0, 0);
  Timestamp t3 = t1;
  Timestamp t4 = Timestamp::FromHMSu(2017, 1, 1, 18, 18, 18, 0);

  EXPECT_NE(t1, t2);
  EXPECT_LT(t1, t2);
  EXPECT_EQ(t1, t3);
  EXPECT_GT(t4, t3);
  EXPECT_GT(t4, t2);
  EXPECT_GT(t4, t1);

  t1 = Timestamp::FromHMSu(-4000, 1, 1, 10, 10, 10, 0);
  t2 = Timestamp::FromHMSu(-4000, 1, 1, 10, 10, 11, 0);
  EXPECT_NE(t1, t2);
  EXPECT_LT(t1, t2);
}

TEST_F(RuntimeTypesTest, VarlenComparisons) {
  // Short strings first.
  {
    auto v1 = storage::VarlenEntry::Create("somethings");
    auto v2 = storage::VarlenEntry::Create("anotherone");
    auto v3 = v1;
    EXPECT_TRUE(v1.IsInlined());
    EXPECT_TRUE(v2.IsInlined());
    EXPECT_NE(v1, v2);
    EXPECT_LT(v2, v1);
    EXPECT_GT(v1, v2);
    EXPECT_EQ(v1, v3);
  }

  // Very short strings.
  {
    auto v1 = storage::VarlenEntry::Create("a");
    auto v2 = storage::VarlenEntry::Create("b");
    auto v3 = v1;
    auto v4 = storage::VarlenEntry::Create("");
    EXPECT_TRUE(v1.IsInlined());
    EXPECT_TRUE(v2.IsInlined());
    EXPECT_TRUE(v3.IsInlined());
    EXPECT_TRUE(v4.IsInlined());
    EXPECT_NE(v1, v2);
    EXPECT_LT(v1, v2);
    EXPECT_GT(v2, v1);
    EXPECT_EQ(v1, v3);
    EXPECT_NE(v1, v4);
    EXPECT_NE(v2, v4);
    EXPECT_NE(v3, v4);
    EXPECT_LT(v4, v1);
  }

  // Longer strings.
  auto s1 = "This is sort of a long string, but the end of the string should be different than XXX";
  auto s2 = "This is sort of a long string, but the end of the string should be different than YYY";
  {
    auto v1 = storage::VarlenEntry::Create(s1);
    auto v2 = storage::VarlenEntry::Create(s2);
    auto v3 = storage::VarlenEntry::Create("smallstring");
    auto v4 = storage::VarlenEntry::Create("This is so");  // A prefix of the longer strings.
    EXPECT_FALSE(v1.IsInlined());
    EXPECT_FALSE(v2.IsInlined());
    EXPECT_NE(v1, v2);
    EXPECT_LT(v1, v2);
    EXPECT_GT(v2, v1);
    EXPECT_EQ(v2, v2);

    EXPECT_NE(v1, v3);
    EXPECT_NE(v2, v3);
    EXPECT_GT(v3, v1);
    EXPECT_GT(v3, v2);

    EXPECT_LT(v4, v1);
    EXPECT_LT(v4, v2);
  }
}

}  // namespace terrier::execution::sql::test
