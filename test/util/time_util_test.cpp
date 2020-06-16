#include "util/time_util.h"
#include "date/date.h"
#include "test_util/test_harness.h"

namespace terrier {

struct TimeUtilTests : public TerrierTest {};

TEST_F(TimeUtilTests, DateTest) {
  date::year_month_day d{date::year(2020), date::month(1), date::day(1)};
  auto res = util::TimeConvertor::ParseDate("2020-01-01");
  EXPECT_TRUE(res.first);
  EXPECT_EQ(res.second, util::TimeConvertor::DateFromYMD(d));
  EXPECT_EQ(util::TimeConvertor::YMDFromDate(res.second), d);
  EXPECT_EQ(util::TimeConvertor::FormatDate(res.second), "2020-01-01");
}

TEST_F(TimeUtilTests, TimestampTest) {
  auto res = util::TimeConvertor::ParseTimestamp("2020-01-01 11:22:33.123");
  EXPECT_TRUE(res.first);
  EXPECT_EQ(util::TimeConvertor::FormatTimestamp(res.second), "2020-01-01 11:22:33.123000");
}

// TODO(WAN): throw in some timezone tests

}  // namespace terrier
