#include "execution/sql/runtime_types.h"
#include "test_util/test_harness.h"

namespace terrier {

struct RuntimeTypesTests : public TerrierTest {};

TEST_F(RuntimeTypesTests, DateTest) {
  auto ymd_res = terrier::execution::sql::Date::FromYMD(2020, 1, 1);
  auto res = terrier::execution::sql::Date::FromString("2020-01-01");
  EXPECT_TRUE(res.first);
  EXPECT_TRUE(ymd_res.first);
  EXPECT_EQ(res.second, ymd_res.second);
  EXPECT_EQ(res.second.ToString(), "2020-01-01");
  EXPECT_EQ(ymd_res.second.ToString(), "2020-01-01");
}

TEST_F(RuntimeTypesTests, TimestampTest) {
  auto res = terrier::execution::sql::Timestamp::FromString("2020-01-01 11:22:33.123");
  EXPECT_TRUE(res.first);
  EXPECT_EQ(res.second.ToString(), "2020-01-01 11:22:33.123000");
}

// TODO(WAN): throw in some timezone tests

}  // namespace terrier
