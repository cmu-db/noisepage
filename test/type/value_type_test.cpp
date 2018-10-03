#include "gtest/gtest.h"
#include "type/value.h"

namespace terrier::type {

TEST(valueTests, BasicTest) {
  // boolean
  boolean_t bool_value = (boolean_t) true;
  Value pv_boolean = Value(bool_value);
  EXPECT_TRUE(pv_boolean.GetBooleanValue() == bool_value);

  // integers
  int8_t tinyint_value = 0;
  Value pv_tiny = Value(tinyint_value);
  EXPECT_TRUE(pv_tiny.GetTinyIntValue() == tinyint_value);

  // smallint
  // integer
  // bigint
  // double
  // timestamp
  timestamp_t timestamp_value = (timestamp_t)0;
  Value pv_timestamp(timestamp_value);
  EXPECT_TRUE(pv_timestamp.GetTimestampValue() == timestamp_value);
  // date
}
}  // namespace terrier::type
