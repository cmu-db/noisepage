#include "gtest/gtest.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace terrier::type {

// NOLINTNEXTLINE
TEST(valueTests, BasicTest) {
  // boolean
  bool bool_value = true;
  // Value pv_boolean = Value(bool_value);
  Value pv_boolean = ValueFactory::GetBooleanValue(bool_value);
  EXPECT_TRUE(pv_boolean.GetBooleanValue() == bool_value);

  // integers
  int8_t tinyint_value = 0;
  Value pv_tiny = ValueFactory::GetTinyIntValue(tinyint_value);
  EXPECT_TRUE(pv_tiny.GetTinyIntValue() == tinyint_value);

  // smallint
  // integer
  // bigint
  // double
  // timestamp
  type::timestamp_t timestamp_value = static_cast<type::timestamp_t>(0);
  Value pv_timestamp = ValueFactory::GetTimeStampValue(timestamp_value);
  EXPECT_TRUE(pv_timestamp.GetTimestampValue() == timestamp_value);
  // date
}
}  // namespace terrier::type
