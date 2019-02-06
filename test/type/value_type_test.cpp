#include <random>
#include "gtest/gtest.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "type/value_peeker.h"
#include "util/test_harness.h"

namespace terrier::type {

class ValueTests : public TerrierTest {
 public:
  std::default_random_engine generator_;
  const uint32_t num_iterations_ = 10000;
};

// NOLINTNEXTLINE
TEST_F(ValueTests, BooleanTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));

    auto value = type::ValueFactory::GetBoolean(data);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::ValuePeeker::PeekBoolean(value));

    auto null = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
    value.SetNull(null);
    EXPECT_EQ(null, value.Null());

    value.SetNull(false);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::ValuePeeker::PeekBoolean(value));

    auto value2(value);
    EXPECT_EQ(value, value2);
    auto value3 = type::ValueFactory::GetBoolean(!data);
    EXPECT_NE(value, value3);
    value3 = value;
    EXPECT_EQ(value, value3);
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, TinyIntTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<int8_t>(std::uniform_int_distribution<int8_t>(INT8_MIN, INT8_MAX)(generator_));

    auto value = type::ValueFactory::GetTinyInt(data);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::ValuePeeker::PeekTinyInt(value));

    auto null = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
    value.SetNull(null);
    EXPECT_EQ(null, value.Null());

    value.SetNull(false);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::ValuePeeker::PeekTinyInt(value));

    auto value2(value);
    EXPECT_EQ(value, value2);
    auto value3 = type::ValueFactory::GetBoolean(true);
    EXPECT_NE(value, value3);
    value3 = value;
    EXPECT_EQ(value, value3);
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, SmallIntTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<int16_t>(std::uniform_int_distribution<int16_t>(INT16_MIN, INT16_MAX)(generator_));

    auto value = type::ValueFactory::GetSmallInt(data);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::ValuePeeker::PeekSmallInt(value));

    auto null = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
    value.SetNull(null);
    EXPECT_EQ(null, value.Null());

    value.SetNull(false);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::ValuePeeker::PeekSmallInt(value));

    auto value2(value);
    EXPECT_EQ(value, value2);
    auto value3 = type::ValueFactory::GetBoolean(true);
    EXPECT_NE(value, value3);
    value3 = value;
    EXPECT_EQ(value, value3);
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, IntegerTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<int32_t>(std::uniform_int_distribution<int32_t>(INT32_MIN, INT32_MAX)(generator_));

    auto value = type::ValueFactory::GetInteger(data);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::ValuePeeker::PeekInteger(value));

    auto null = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
    value.SetNull(null);
    EXPECT_EQ(null, value.Null());

    value.SetNull(false);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::ValuePeeker::PeekInteger(value));

    auto value2(value);
    EXPECT_EQ(value, value2);
    auto value3 = type::ValueFactory::GetBoolean(true);
    EXPECT_NE(value, value3);
    value3 = value;
    EXPECT_EQ(value, value3);
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, BigIntTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<int64_t>(std::uniform_int_distribution<int64_t>(INT64_MIN, INT64_MAX)(generator_));

    auto value = type::ValueFactory::GetBigInt(data);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::ValuePeeker::PeekBigInt(value));

    auto null = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
    value.SetNull(null);
    EXPECT_EQ(null, value.Null());

    value.SetNull(false);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::ValuePeeker::PeekBigInt(value));

    auto value2(value);
    EXPECT_EQ(value, value2);
    auto value3 = type::ValueFactory::GetBoolean(true);
    EXPECT_NE(value, value3);
    value3 = value;
    EXPECT_EQ(value, value3);
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, DecimalTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = std::uniform_real_distribution<double>(DBL_MIN, DBL_MAX)(generator_);

    auto value = type::ValueFactory::GetDecimal(data);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::ValuePeeker::PeekDecimal(value));

    auto null = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
    value.SetNull(null);
    EXPECT_EQ(null, value.Null());

    value.SetNull(false);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::ValuePeeker::PeekDecimal(value));

    auto value2(value);
    EXPECT_EQ(value, value2);
    auto value3 = type::ValueFactory::GetBoolean(true);
    EXPECT_NE(value, value3);
    value3 = value;
    EXPECT_EQ(value, value3);
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, TimestampTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<type::timestamp_t>(std::uniform_int_distribution<uint64_t>(0, UINT64_MAX)(generator_));

    auto value = type::ValueFactory::GetTimestamp(data);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::ValuePeeker::PeekTimestamp(value));

    auto null = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
    value.SetNull(null);
    EXPECT_EQ(null, value.Null());

    value.SetNull(false);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::ValuePeeker::PeekTimestamp(value));

    auto value2(value);
    EXPECT_EQ(value, value2);
    auto value3 = type::ValueFactory::GetBoolean(true);
    EXPECT_NE(value, value3);
    value3 = value;
    EXPECT_EQ(value, value3);
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, DateTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<type::date_t>(std::uniform_int_distribution<uint32_t>(0, UINT32_MAX)(generator_));

    auto value = type::ValueFactory::GetDate(data);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::ValuePeeker::PeekDate(value));

    auto null = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
    value.SetNull(null);
    EXPECT_EQ(null, value.Null());

    value.SetNull(false);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::ValuePeeker::PeekDate(value));

    auto value2(value);
    EXPECT_EQ(value, value2);
    auto value3 = type::ValueFactory::GetBoolean(true);
    EXPECT_NE(value, value3);
    value3 = value;
    EXPECT_EQ(value, value3);
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, VarCharTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto length = std::uniform_int_distribution<uint32_t>(1, UINT8_MAX)(generator_);
    char *const data = new char[length];
    for (uint32_t j = 0; j < length - 1; j++) {
      data[j] = std::uniform_int_distribution<char>('A', 'z')(generator_);
    }
    data[length - 1] = '\0';  // null terminate the c-string

    auto value = type::ValueFactory::GetVarChar(data);
    EXPECT_FALSE(value.Null());
    const char *peeked_data = type::ValuePeeker::PeekVarChar(value);
    EXPECT_EQ(0, std::memcmp(data, peeked_data, length));
    delete[] peeked_data;

    auto null = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
    value.SetNull(null);
    EXPECT_EQ(null, value.Null());

    value.SetNull(false);
    EXPECT_FALSE(value.Null());
    peeked_data = type::ValuePeeker::PeekVarChar(value);
    EXPECT_EQ(0, std::memcmp(data, peeked_data, length));
    delete[] peeked_data;
    delete[] data;

    auto value2(value);
    EXPECT_EQ(value, value2);
    auto value3 = type::ValueFactory::GetBoolean(true);
    EXPECT_NE(value, value3);
    value3 = value;
    EXPECT_EQ(value, value3);
  }
}

}  // namespace terrier::type
