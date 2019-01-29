#include <random>
#include "gtest/gtest.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "type/value_peeker.h"
#include "type/value_wrapper.h"
#include "util/test_harness.h"

namespace terrier {

class ValueTests : public TerrierTest {
 public:
  std::default_random_engine generator_;
  const uint32_t num_iterations_ = 10000;
};

// NOLINTNEXTLINE
TEST_F(ValueTests, WrapNullTest) {
  byte *const data = nullptr;
  auto value = type::ValueWrapper::WrapBoolean(data);
  EXPECT_TRUE(value.IsNull());
  value = type::ValueWrapper::WrapTinyInt(data);
  EXPECT_TRUE(value.IsNull());
  value = type::ValueWrapper::WrapSmallInt(data);
  EXPECT_TRUE(value.IsNull());
  value = type::ValueWrapper::WrapInteger(data);
  EXPECT_TRUE(value.IsNull());
  value = type::ValueWrapper::WrapBigInt(data);
  EXPECT_TRUE(value.IsNull());
  value = type::ValueWrapper::WrapDecimal(data);
  EXPECT_TRUE(value.IsNull());
  value = type::ValueWrapper::WrapDate(data);
  EXPECT_TRUE(value.IsNull());
  value = type::ValueWrapper::WrapTimestamp(data);
  EXPECT_TRUE(value.IsNull());
}

// NOLINTNEXTLINE
TEST_F(ValueTests, GetNullTest) {
  auto value = type::ValueFactory::GetNull();
  EXPECT_TRUE(value.IsNull());
}

// NOLINTNEXTLINE
TEST_F(ValueTests, WrapBooleanTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
    auto value = type::ValueWrapper::WrapBoolean(reinterpret_cast<byte *>(&data));
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, type::ValuePeeker::PeekBoolean(value));
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, GetBooleanTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
    auto value = type::ValueFactory::GetBoolean(data);
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, type::ValuePeeker::PeekBoolean(value));
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, WrapTinyIntTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<int8_t>(std::uniform_int_distribution<int8_t>(INT8_MIN, INT8_MAX)(generator_));
    auto value = type::ValueWrapper::WrapTinyInt(reinterpret_cast<byte *>(&data));
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, type::ValuePeeker::PeekTinyInt(value));
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, GetTinyIntTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<int8_t>(std::uniform_int_distribution<int8_t>(INT8_MIN, INT8_MAX)(generator_));
    auto value = type::ValueFactory::GetTinyInt(data);
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, type::ValuePeeker::PeekTinyInt(value));
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, WrapSmallIntTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<int16_t>(std::uniform_int_distribution<int16_t>(INT16_MIN, INT16_MAX)(generator_));
    auto value = type::ValueWrapper::WrapSmallInt(reinterpret_cast<byte *>(&data));
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, type::ValuePeeker::PeekSmallInt(value));
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, GetSmallIntTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<int16_t>(std::uniform_int_distribution<int16_t>(INT16_MIN, INT16_MAX)(generator_));
    auto value = type::ValueFactory::GetSmallInt(data);
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, type::ValuePeeker::PeekSmallInt(value));
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, WrapIntegerTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<int32_t>(std::uniform_int_distribution<int32_t>(INT32_MIN, INT32_MAX)(generator_));
    auto value = type::ValueWrapper::WrapInteger(reinterpret_cast<byte *>(&data));
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, type::ValuePeeker::PeekInteger(value));
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, GetIntegerTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<int32_t>(std::uniform_int_distribution<int32_t>(INT32_MIN, INT32_MAX)(generator_));
    auto value = type::ValueFactory::GetInteger(data);
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, type::ValuePeeker::PeekInteger(value));
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, WrapBigIntTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<int64_t>(std::uniform_int_distribution<int64_t>(INT64_MIN, INT64_MAX)(generator_));
    auto value = type::ValueWrapper::WrapBigInt(reinterpret_cast<byte *>(&data));
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, type::ValuePeeker::PeekBigInt(value));
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, GetBigIntTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<int64_t>(std::uniform_int_distribution<int64_t>(INT64_MIN, INT64_MAX)(generator_));
    auto value = type::ValueFactory::GetBigInt(data);
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, type::ValuePeeker::PeekBigInt(value));
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, WrapDateTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<type::date_t>(std::uniform_int_distribution<uint32_t>(0, UINT32_MAX)(generator_));
    auto value = type::ValueWrapper::WrapDate(reinterpret_cast<byte *>(&data));
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, type::ValuePeeker::PeekDate(value));
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, GetDateTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<type::date_t>(std::uniform_int_distribution<uint32_t>(0, UINT32_MAX)(generator_));
    auto value = type::ValueFactory::GetDate(data);
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, type::ValuePeeker::PeekDate(value));
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, WrapTimestampTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<type::timestamp_t>(std::uniform_int_distribution<uint64_t>(0, UINT64_MAX)(generator_));
    auto value = type::ValueWrapper::WrapTimestamp(reinterpret_cast<byte *>(&data));
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, type::ValuePeeker::PeekTimestamp(value));
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, GetTimestampTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<type::timestamp_t>(std::uniform_int_distribution<uint64_t>(0, UINT64_MAX)(generator_));
    auto value = type::ValueFactory::GetTimestamp(data);
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, type::ValuePeeker::PeekTimestamp(value));
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, WrapDecimalTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = std::uniform_real_distribution<double>(DBL_MIN, DBL_MAX)(generator_);
    auto value = type::ValueWrapper::WrapDecimal(reinterpret_cast<byte *>(&data));
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, type::ValuePeeker::PeekDecimal(value));
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, GetDecimalTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = std::uniform_real_distribution<double>(DBL_MIN, DBL_MAX)(generator_);
    auto value = type::ValueFactory::GetDecimal(data);
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, type::ValuePeeker::PeekDecimal(value));
  }
}

}  // namespace terrier
