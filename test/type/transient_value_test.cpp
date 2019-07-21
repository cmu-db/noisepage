#include "type/transient_value.h"
#include <random>
#include <utility>
#include "type/transient_value_factory.h"
#include "type/transient_value_peeker.h"
#include "util/test_harness.h"

namespace terrier::type {

class ValueTests : public TerrierTest {
 public:
  std::default_random_engine generator_;
  const uint32_t num_iterations_ = 10000;
};

/**
 * These tests all follow the same basic structure:
 * 1) Randomly generate reference C type data
 * 2) Create a TransientValue using the TransientValueFactory from this C type
 * 3) Assert that the new TransientValue is not NULL
 * 4) Assert that the C type returned by TransientValuePeeker matches the reference C type data
 * 5) Flip a coin for reference NULL value
 * 6) Test SetNull, compare NULL value to reference
 * 7) Clear SetNull, compare to original reference C type
 * 8) Test copy constructor
 * 9) Test copy assignment operator
 * 10) Test move constructor
 * 11) Test move assignment operator
 */

// NOLINTNEXTLINE
TEST_F(ValueTests, BooleanTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));

    auto value = type::TransientValueFactory::GetBoolean(data);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::TransientValuePeeker::PeekBoolean(value));
    EXPECT_EQ(value.ToString(), "BOOLEAN");

    auto null = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
    value.SetNull(null);
    EXPECT_EQ(null, value.Null());

    value.SetNull(false);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::TransientValuePeeker::PeekBoolean(value));

    auto copy_constructed_value(value);
    EXPECT_EQ(value, copy_constructed_value);
    EXPECT_EQ(value.Hash(), copy_constructed_value.Hash());
    auto copy_assigned_value = type::TransientValueFactory::GetBoolean(!data);
    EXPECT_NE(value, copy_assigned_value);
    EXPECT_NE(value.Hash(), copy_assigned_value.Hash());
    copy_assigned_value = value;
    EXPECT_EQ(value, copy_assigned_value);
    EXPECT_EQ(value.Hash(), copy_assigned_value.Hash());

    auto move_constructed_value(std::move(value));
    EXPECT_EQ(copy_assigned_value, move_constructed_value);
    EXPECT_EQ(copy_assigned_value.Hash(), move_constructed_value.Hash());
    auto move_assigned_value = type::TransientValueFactory::GetBoolean(!data);
    EXPECT_NE(copy_assigned_value, move_assigned_value);
    EXPECT_NE(copy_assigned_value.Hash(), move_assigned_value.Hash());
    move_assigned_value = std::move(copy_assigned_value);
    EXPECT_EQ(copy_constructed_value, move_assigned_value);
    EXPECT_EQ(copy_constructed_value.Hash(), move_assigned_value.Hash());
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, TinyIntTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<int8_t>(std::uniform_int_distribution<int8_t>(INT8_MIN, INT8_MAX)(generator_));

    auto value = type::TransientValueFactory::GetTinyInt(data);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::TransientValuePeeker::PeekTinyInt(value));
    EXPECT_EQ(value.ToString(), "TINYINT");

    auto null = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
    value.SetNull(null);
    EXPECT_EQ(null, value.Null());

    value.SetNull(false);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::TransientValuePeeker::PeekTinyInt(value));

    auto copy_constructed_value(value);
    EXPECT_EQ(value, copy_constructed_value);
    EXPECT_EQ(value.Hash(), copy_constructed_value.Hash());
    auto copy_assigned_value = type::TransientValueFactory::GetBoolean(true);
    EXPECT_NE(value, copy_assigned_value);
    EXPECT_NE(value.Hash(), copy_assigned_value.Hash());
    copy_assigned_value = value;
    EXPECT_EQ(value, copy_assigned_value);
    EXPECT_EQ(value.Hash(), copy_assigned_value.Hash());

    auto move_constructed_value(std::move(value));
    EXPECT_EQ(copy_assigned_value, move_constructed_value);
    EXPECT_EQ(copy_assigned_value.Hash(), move_constructed_value.Hash());
    auto move_assigned_value = type::TransientValueFactory::GetBoolean(true);
    EXPECT_NE(copy_assigned_value, move_assigned_value);
    EXPECT_NE(copy_assigned_value.Hash(), move_assigned_value.Hash());
    move_assigned_value = std::move(copy_assigned_value);
    EXPECT_EQ(copy_constructed_value, move_assigned_value);
    EXPECT_EQ(copy_constructed_value.Hash(), move_assigned_value.Hash());
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, SmallIntTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<int16_t>(std::uniform_int_distribution<int16_t>(INT16_MIN, INT16_MAX)(generator_));

    auto value = type::TransientValueFactory::GetSmallInt(data);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::TransientValuePeeker::PeekSmallInt(value));
    EXPECT_EQ(value.ToString(), "SMALLINT");

    auto null = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
    value.SetNull(null);
    EXPECT_EQ(null, value.Null());

    value.SetNull(false);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::TransientValuePeeker::PeekSmallInt(value));

    auto copy_constructed_value(value);
    EXPECT_EQ(value, copy_constructed_value);
    EXPECT_EQ(value.Hash(), copy_constructed_value.Hash());
    auto copy_assigned_value = type::TransientValueFactory::GetBoolean(true);
    EXPECT_NE(value, copy_assigned_value);
    EXPECT_NE(value.Hash(), copy_assigned_value.Hash());
    copy_assigned_value = value;
    EXPECT_EQ(value, copy_assigned_value);
    EXPECT_EQ(value.Hash(), copy_assigned_value.Hash());

    auto move_constructed_value(std::move(value));
    EXPECT_EQ(copy_assigned_value, move_constructed_value);
    EXPECT_EQ(copy_assigned_value.Hash(), move_constructed_value.Hash());
    auto move_assigned_value = type::TransientValueFactory::GetBoolean(true);
    EXPECT_NE(copy_assigned_value, move_assigned_value);
    EXPECT_NE(copy_assigned_value.Hash(), move_assigned_value.Hash());
    move_assigned_value = std::move(copy_assigned_value);
    EXPECT_EQ(copy_constructed_value, move_assigned_value);
    EXPECT_EQ(copy_constructed_value.Hash(), move_assigned_value.Hash());
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, IntegerTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<int32_t>(std::uniform_int_distribution<int32_t>(INT32_MIN, INT32_MAX)(generator_));

    auto value = type::TransientValueFactory::GetInteger(data);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::TransientValuePeeker::PeekInteger(value));
    EXPECT_EQ(value.ToString(), "INTEGER");

    auto null = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
    value.SetNull(null);
    EXPECT_EQ(null, value.Null());

    value.SetNull(false);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::TransientValuePeeker::PeekInteger(value));

    auto copy_constructed_value(value);
    EXPECT_EQ(value, copy_constructed_value);
    EXPECT_EQ(value.Hash(), copy_constructed_value.Hash());
    auto copy_assigned_value = type::TransientValueFactory::GetBoolean(true);
    EXPECT_NE(value, copy_assigned_value);
    EXPECT_NE(value.Hash(), copy_assigned_value.Hash());
    copy_assigned_value = value;
    EXPECT_EQ(value, copy_assigned_value);
    EXPECT_EQ(value.Hash(), copy_assigned_value.Hash());

    auto move_constructed_value(std::move(value));
    EXPECT_EQ(copy_assigned_value, move_constructed_value);
    EXPECT_EQ(copy_assigned_value.Hash(), move_constructed_value.Hash());
    auto move_assigned_value = type::TransientValueFactory::GetBoolean(true);
    EXPECT_NE(copy_assigned_value, move_assigned_value);
    EXPECT_NE(copy_assigned_value.Hash(), move_assigned_value.Hash());
    move_assigned_value = std::move(copy_assigned_value);
    EXPECT_EQ(copy_constructed_value, move_assigned_value);
    EXPECT_EQ(copy_constructed_value.Hash(), move_assigned_value.Hash());
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, BigIntTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<int64_t>(std::uniform_int_distribution<int64_t>(INT64_MIN, INT64_MAX)(generator_));

    auto value = type::TransientValueFactory::GetBigInt(data);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::TransientValuePeeker::PeekBigInt(value));
    EXPECT_EQ(value.ToString(), "BIGINT");

    auto null = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
    value.SetNull(null);
    EXPECT_EQ(null, value.Null());

    value.SetNull(false);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::TransientValuePeeker::PeekBigInt(value));

    auto copy_constructed_value(value);
    EXPECT_EQ(value, copy_constructed_value);
    EXPECT_EQ(value.Hash(), copy_constructed_value.Hash());
    auto copy_assigned_value = type::TransientValueFactory::GetBoolean(true);
    EXPECT_NE(value, copy_assigned_value);
    EXPECT_NE(value.Hash(), copy_assigned_value.Hash());
    copy_assigned_value = value;
    EXPECT_EQ(value, copy_assigned_value);
    EXPECT_EQ(value.Hash(), copy_assigned_value.Hash());

    auto move_constructed_value(std::move(value));
    EXPECT_EQ(copy_assigned_value, move_constructed_value);
    EXPECT_EQ(copy_assigned_value.Hash(), move_constructed_value.Hash());
    auto move_assigned_value = type::TransientValueFactory::GetBoolean(true);
    EXPECT_NE(copy_assigned_value, move_assigned_value);
    EXPECT_NE(copy_assigned_value.Hash(), move_assigned_value.Hash());
    move_assigned_value = std::move(copy_assigned_value);
    EXPECT_EQ(copy_constructed_value, move_assigned_value);
    EXPECT_EQ(copy_constructed_value.Hash(), move_assigned_value.Hash());
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, DecimalTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = std::uniform_real_distribution<double>(DBL_MIN, DBL_MAX)(generator_);

    auto value = type::TransientValueFactory::GetDecimal(data);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::TransientValuePeeker::PeekDecimal(value));
    EXPECT_EQ(value.ToString(), "DECIMAL");

    auto null = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
    value.SetNull(null);
    EXPECT_EQ(null, value.Null());

    value.SetNull(false);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::TransientValuePeeker::PeekDecimal(value));

    auto copy_constructed_value(value);
    EXPECT_EQ(value, copy_constructed_value);
    EXPECT_EQ(value.Hash(), copy_constructed_value.Hash());
    auto copy_assigned_value = type::TransientValueFactory::GetBoolean(true);
    EXPECT_NE(value, copy_assigned_value);
    EXPECT_NE(value.Hash(), copy_assigned_value.Hash());
    copy_assigned_value = value;
    EXPECT_EQ(value, copy_assigned_value);
    EXPECT_EQ(value.Hash(), copy_assigned_value.Hash());

    auto move_constructed_value(std::move(value));
    EXPECT_EQ(copy_assigned_value, move_constructed_value);
    EXPECT_EQ(copy_assigned_value.Hash(), move_constructed_value.Hash());
    auto move_assigned_value = type::TransientValueFactory::GetBoolean(true);
    EXPECT_NE(copy_assigned_value, move_assigned_value);
    EXPECT_NE(copy_assigned_value.Hash(), move_assigned_value.Hash());
    move_assigned_value = std::move(copy_assigned_value);
    EXPECT_EQ(copy_constructed_value, move_assigned_value);
    EXPECT_EQ(copy_constructed_value.Hash(), move_assigned_value.Hash());
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, TimestampTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<type::timestamp_t>(std::uniform_int_distribution<uint64_t>(0, UINT64_MAX)(generator_));

    auto value = type::TransientValueFactory::GetTimestamp(data);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::TransientValuePeeker::PeekTimestamp(value));
    EXPECT_EQ(value.ToString(), "TIMESTAMP");

    auto null = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
    value.SetNull(null);
    EXPECT_EQ(null, value.Null());

    value.SetNull(false);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::TransientValuePeeker::PeekTimestamp(value));

    auto copy_constructed_value(value);
    EXPECT_EQ(value, copy_constructed_value);
    EXPECT_EQ(value.Hash(), copy_constructed_value.Hash());
    auto copy_assigned_value = type::TransientValueFactory::GetBoolean(true);
    EXPECT_NE(value, copy_assigned_value);
    EXPECT_NE(value.Hash(), copy_assigned_value.Hash());
    copy_assigned_value = value;
    EXPECT_EQ(value, copy_assigned_value);
    EXPECT_EQ(value.Hash(), copy_assigned_value.Hash());

    auto move_constructed_value(std::move(value));
    EXPECT_EQ(copy_assigned_value, move_constructed_value);
    EXPECT_EQ(copy_assigned_value.Hash(), move_constructed_value.Hash());
    auto move_assigned_value = type::TransientValueFactory::GetBoolean(true);
    EXPECT_NE(copy_assigned_value, move_assigned_value);
    EXPECT_NE(copy_assigned_value.Hash(), move_assigned_value.Hash());
    move_assigned_value = std::move(copy_assigned_value);
    EXPECT_EQ(copy_constructed_value, move_assigned_value);
    EXPECT_EQ(copy_constructed_value.Hash(), move_assigned_value.Hash());
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, DateTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<type::date_t>(std::uniform_int_distribution<uint32_t>(0, UINT32_MAX)(generator_));

    auto value = type::TransientValueFactory::GetDate(data);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::TransientValuePeeker::PeekDate(value));
    EXPECT_EQ(value.ToString(), "DATE");

    auto null = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
    value.SetNull(null);
    EXPECT_EQ(null, value.Null());

    value.SetNull(false);
    EXPECT_FALSE(value.Null());
    EXPECT_EQ(data, type::TransientValuePeeker::PeekDate(value));

    auto copy_constructed_value(value);
    EXPECT_EQ(value, copy_constructed_value);
    EXPECT_EQ(value.Hash(), copy_constructed_value.Hash());
    auto copy_assigned_value = type::TransientValueFactory::GetBoolean(true);
    EXPECT_NE(value, copy_assigned_value);
    EXPECT_NE(value.Hash(), copy_assigned_value.Hash());
    copy_assigned_value = value;
    EXPECT_EQ(value, copy_assigned_value);
    EXPECT_EQ(value.Hash(), copy_assigned_value.Hash());

    auto move_constructed_value(std::move(value));
    EXPECT_EQ(copy_assigned_value, move_constructed_value);
    EXPECT_EQ(copy_assigned_value.Hash(), move_constructed_value.Hash());
    auto move_assigned_value = type::TransientValueFactory::GetBoolean(true);
    EXPECT_NE(copy_assigned_value, move_assigned_value);
    EXPECT_NE(copy_assigned_value.Hash(), move_assigned_value.Hash());
    move_assigned_value = std::move(copy_assigned_value);
    EXPECT_EQ(copy_constructed_value, move_assigned_value);
    EXPECT_EQ(copy_constructed_value.Hash(), move_assigned_value.Hash());
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, VarCharTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto length = std::uniform_int_distribution<uint32_t>(1, UINT8_MAX)(generator_);
    auto *const data = new char[length];
    for (uint32_t j = 0; j < length - 1; j++) {
      data[j] = std::uniform_int_distribution<char>('A', 'z')(generator_);
    }
    data[length - 1] = '\0';  // null terminate the c-string

    auto value = type::TransientValueFactory::GetVarChar(data);
    EXPECT_FALSE(value.Null());
    std::string_view string_view = type::TransientValuePeeker::PeekVarChar(value);
    EXPECT_EQ(data, string_view);
    EXPECT_EQ(value.ToString(), "VARCHAR");

    auto null = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
    value.SetNull(null);
    EXPECT_EQ(null, value.Null());

    value.SetNull(false);
    EXPECT_FALSE(value.Null());
    string_view = type::TransientValuePeeker::PeekVarChar(value);
    EXPECT_EQ(data, string_view);
    delete[] data;

    auto copy_constructed_value(value);
    EXPECT_EQ(value, copy_constructed_value);
    EXPECT_EQ(value.Hash(), copy_constructed_value.Hash());
    auto copy_assigned_value = type::TransientValueFactory::GetBoolean(true);
    EXPECT_NE(value, copy_assigned_value);
    EXPECT_NE(value.Hash(), copy_assigned_value.Hash());
    copy_assigned_value = value;
    EXPECT_EQ(value, copy_assigned_value);
    EXPECT_EQ(value.Hash(), copy_assigned_value.Hash());

    auto move_constructed_value(std::move(value));
    EXPECT_EQ(copy_assigned_value, move_constructed_value);
    EXPECT_EQ(copy_assigned_value.Hash(), move_constructed_value.Hash());
    auto move_assigned_value = type::TransientValueFactory::GetBoolean(true);
    EXPECT_NE(copy_assigned_value, move_assigned_value);
    EXPECT_NE(copy_assigned_value.Hash(), move_assigned_value.Hash());
    move_assigned_value = std::move(copy_assigned_value);
    EXPECT_EQ(copy_constructed_value, move_assigned_value);
    EXPECT_EQ(copy_constructed_value.Hash(), move_assigned_value.Hash());
  }
}

// NOLINTNEXTLINE
TEST_F(ValueTests, BooleanJsonTest) {
  auto data = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));

  auto value = type::TransientValueFactory::GetBoolean(data);
  EXPECT_FALSE(value.Null());

  auto json = value.ToJson();
  EXPECT_FALSE(json.is_null());

  TransientValue deserialized_value;
  deserialized_value.FromJson(json);

  EXPECT_EQ(value, deserialized_value);
}

// NOLINTNEXTLINE
TEST_F(ValueTests, TinyIntJsonTest) {
  auto data = static_cast<int8_t>(std::uniform_int_distribution<int8_t>(INT8_MIN, INT8_MAX)(generator_));

  auto value = type::TransientValueFactory::GetTinyInt(data);
  EXPECT_FALSE(value.Null());

  auto json = value.ToJson();
  EXPECT_FALSE(json.is_null());

  TransientValue deserialized_value;
  deserialized_value.FromJson(json);

  EXPECT_EQ(value, deserialized_value);
}

// NOLINTNEXTLINE
TEST_F(ValueTests, SmallIntJsonTest) {
  auto data = static_cast<int16_t>(std::uniform_int_distribution<int16_t>(INT16_MIN, INT16_MAX)(generator_));

  auto value = type::TransientValueFactory::GetSmallInt(data);
  EXPECT_FALSE(value.Null());

  auto json = value.ToJson();
  EXPECT_FALSE(json.is_null());

  TransientValue deserialized_value;
  deserialized_value.FromJson(json);

  EXPECT_EQ(value, deserialized_value);
}

// NOLINTNEXTLINE
TEST_F(ValueTests, IntegerJsonTest) {
  auto data = static_cast<int32_t>(std::uniform_int_distribution<int32_t>(INT32_MIN, INT32_MAX)(generator_));

  auto value = type::TransientValueFactory::GetInteger(data);
  EXPECT_FALSE(value.Null());

  auto json = value.ToJson();
  EXPECT_FALSE(json.is_null());

  TransientValue deserialized_value;
  deserialized_value.FromJson(json);

  EXPECT_EQ(value, deserialized_value);
}

// NOLINTNEXTLINE
TEST_F(ValueTests, BigIntJsonTest) {
  auto data = static_cast<int64_t>(std::uniform_int_distribution<int64_t>(INT64_MIN, INT64_MAX)(generator_));

  auto value = type::TransientValueFactory::GetBigInt(data);
  EXPECT_FALSE(value.Null());

  auto json = value.ToJson();
  EXPECT_FALSE(json.is_null());

  TransientValue deserialized_value;
  deserialized_value.FromJson(json);

  EXPECT_EQ(value, deserialized_value);
}

// NOLINTNEXTLINE
TEST_F(ValueTests, DecimalJsonTest) {
  auto data = std::uniform_real_distribution<double>(DBL_MIN, DBL_MAX)(generator_);

  auto value = type::TransientValueFactory::GetDecimal(data);
  EXPECT_FALSE(value.Null());

  auto json = value.ToJson();
  EXPECT_FALSE(json.is_null());

  TransientValue deserialized_value;
  deserialized_value.FromJson(json);

  EXPECT_EQ(value, deserialized_value);
}

// NOLINTNEXTLINE
TEST_F(ValueTests, TimestampJsonTest) {
  auto data = static_cast<type::timestamp_t>(std::uniform_int_distribution<uint64_t>(0, UINT64_MAX)(generator_));

  auto value = type::TransientValueFactory::GetTimestamp(data);
  EXPECT_FALSE(value.Null());

  auto json = value.ToJson();
  EXPECT_FALSE(json.is_null());

  TransientValue deserialized_value;
  deserialized_value.FromJson(json);

  EXPECT_EQ(value, deserialized_value);
}

// NOLINTNEXTLINE
TEST_F(ValueTests, DateJsonTest) {
  auto data = static_cast<type::date_t>(std::uniform_int_distribution<uint32_t>(0, UINT32_MAX)(generator_));

  auto value = type::TransientValueFactory::GetDate(data);
  EXPECT_FALSE(value.Null());

  auto json = value.ToJson();
  EXPECT_FALSE(json.is_null());

  TransientValue deserialized_value;
  deserialized_value.FromJson(json);

  EXPECT_EQ(value, deserialized_value);
}

// NOLINTNEXTLINE
TEST_F(ValueTests, VarCharJsonTest) {
  auto length = std::uniform_int_distribution<uint32_t>(1, UINT8_MAX)(generator_);
  auto *const data = new char[length];
  for (uint32_t j = 0; j < length - 1; j++) {
    data[j] = std::uniform_int_distribution<char>('A', 'z')(generator_);
  }
  data[length - 1] = '\0';  // null terminate the c-string

  TransientValue value = type::TransientValueFactory::GetVarChar(data);
  EXPECT_FALSE(value.Null());

  auto json = value.ToJson();
  EXPECT_FALSE(json.is_null());

  TransientValue deserialized_value;
  deserialized_value.FromJson(json);

  EXPECT_EQ(value, deserialized_value);
  delete[] data;
}

}  // namespace terrier::type
