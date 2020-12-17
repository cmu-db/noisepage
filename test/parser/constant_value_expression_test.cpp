#include "parser/expression/constant_value_expression.h"

#include <random>
#include <utility>

#include "common/json.h"
#include "execution/sql/value_util.h"
#include "test_util/test_harness.h"

namespace noisepage::parser {

class CVETests : public TerrierTest {
 public:
  std::default_random_engine generator_;
  const uint32_t num_iterations_ = 10000;
};

/**
 * These tests all follow the same basic structure:
 * 1) Randomly generate reference C type data
 * 2) Create a ConstantValueExpression from this C type
 * 3) Assert that the new ConstantValueExpression is not NULL
 * 4) Assert that the C type returned by value matches the reference C type data
 * 5) Test copy constructor
 * 6) Test copy assignment operator
 * 7) Test move constructor
 * 8) Test move assignment operator
 */

// NOLINTNEXTLINE
TEST_F(CVETests, BooleanTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));

    ConstantValueExpression value(type::TypeId::BOOLEAN, execution::sql::BoolVal(data));
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, value.Peek<bool>());

    auto copy_constructed_value(value);
    EXPECT_EQ(value, copy_constructed_value);
    EXPECT_EQ(value.Hash(), copy_constructed_value.Hash());
    ConstantValueExpression copy_assigned_value(type::TypeId::BOOLEAN, execution::sql::BoolVal(!data));
    EXPECT_NE(value, copy_assigned_value);
    EXPECT_NE(value.Hash(), copy_assigned_value.Hash());
    copy_assigned_value = value;
    EXPECT_EQ(value, copy_assigned_value);
    EXPECT_EQ(value.Hash(), copy_assigned_value.Hash());

    auto move_constructed_value(std::move(value));
    EXPECT_EQ(copy_assigned_value, move_constructed_value);
    EXPECT_EQ(copy_assigned_value.Hash(), move_constructed_value.Hash());
    ConstantValueExpression move_assigned_value(type::TypeId::BOOLEAN, execution::sql::BoolVal(!data));
    EXPECT_NE(copy_assigned_value, move_assigned_value);
    EXPECT_NE(copy_assigned_value.Hash(), move_assigned_value.Hash());
    move_assigned_value = std::move(copy_assigned_value);
    EXPECT_EQ(copy_constructed_value, move_assigned_value);
    EXPECT_EQ(copy_constructed_value.Hash(), move_assigned_value.Hash());
  }
}

// NOLINTNEXTLINE
TEST_F(CVETests, TinyIntTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<int8_t>(std::uniform_int_distribution<int8_t>(INT8_MIN, INT8_MAX)(generator_));

    ConstantValueExpression value(type::TypeId::TINYINT, execution::sql::Integer(data));
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, value.Peek<int8_t>());

    auto copy_constructed_value(value);
    EXPECT_EQ(value, copy_constructed_value);
    EXPECT_EQ(value.Hash(), copy_constructed_value.Hash());
    ConstantValueExpression copy_assigned_value(type::TypeId::BOOLEAN);
    EXPECT_NE(value, copy_assigned_value);
    EXPECT_NE(value.Hash(), copy_assigned_value.Hash());
    copy_assigned_value = value;
    EXPECT_EQ(value, copy_assigned_value);
    EXPECT_EQ(value.Hash(), copy_assigned_value.Hash());

    auto move_constructed_value(std::move(value));
    EXPECT_EQ(copy_assigned_value, move_constructed_value);
    EXPECT_EQ(copy_assigned_value.Hash(), move_constructed_value.Hash());
    ConstantValueExpression move_assigned_value(type::TypeId::BOOLEAN);
    EXPECT_NE(copy_assigned_value, move_assigned_value);
    EXPECT_NE(copy_assigned_value.Hash(), move_assigned_value.Hash());
    move_assigned_value = std::move(copy_assigned_value);
    EXPECT_EQ(copy_constructed_value, move_assigned_value);
    EXPECT_EQ(copy_constructed_value.Hash(), move_assigned_value.Hash());
  }
}

// NOLINTNEXTLINE
TEST_F(CVETests, SmallIntTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<int16_t>(std::uniform_int_distribution<int16_t>(INT16_MIN, INT16_MAX)(generator_));

    ConstantValueExpression value(type::TypeId::SMALLINT, execution::sql::Integer(data));
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, value.Peek<int16_t>());

    auto copy_constructed_value(value);
    EXPECT_EQ(value, copy_constructed_value);
    EXPECT_EQ(value.Hash(), copy_constructed_value.Hash());
    ConstantValueExpression copy_assigned_value(type::TypeId::BOOLEAN);
    EXPECT_NE(value, copy_assigned_value);
    EXPECT_NE(value.Hash(), copy_assigned_value.Hash());
    copy_assigned_value = value;
    EXPECT_EQ(value, copy_assigned_value);
    EXPECT_EQ(value.Hash(), copy_assigned_value.Hash());

    auto move_constructed_value(std::move(value));
    EXPECT_EQ(copy_assigned_value, move_constructed_value);
    EXPECT_EQ(copy_assigned_value.Hash(), move_constructed_value.Hash());
    ConstantValueExpression move_assigned_value(type::TypeId::BOOLEAN);
    EXPECT_NE(copy_assigned_value, move_assigned_value);
    EXPECT_NE(copy_assigned_value.Hash(), move_assigned_value.Hash());
    move_assigned_value = std::move(copy_assigned_value);
    EXPECT_EQ(copy_constructed_value, move_assigned_value);
    EXPECT_EQ(copy_constructed_value.Hash(), move_assigned_value.Hash());
  }
}

// NOLINTNEXTLINE
TEST_F(CVETests, IntegerTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<int32_t>(std::uniform_int_distribution<int32_t>(INT32_MIN, INT32_MAX)(generator_));

    ConstantValueExpression value(type::TypeId::INTEGER, execution::sql::Integer(data));
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, value.Peek<int32_t>());

    auto copy_constructed_value(value);
    EXPECT_EQ(value, copy_constructed_value);
    EXPECT_EQ(value.Hash(), copy_constructed_value.Hash());
    ConstantValueExpression copy_assigned_value(type::TypeId::BOOLEAN);
    EXPECT_NE(value, copy_assigned_value);
    EXPECT_NE(value.Hash(), copy_assigned_value.Hash());
    copy_assigned_value = value;
    EXPECT_EQ(value, copy_assigned_value);
    EXPECT_EQ(value.Hash(), copy_assigned_value.Hash());

    auto move_constructed_value(std::move(value));
    EXPECT_EQ(copy_assigned_value, move_constructed_value);
    EXPECT_EQ(copy_assigned_value.Hash(), move_constructed_value.Hash());
    ConstantValueExpression move_assigned_value(type::TypeId::BOOLEAN);
    EXPECT_NE(copy_assigned_value, move_assigned_value);
    EXPECT_NE(copy_assigned_value.Hash(), move_assigned_value.Hash());
    move_assigned_value = std::move(copy_assigned_value);
    EXPECT_EQ(copy_constructed_value, move_assigned_value);
    EXPECT_EQ(copy_constructed_value.Hash(), move_assigned_value.Hash());
  }
}

// NOLINTNEXTLINE
TEST_F(CVETests, BigIntTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<int64_t>(std::uniform_int_distribution<int64_t>(INT64_MIN, INT64_MAX)(generator_));

    ConstantValueExpression value(type::TypeId::BIGINT, execution::sql::Integer(data));
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, value.Peek<int64_t>());

    auto copy_constructed_value(value);
    EXPECT_EQ(value, copy_constructed_value);
    EXPECT_EQ(value.Hash(), copy_constructed_value.Hash());
    ConstantValueExpression copy_assigned_value(type::TypeId::BOOLEAN);
    EXPECT_NE(value, copy_assigned_value);
    EXPECT_NE(value.Hash(), copy_assigned_value.Hash());
    copy_assigned_value = value;
    EXPECT_EQ(value, copy_assigned_value);
    EXPECT_EQ(value.Hash(), copy_assigned_value.Hash());

    auto move_constructed_value(std::move(value));
    EXPECT_EQ(copy_assigned_value, move_constructed_value);
    EXPECT_EQ(copy_assigned_value.Hash(), move_constructed_value.Hash());
    ConstantValueExpression move_assigned_value(type::TypeId::BOOLEAN);
    EXPECT_NE(copy_assigned_value, move_assigned_value);
    EXPECT_NE(copy_assigned_value.Hash(), move_assigned_value.Hash());
    move_assigned_value = std::move(copy_assigned_value);
    EXPECT_EQ(copy_constructed_value, move_assigned_value);
    EXPECT_EQ(copy_constructed_value.Hash(), move_assigned_value.Hash());
  }
}

// NOLINTNEXTLINE
TEST_F(CVETests, DecimalTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = std::uniform_real_distribution<double>(DBL_MIN, DBL_MAX)(generator_);

    ConstantValueExpression value(type::TypeId::REAL, execution::sql::Real(data));
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, value.Peek<double>());

    auto copy_constructed_value(value);
    EXPECT_EQ(value, copy_constructed_value);
    EXPECT_EQ(value.Hash(), copy_constructed_value.Hash());
    ConstantValueExpression copy_assigned_value(type::TypeId::BOOLEAN);
    EXPECT_NE(value, copy_assigned_value);
    EXPECT_NE(value.Hash(), copy_assigned_value.Hash());
    copy_assigned_value = value;
    EXPECT_EQ(value, copy_assigned_value);
    EXPECT_EQ(value.Hash(), copy_assigned_value.Hash());

    auto move_constructed_value(std::move(value));
    EXPECT_EQ(copy_assigned_value, move_constructed_value);
    EXPECT_EQ(copy_assigned_value.Hash(), move_constructed_value.Hash());
    ConstantValueExpression move_assigned_value(type::TypeId::BOOLEAN);
    EXPECT_NE(copy_assigned_value, move_assigned_value);
    EXPECT_NE(copy_assigned_value.Hash(), move_assigned_value.Hash());
    move_assigned_value = std::move(copy_assigned_value);
    EXPECT_EQ(copy_constructed_value, move_assigned_value);
    EXPECT_EQ(copy_constructed_value.Hash(), move_assigned_value.Hash());
  }
}

// NOLINTNEXTLINE
TEST_F(CVETests, TimestampTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<uint64_t>(std::uniform_int_distribution<uint64_t>(0, UINT64_MAX)(generator_));

    ConstantValueExpression value(type::TypeId::TIMESTAMP, execution::sql::TimestampVal(data));
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, value.Peek<execution::sql::Timestamp>().ToNative());

    auto copy_constructed_value(value);
    EXPECT_EQ(value, copy_constructed_value);
    EXPECT_EQ(value.Hash(), copy_constructed_value.Hash());
    ConstantValueExpression copy_assigned_value(type::TypeId::BOOLEAN);
    EXPECT_NE(value, copy_assigned_value);
    EXPECT_NE(value.Hash(), copy_assigned_value.Hash());
    copy_assigned_value = value;
    EXPECT_EQ(value, copy_assigned_value);
    EXPECT_EQ(value.Hash(), copy_assigned_value.Hash());

    auto move_constructed_value(std::move(value));
    EXPECT_EQ(copy_assigned_value, move_constructed_value);
    EXPECT_EQ(copy_assigned_value.Hash(), move_constructed_value.Hash());
    ConstantValueExpression move_assigned_value(type::TypeId::BOOLEAN);
    EXPECT_NE(copy_assigned_value, move_assigned_value);
    EXPECT_NE(copy_assigned_value.Hash(), move_assigned_value.Hash());
    move_assigned_value = std::move(copy_assigned_value);
    EXPECT_EQ(copy_constructed_value, move_assigned_value);
    EXPECT_EQ(copy_constructed_value.Hash(), move_assigned_value.Hash());
  }
}

// NOLINTNEXTLINE
TEST_F(CVETests, DateTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto data = static_cast<uint32_t>(std::uniform_int_distribution<uint32_t>(0, UINT32_MAX)(generator_));

    ConstantValueExpression value(type::TypeId::DATE, execution::sql::DateVal(data));
    EXPECT_FALSE(value.IsNull());
    EXPECT_EQ(data, value.Peek<execution::sql::Date>().ToNative());

    auto copy_constructed_value(value);
    EXPECT_EQ(value, copy_constructed_value);
    EXPECT_EQ(value.Hash(), copy_constructed_value.Hash());
    ConstantValueExpression copy_assigned_value(type::TypeId::BOOLEAN);
    EXPECT_NE(value, copy_assigned_value);
    EXPECT_NE(value.Hash(), copy_assigned_value.Hash());
    copy_assigned_value = value;
    EXPECT_EQ(value, copy_assigned_value);
    EXPECT_EQ(value.Hash(), copy_assigned_value.Hash());

    auto move_constructed_value(std::move(value));
    EXPECT_EQ(copy_assigned_value, move_constructed_value);
    EXPECT_EQ(copy_assigned_value.Hash(), move_constructed_value.Hash());
    ConstantValueExpression move_assigned_value(type::TypeId::BOOLEAN);
    EXPECT_NE(copy_assigned_value, move_assigned_value);
    EXPECT_NE(copy_assigned_value.Hash(), move_assigned_value.Hash());
    move_assigned_value = std::move(copy_assigned_value);
    EXPECT_EQ(copy_constructed_value, move_assigned_value);
    EXPECT_EQ(copy_constructed_value.Hash(), move_assigned_value.Hash());
  }
}

// NOLINTNEXTLINE
TEST_F(CVETests, VarCharTest) {
  for (uint32_t i = 0; i < num_iterations_; i++) {
    auto length = std::uniform_int_distribution<uint32_t>(1, UINT8_MAX)(generator_);
    auto *const data = new char[length];
    for (uint32_t j = 0; j < length; j++) {
      data[j] = std::uniform_int_distribution<char>('A', 'z')(generator_);
    }

    auto string_val = execution::sql::ValueUtil::CreateStringVal(
        common::ManagedPointer(reinterpret_cast<const char *>(data)), length);
    ConstantValueExpression value(type::TypeId::VARCHAR, string_val.first, std::move(string_val.second));
    EXPECT_FALSE(value.IsNull());
    const auto string_view = value.Peek<std::string_view>();
    EXPECT_EQ(std::string_view(data, length), string_view);
    delete[] data;

    auto copy_constructed_value(value);
    EXPECT_EQ(value, copy_constructed_value);
    EXPECT_EQ(value.Hash(), copy_constructed_value.Hash());
    ConstantValueExpression copy_assigned_value(type::TypeId::BOOLEAN);
    EXPECT_NE(value, copy_assigned_value);
    EXPECT_NE(value.Hash(), copy_assigned_value.Hash());
    copy_assigned_value = value;
    EXPECT_EQ(value, copy_assigned_value);
    EXPECT_EQ(value.Hash(), copy_assigned_value.Hash());

    auto move_constructed_value(std::move(value));
    EXPECT_EQ(copy_assigned_value, move_constructed_value);
    EXPECT_EQ(copy_assigned_value.Hash(), move_constructed_value.Hash());
    ConstantValueExpression move_assigned_value(type::TypeId::BOOLEAN);
    EXPECT_NE(copy_assigned_value, move_assigned_value);
    EXPECT_NE(copy_assigned_value.Hash(), move_assigned_value.Hash());
    move_assigned_value = std::move(copy_assigned_value);
    EXPECT_EQ(copy_constructed_value, move_assigned_value);
    EXPECT_EQ(copy_constructed_value.Hash(), move_assigned_value.Hash());
  }
}

// NOLINTNEXTLINE
TEST_F(CVETests, BooleanJsonTest) {
  auto data = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));

  ConstantValueExpression value(type::TypeId::BOOLEAN, execution::sql::BoolVal(data));
  EXPECT_FALSE(value.IsNull());

  auto json = value.ToJson();
  EXPECT_FALSE(json.is_null());

  ConstantValueExpression deserialized_value;
  deserialized_value.FromJson(json);

  EXPECT_EQ(value, deserialized_value);
}

// NOLINTNEXTLINE
TEST_F(CVETests, TinyIntJsonTest) {
  auto data = static_cast<int8_t>(std::uniform_int_distribution<int8_t>(INT8_MIN, INT8_MAX)(generator_));

  ConstantValueExpression value(type::TypeId::TINYINT, execution::sql::Integer(data));
  EXPECT_FALSE(value.IsNull());

  auto json = value.ToJson();
  EXPECT_FALSE(json.is_null());

  ConstantValueExpression deserialized_value;
  deserialized_value.FromJson(json);

  EXPECT_EQ(value, deserialized_value);
}

// NOLINTNEXTLINE
TEST_F(CVETests, SmallIntJsonTest) {
  auto data = static_cast<int16_t>(std::uniform_int_distribution<int16_t>(INT16_MIN, INT16_MAX)(generator_));

  ConstantValueExpression value(type::TypeId::SMALLINT, execution::sql::Integer(data));
  EXPECT_FALSE(value.IsNull());

  auto json = value.ToJson();
  EXPECT_FALSE(json.is_null());

  ConstantValueExpression deserialized_value;
  deserialized_value.FromJson(json);

  EXPECT_EQ(value, deserialized_value);
}

// NOLINTNEXTLINE
TEST_F(CVETests, IntegerJsonTest) {
  auto data = static_cast<int32_t>(std::uniform_int_distribution<int32_t>(INT32_MIN, INT32_MAX)(generator_));

  ConstantValueExpression value(type::TypeId::INTEGER, execution::sql::Integer(data));
  EXPECT_FALSE(value.IsNull());

  auto json = value.ToJson();
  EXPECT_FALSE(json.is_null());

  ConstantValueExpression deserialized_value;
  deserialized_value.FromJson(json);

  EXPECT_EQ(value, deserialized_value);
}

// NOLINTNEXTLINE
TEST_F(CVETests, BigIntJsonTest) {
  auto data = static_cast<int64_t>(std::uniform_int_distribution<int64_t>(INT64_MIN, INT64_MAX)(generator_));

  ConstantValueExpression value(type::TypeId::BIGINT, execution::sql::Integer(data));
  EXPECT_FALSE(value.IsNull());

  auto json = value.ToJson();
  EXPECT_FALSE(json.is_null());

  ConstantValueExpression deserialized_value;
  deserialized_value.FromJson(json);

  EXPECT_EQ(value, deserialized_value);
}

// NOLINTNEXTLINE
TEST_F(CVETests, DecimalJsonTest) {
  auto data = std::uniform_real_distribution<double>(DBL_MIN, DBL_MAX)(generator_);

  ConstantValueExpression value(type::TypeId::REAL, execution::sql::Real(data));
  EXPECT_FALSE(value.IsNull());

  auto json = value.ToJson();
  EXPECT_FALSE(json.is_null());

  ConstantValueExpression deserialized_value;
  deserialized_value.FromJson(json);

  EXPECT_EQ(value, deserialized_value);
}

// NOLINTNEXTLINE
TEST_F(CVETests, TimestampJsonTest) {
  auto data = static_cast<uint64_t>(std::uniform_int_distribution<uint64_t>(0, UINT64_MAX)(generator_));

  ConstantValueExpression value(type::TypeId::TIMESTAMP, execution::sql::TimestampVal(data));
  EXPECT_FALSE(value.IsNull());

  auto json = value.ToJson();
  EXPECT_FALSE(json.is_null());

  ConstantValueExpression deserialized_value;
  deserialized_value.FromJson(json);

  EXPECT_EQ(value, deserialized_value);
}

// NOLINTNEXTLINE
TEST_F(CVETests, DateJsonTest) {
  auto data = static_cast<uint32_t>(std::uniform_int_distribution<uint32_t>(0, UINT32_MAX)(generator_));

  ConstantValueExpression value(type::TypeId::DATE, execution::sql::DateVal(data));
  EXPECT_FALSE(value.IsNull());

  auto json = value.ToJson();
  EXPECT_FALSE(json.is_null());

  ConstantValueExpression deserialized_value;
  deserialized_value.FromJson(json);

  EXPECT_EQ(value, deserialized_value);
}

// NOLINTNEXTLINE
TEST_F(CVETests, VarCharJsonTest) {
  auto length = std::uniform_int_distribution<uint32_t>(1, UINT8_MAX)(generator_);
  auto *const data = new char[length];
  for (uint32_t j = 0; j < length; j++) {
    data[j] = std::uniform_int_distribution<char>('A', 'z')(generator_);
  }

  auto string_val =
      execution::sql::ValueUtil::CreateStringVal(common::ManagedPointer(reinterpret_cast<const char *>(data)), length);
  ConstantValueExpression value(type::TypeId::VARCHAR, string_val.first, std::move(string_val.second));
  EXPECT_FALSE(value.IsNull());

  auto json = value.ToJson();
  EXPECT_FALSE(json.is_null());

  ConstantValueExpression deserialized_value;
  deserialized_value.FromJson(json);

  EXPECT_EQ(value, deserialized_value);
  delete[] data;
}

}  // namespace noisepage::parser
