#include "execution/sql/data_types.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::sql::test {

class DataTypesTests : public TplTest {};

#define CHECK_TYPE_PROPERTIES(TYPE, TYPE_ID, IS_ARITHMETIC) \
  const auto &type = TYPE::Instance(true);                  \
  EXPECT_TRUE(type.IsNullable());                           \
  EXPECT_EQ(TYPE_ID, type.GetId());                         \
  EXPECT_EQ(IS_ARITHMETIC, type.IsArithmetic());            \
  EXPECT_TRUE(type.Equals(TYPE::InstanceNullable()));       \
  EXPECT_FALSE(type.Equals(TYPE::InstanceNonNullable()));

#define CHECK_NOT_EQUAL(INSTANCE, OTHER_TYPE)                \
  EXPECT_FALSE(INSTANCE.Equals(OTHER_TYPE::Instance(true))); \
  EXPECT_FALSE(INSTANCE.Equals(OTHER_TYPE::Instance(false)));

#define CHECK_NOT_EQUAL_ALT(INSTANCE, OTHER_TYPE, ...)                    \
  EXPECT_FALSE(INSTANCE.Equals(OTHER_TYPE::Instance(true, __VA_ARGS__))); \
  EXPECT_FALSE(INSTANCE.Equals(OTHER_TYPE::Instance(false, __VA_ARGS__)));

// NOLINTNEXTLINE
TEST_F(DataTypesTests, BooleanType) {
  CHECK_TYPE_PROPERTIES(BooleanType, SqlTypeId::Boolean, false);
  CHECK_NOT_EQUAL(type, SmallIntType);
  CHECK_NOT_EQUAL(type, IntegerType);
  CHECK_NOT_EQUAL(type, BigIntType);
  CHECK_NOT_EQUAL(type, RealType);
  CHECK_NOT_EQUAL(type, DateType);
  CHECK_NOT_EQUAL(type, RealType);
  CHECK_NOT_EQUAL(type, DoubleType);
}

// NOLINTNEXTLINE
TEST_F(DataTypesTests, TinyIntType) {
  CHECK_TYPE_PROPERTIES(TinyIntType, SqlTypeId::TinyInt, true);
  CHECK_NOT_EQUAL(type, BooleanType);
  CHECK_NOT_EQUAL(type, IntegerType);
  CHECK_NOT_EQUAL(type, BigIntType);
  CHECK_NOT_EQUAL(type, RealType);
  CHECK_NOT_EQUAL(type, DateType);
  CHECK_NOT_EQUAL(type, RealType);
  CHECK_NOT_EQUAL(type, DoubleType);
}

// NOLINTNEXTLINE
TEST_F(DataTypesTests, SmallIntType) {
  CHECK_TYPE_PROPERTIES(SmallIntType, SqlTypeId::SmallInt, true);
  CHECK_NOT_EQUAL(type, BooleanType);
  CHECK_NOT_EQUAL(type, IntegerType);
  CHECK_NOT_EQUAL(type, BigIntType);
  CHECK_NOT_EQUAL(type, RealType);
  CHECK_NOT_EQUAL(type, DateType);
  CHECK_NOT_EQUAL(type, RealType);
  CHECK_NOT_EQUAL(type, DoubleType);
}

// NOLINTNEXTLINE
TEST_F(DataTypesTests, IntegerType) {
  CHECK_TYPE_PROPERTIES(IntegerType, SqlTypeId::Integer, true);
  CHECK_NOT_EQUAL(type, BooleanType);
  CHECK_NOT_EQUAL(type, SmallIntType);
  CHECK_NOT_EQUAL(type, BigIntType);
  CHECK_NOT_EQUAL(type, RealType);
  CHECK_NOT_EQUAL(type, DateType);
  CHECK_NOT_EQUAL(type, RealType);
  CHECK_NOT_EQUAL(type, DoubleType);
}

// NOLINTNEXTLINE
TEST_F(DataTypesTests, BigIntType) {
  CHECK_TYPE_PROPERTIES(BigIntType, SqlTypeId::BigInt, true);
  CHECK_NOT_EQUAL(type, BooleanType);
  CHECK_NOT_EQUAL(type, SmallIntType);
  CHECK_NOT_EQUAL(type, IntegerType);
  CHECK_NOT_EQUAL(type, RealType);
  CHECK_NOT_EQUAL(type, DateType);
  CHECK_NOT_EQUAL(type, RealType);
  CHECK_NOT_EQUAL(type, DoubleType);
}

// NOLINTNEXTLINE
TEST_F(DataTypesTests, RealType) {
  CHECK_TYPE_PROPERTIES(RealType, SqlTypeId::Real, true);
  CHECK_NOT_EQUAL(type, BooleanType);
  CHECK_NOT_EQUAL(type, SmallIntType);
  CHECK_NOT_EQUAL(type, IntegerType);
  CHECK_NOT_EQUAL(type, BigIntType);
  CHECK_NOT_EQUAL(type, DateType);
  CHECK_NOT_EQUAL(type, DoubleType);
}

// NOLINTNEXTLINE
TEST_F(DataTypesTests, DoubleType) {
  CHECK_TYPE_PROPERTIES(DoubleType, SqlTypeId::Double, true);
  CHECK_NOT_EQUAL(type, BooleanType);
  CHECK_NOT_EQUAL(type, SmallIntType);
  CHECK_NOT_EQUAL(type, IntegerType);
  CHECK_NOT_EQUAL(type, BigIntType);
  CHECK_NOT_EQUAL(type, DateType);
  CHECK_NOT_EQUAL(type, RealType);
}

// NOLINTNEXTLINE
TEST_F(DataTypesTests, DateType) {
  CHECK_TYPE_PROPERTIES(DateType, SqlTypeId::Date, false);
  CHECK_NOT_EQUAL(type, BooleanType);
  CHECK_NOT_EQUAL(type, SmallIntType);
  CHECK_NOT_EQUAL(type, IntegerType);
  CHECK_NOT_EQUAL(type, BigIntType);
  CHECK_NOT_EQUAL(type, RealType);
}

// NOLINTNEXTLINE
TEST_F(DataTypesTests, DecimalType) {
  const auto &type1 = DecimalType::InstanceNullable(5, 2);
  EXPECT_TRUE(type1.IsNullable());
  EXPECT_EQ(SqlTypeId::Decimal, type1.GetId());
  EXPECT_EQ(5u, type1.Precision());
  EXPECT_EQ(2u, type1.Scale());
  EXPECT_TRUE(type1.IsArithmetic());

  CHECK_NOT_EQUAL(type1, BooleanType);
  CHECK_NOT_EQUAL(type1, SmallIntType);
  CHECK_NOT_EQUAL(type1, IntegerType);
  CHECK_NOT_EQUAL(type1, BigIntType);
  CHECK_NOT_EQUAL(type1, RealType);
  CHECK_NOT_EQUAL(type1, RealType);
  CHECK_NOT_EQUAL(type1, DoubleType);
  CHECK_NOT_EQUAL_ALT(type1, DecimalType, 3, 4);

  EXPECT_FALSE(type1.Equals(DecimalType::InstanceNonNullable(5, 2)));
  EXPECT_TRUE(type1.Equals(DecimalType::InstanceNullable(5, 2)));
}

// NOLINTNEXTLINE
TEST_F(DataTypesTests, CharType) {
  const auto &type1 = CharType::InstanceNullable(100);
  EXPECT_TRUE(type1.IsNullable());
  EXPECT_EQ(SqlTypeId::Char, type1.GetId());
  EXPECT_EQ(100u, type1.Length());
  EXPECT_FALSE(type1.IsArithmetic());

  CHECK_NOT_EQUAL(type1, BooleanType);
  CHECK_NOT_EQUAL(type1, SmallIntType);
  CHECK_NOT_EQUAL(type1, IntegerType);
  CHECK_NOT_EQUAL(type1, BigIntType);
  CHECK_NOT_EQUAL(type1, RealType);
  CHECK_NOT_EQUAL(type1, DateType);
  CHECK_NOT_EQUAL(type1, RealType);
  CHECK_NOT_EQUAL(type1, DoubleType);
  CHECK_NOT_EQUAL_ALT(type1, DecimalType, 5, 2);
  CHECK_NOT_EQUAL_ALT(type1, CharType, 200);
  CHECK_NOT_EQUAL_ALT(type1, VarcharType, 200);

  EXPECT_TRUE(type1.Equals(CharType::InstanceNullable(100)));
}

// NOLINTNEXTLINE
TEST_F(DataTypesTests, VarcharType) {
  const auto &type1 = VarcharType::InstanceNullable(100);
  EXPECT_TRUE(type1.IsNullable());
  EXPECT_EQ(SqlTypeId::Varchar, type1.GetId());
  EXPECT_EQ(100u, type1.MaxLength());
  EXPECT_FALSE(type1.IsArithmetic());

  CHECK_NOT_EQUAL(type1, BooleanType);
  CHECK_NOT_EQUAL(type1, SmallIntType);
  CHECK_NOT_EQUAL(type1, IntegerType);
  CHECK_NOT_EQUAL(type1, BigIntType);
  CHECK_NOT_EQUAL(type1, RealType);
  CHECK_NOT_EQUAL(type1, DateType);
  CHECK_NOT_EQUAL(type1, RealType);
  CHECK_NOT_EQUAL(type1, DoubleType);
  CHECK_NOT_EQUAL_ALT(type1, DecimalType, 5, 2);
  CHECK_NOT_EQUAL_ALT(type1, VarcharType, 200);

  EXPECT_TRUE(type1.Equals(VarcharType::InstanceNullable(100)));
}

}  // namespace noisepage::execution::sql::test
