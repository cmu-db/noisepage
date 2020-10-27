#include "type/type_util.h"

#include "gtest/gtest.h"
#include "type/type_id.h"

namespace noisepage::type {

// NOLINTNEXTLINE
TEST(TypeUtilTests, GetTypeSizeTest) {
  EXPECT_EQ(TypeUtil::GetTypeSize(TypeId::BOOLEAN), 1);
  EXPECT_EQ(TypeUtil::GetTypeSize(TypeId::TINYINT), 1);
  EXPECT_EQ(TypeUtil::GetTypeSize(TypeId::SMALLINT), 2);
  EXPECT_EQ(TypeUtil::GetTypeSize(TypeId::INTEGER), 4);
  EXPECT_EQ(TypeUtil::GetTypeSize(TypeId::DATE), 4);
  EXPECT_EQ(TypeUtil::GetTypeSize(TypeId::BIGINT), 8);
  EXPECT_EQ(TypeUtil::GetTypeSize(TypeId::DECIMAL), 8);
  EXPECT_EQ(TypeUtil::GetTypeSize(TypeId::TIMESTAMP), 8);
  EXPECT_EQ(TypeUtil::GetTypeSize(TypeId::VARCHAR), storage::VARLEN_COLUMN);
  EXPECT_EQ(TypeUtil::GetTypeSize(TypeId::VARBINARY), storage::VARLEN_COLUMN);

  // check to make sure that we throw an exception if we give GetTypeSize
  // an invalid TypeId that it handles it correctly
  EXPECT_THROW(TypeUtil::GetTypeSize(TypeId::INVALID), std::runtime_error);
}

// NOLINTNEXTLINE
TEST(TypeUtilTests, TypeIdToStringTest) {
  EXPECT_EQ(TypeUtil::TypeIdToString(TypeId::INVALID), "INVALID");
  EXPECT_EQ(TypeUtil::TypeIdToString(TypeId::BOOLEAN), "BOOLEAN");
  EXPECT_EQ(TypeUtil::TypeIdToString(TypeId::TINYINT), "TINYINT");
  EXPECT_EQ(TypeUtil::TypeIdToString(TypeId::SMALLINT), "SMALLINT");
  EXPECT_EQ(TypeUtil::TypeIdToString(TypeId::INTEGER), "INTEGER");
  EXPECT_EQ(TypeUtil::TypeIdToString(TypeId::BIGINT), "BIGINT");
  EXPECT_EQ(TypeUtil::TypeIdToString(TypeId::DECIMAL), "DECIMAL");
  EXPECT_EQ(TypeUtil::TypeIdToString(TypeId::TIMESTAMP), "TIMESTAMP");
  EXPECT_EQ(TypeUtil::TypeIdToString(TypeId::DATE), "DATE");
  EXPECT_EQ(TypeUtil::TypeIdToString(TypeId::VARCHAR), "VARCHAR");
  EXPECT_EQ(TypeUtil::TypeIdToString(TypeId::VARBINARY), "VARBINARY");
  EXPECT_THROW(TypeUtil::TypeIdToString(TypeId(12)), ConversionException);
}

}  // namespace noisepage::type
