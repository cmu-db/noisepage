#include "binder/binder_util.h"

#include <memory>

#include "common/managed_pointer.h"
#include "execution/sql/value_util.h"
#include "parser/expression/constant_value_expression.h"
#include "test_util/test_harness.h"

namespace terrier {
class BinderUtilTest : public TerrierTest {};

// NOLINTNEXTLINE
TEST_F(BinderUtilTest, VarcharToTinyInt) {
  const std::string val = "15";
  auto string_val = execution::sql::ValueUtil::CreateStringVal(val);
  const auto cve = std::make_unique<parser::ConstantValueExpression>(type::TypeId::VARCHAR, string_val.first,
                                                                     std::move(string_val.second));
  binder::BinderUtil::CheckAndTryPromoteType(common::ManagedPointer(cve), type::TypeId::TINYINT);
  EXPECT_EQ(cve->Peek<int8_t>(), 15);
}

// NOLINTNEXTLINE
TEST_F(BinderUtilTest, VarcharToSmallInt) {
  const std::string val = "15721";
  auto string_val = execution::sql::ValueUtil::CreateStringVal(val);
  const auto cve = std::make_unique<parser::ConstantValueExpression>(type::TypeId::VARCHAR, string_val.first,
                                                                     std::move(string_val.second));
  binder::BinderUtil::CheckAndTryPromoteType(common::ManagedPointer(cve), type::TypeId::SMALLINT);
  EXPECT_EQ(cve->Peek<int16_t>(), 15721);
}

// NOLINTNEXTLINE
TEST_F(BinderUtilTest, VarcharToInteger) {
  const std::string val = "1572115445";
  auto string_val = execution::sql::ValueUtil::CreateStringVal(val);
  const auto cve = std::make_unique<parser::ConstantValueExpression>(type::TypeId::VARCHAR, string_val.first,
                                                                     std::move(string_val.second));
  binder::BinderUtil::CheckAndTryPromoteType(common::ManagedPointer(cve), type::TypeId::INTEGER);
  EXPECT_EQ(cve->Peek<int32_t>(), 1572115445);
}

// NOLINTNEXTLINE
TEST_F(BinderUtilTest, VarcharToBigInt) {
  const std::string val = "157210154450";
  auto string_val = execution::sql::ValueUtil::CreateStringVal(val);
  const auto cve = std::make_unique<parser::ConstantValueExpression>(type::TypeId::VARCHAR, string_val.first,
                                                                     std::move(string_val.second));
  binder::BinderUtil::CheckAndTryPromoteType(common::ManagedPointer(cve), type::TypeId::BIGINT);
  EXPECT_EQ(cve->Peek<int64_t>(), 157210154450);
}

// NOLINTNEXTLINE
TEST_F(BinderUtilTest, VarcharToDecimal) {
  const std::string val = "15721.15445";
  auto string_val = execution::sql::ValueUtil::CreateStringVal(val);
  const auto cve = std::make_unique<parser::ConstantValueExpression>(type::TypeId::VARCHAR, string_val.first,
                                                                     std::move(string_val.second));
  binder::BinderUtil::CheckAndTryPromoteType(common::ManagedPointer(cve), type::TypeId::DECIMAL);
  EXPECT_EQ(cve->Peek<double>(), 15721.15445);
}
}  // namespace terrier
