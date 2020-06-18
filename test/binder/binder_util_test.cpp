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
  constexpr std::string_view val = "100";
  auto string_val = execution::sql::ValueUtil::CreateStringVal(val);
  const auto cve = std::make_unique<parser::ConstantValueExpression>(type::TypeId::VARCHAR, string_val.first,
                                                                     std::move(string_val.second));
  binder::BinderUtil::CheckAndTryPromoteType(common::ManagedPointer(cve), type::TypeId::TINYINT);
  EXPECT_EQ(cve->Peek<int8_t>(), 100);
}

// NOLINTNEXTLINE
TEST_F(BinderUtilTest, VarcharToSmallInt) {
  constexpr std::string_view val = "100";
  auto string_val = execution::sql::ValueUtil::CreateStringVal(val);
  const auto cve = std::make_unique<parser::ConstantValueExpression>(type::TypeId::VARCHAR, string_val.first,
                                                                     std::move(string_val.second));
  binder::BinderUtil::CheckAndTryPromoteType(common::ManagedPointer(cve), type::TypeId::SMALLINT);
  EXPECT_EQ(cve->Peek<int16_t>(), 100);
}

// NOLINTNEXTLINE
TEST_F(BinderUtilTest, VarcharToInteger) {
  constexpr std::string_view val = "100";
  auto string_val = execution::sql::ValueUtil::CreateStringVal(val);
  const auto cve = std::make_unique<parser::ConstantValueExpression>(type::TypeId::VARCHAR, string_val.first,
                                                                     std::move(string_val.second));
  binder::BinderUtil::CheckAndTryPromoteType(common::ManagedPointer(cve), type::TypeId::INTEGER);
  EXPECT_EQ(cve->Peek<int32_t>(), 100);
}

// NOLINTNEXTLINE
TEST_F(BinderUtilTest, VarcharToBigInt) {
  constexpr std::string_view val = "100";
  auto string_val = execution::sql::ValueUtil::CreateStringVal(val);
  const auto cve = std::make_unique<parser::ConstantValueExpression>(type::TypeId::VARCHAR, string_val.first,
                                                                     std::move(string_val.second));
  binder::BinderUtil::CheckAndTryPromoteType(common::ManagedPointer(cve), type::TypeId::BIGINT);
  EXPECT_EQ(cve->Peek<int64_t>(), 100);
}

// NOLINTNEXTLINE
TEST_F(BinderUtilTest, VarcharToDecimal) {
  constexpr std::string_view val = "100.0";
  auto string_val = execution::sql::ValueUtil::CreateStringVal(val);
  const auto cve = std::make_unique<parser::ConstantValueExpression>(type::TypeId::VARCHAR, string_val.first,
                                                                     std::move(string_val.second));
  binder::BinderUtil::CheckAndTryPromoteType(common::ManagedPointer(cve), type::TypeId::DECIMAL);
  EXPECT_EQ(cve->Peek<double>(), 100.0);
}
}  // namespace terrier
