#include "optimizer/statistics/value_condition.h"

#include <memory>
#include <string>
#include <utility>

#include "catalog/catalog_defs.h"
#include "execution/sql/value.h"
#include "gtest/gtest.h"
#include "parser/expression/constant_value_expression.h"
#include "type/type_id.h"

namespace noisepage::optimizer {
// NOLINTNEXTLINE
TEST(ValueConditionTests, GetColumnIDTest) {
  auto val = std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(1));
  ValueCondition v(catalog::col_oid_t(1), "", parser::ExpressionType::INVALID, std::move(val));

  EXPECT_EQ(catalog::col_oid_t(1), v.GetColumnID());
}

// NOLINTNEXTLINE
TEST(ValueConditionTests, GetColumnNameTest) {
  auto val = std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(1));
  ValueCondition v(catalog::col_oid_t(1), "", parser::ExpressionType::INVALID, std::move(val));

  EXPECT_EQ("", v.GetColumnName());
}

// NOLINTNEXTLINE
TEST(ValueConditionTests, GetTypeTest) {
  auto val = std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(1));
  ValueCondition v(catalog::col_oid_t(1), "", parser::ExpressionType::INVALID, std::move(val));

  EXPECT_EQ(parser::ExpressionType::INVALID, v.GetType());
}

// NOLINTNEXTLINE
TEST(ValueConditionTests, GetPointerToValueTest) {
  auto val = std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(1));
  ValueCondition v(catalog::col_oid_t(1), "", parser::ExpressionType::INVALID, std::move(val));

  EXPECT_EQ(parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(1)), *v.GetPointerToValue());
}
}  // namespace noisepage::optimizer
