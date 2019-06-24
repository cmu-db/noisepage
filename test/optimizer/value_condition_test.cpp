#include <memory>
#include <string>
#include <utility>

#include "catalog/catalog_defs.h"
#include "optimizer/statistics/value_condition.h"
#include "type/transient_value.h"
#include "type/transient_value_factory.h"

#include "gtest/gtest.h"

namespace terrier::optimizer {
// NOLINTNEXTLINE
TEST(ValueConditionTests, GetColumnIDTest) {
  auto val = std::make_shared<type::TransientValue>(type::TransientValueFactory::GetInteger(1));
  ValueCondition v(catalog::col_oid_t(1), "", parser::ExpressionType::INVALID, val);

  EXPECT_EQ(catalog::col_oid_t(1), v.GetColumnID());
}

// NOLINTNEXTLINE
TEST(ValueConditionTests, GetColumnNameTest) {
  auto val = std::make_shared<type::TransientValue>(type::TransientValueFactory::GetInteger(1));
  ValueCondition v(catalog::col_oid_t(1), "", parser::ExpressionType::INVALID, val);

  EXPECT_EQ("", v.GetColumnName());
}

// NOLINTNEXTLINE
TEST(ValueConditionTests, GetTypeTest) {
  auto val = std::make_shared<type::TransientValue>(type::TransientValueFactory::GetInteger(1));
  ValueCondition v(catalog::col_oid_t(1), "", parser::ExpressionType::INVALID, val);

  EXPECT_EQ(parser::ExpressionType::INVALID, v.GetType());
}

// NOLINTNEXTLINE
TEST(ValueConditionTests, GetPointerToValueTest) {
  auto val = std::make_shared<type::TransientValue>(type::TransientValueFactory::GetInteger(1));
  ValueCondition v(catalog::col_oid_t(1), "", parser::ExpressionType::INVALID, val);

  EXPECT_EQ(*val, *v.GetPointerToValue());
}
}  // namespace terrier::optimizer
