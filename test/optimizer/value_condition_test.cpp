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
  type::TransientValue val = type::TransientValueFactory::GetInteger(1);
  ValueCondition v(catalog::col_oid_t(1), "", parser::ExpressionType::INVALID, std::move(val));

  catalog::col_oid_t expected_column_id = catalog::col_oid_t(1);
  catalog::col_oid_t actual_column_id = v.GetColumnID();

  EXPECT_EQ(actual_column_id, expected_column_id);
}

// NOLINTNEXTLINE
TEST(ValueConditionTests, GetColumnNameTest) {
  type::TransientValue val = type::TransientValueFactory::GetInteger(1);
  ValueCondition v(catalog::col_oid_t(1), "", parser::ExpressionType::INVALID, std::move(val));

  std::string expected_column_name;
  std::string actual_column_name = v.GetColumnName();

  EXPECT_EQ(actual_column_name, expected_column_name);
}

// NOLINTNEXTLINE
TEST(ValueConditionTests, GetTypeTest) {
  type::TransientValue val = type::TransientValueFactory::GetInteger(1);
  ValueCondition v(catalog::col_oid_t(1), "", parser::ExpressionType::INVALID, std::move(val));

  parser::ExpressionType expected_type = parser::ExpressionType::INVALID;
  parser::ExpressionType actual_type = v.GetType();

  EXPECT_EQ(expected_type, actual_type);
}

}  // namespace terrier::optimizer
