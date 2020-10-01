#include "optimizer/statistics/selectivity_util.h"

#include <memory>

#include "gtest/gtest.h"
#include "test_util/test_harness.h"

namespace terrier::optimizer {
class SelectivityUtilTests : public TerrierTest {
 protected:
  ColumnStats column_stats_obj_1_;
  SelectivityUtil selectivity_util;

  void SetUp() override {
    column_stats_obj_1_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1),
                                      catalog::col_oid_t(1), 5, 4, 0.2,
                                      {3, 4, 5}, {2, 2, 2}, {1.0, 5.0},
                                      true);
  }
};

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestDecimal1) {
  // Create a constant value expression to pass to ValueCondition.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(6.f));
  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(catalog::col_oid_t(1), parser::ExpressionType::COMPARE_LESS_THAN,
                                 std::move(const_value_expr_ptr));
  double res = selectivity_util.ComputeSelectivity(common::ManagedPointer<ColumnStats>(&column_stats_obj_1_),
                                                   value_condition);
  // The value 6 goes past the last bucket in the histogram and so selectivity must be predicted to be 1.
  ASSERT_EQ(res, 1.f);
}

TEST_F(SelectivityUtilTests, TestDecimal2) {
  // Create a constant value expression to pass to ValueCondition.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
          std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(3.f));
  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(catalog::col_oid_t(1), parser::ExpressionType::COMPARE_LESS_THAN,
                                 std::move(const_value_expr_ptr));
  double res = selectivity_util.ComputeSelectivity(common::ManagedPointer<ColumnStats>(&column_stats_obj_1_),
                                                   value_condition);
  // The value 3 falls in the last bucket of the histogram (with 2 buckets) and so selectivity is predicted to be 0.5.
  ASSERT_EQ(res, 0.5f);
}

TEST_F(SelectivityUtilTests, TestDecimal3) {
  // Create a constant value expression to pass to ValueCondition.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
          std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(0.f));
  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(catalog::col_oid_t(1), parser::ExpressionType::COMPARE_LESS_THAN,
                                 std::move(const_value_expr_ptr));
  double res = selectivity_util.ComputeSelectivity(common::ManagedPointer<ColumnStats>(&column_stats_obj_1_),
                                                   value_condition);
  // The value 0 falls in the first bucket of the histogram (with 2 buckets) and so selectivity is predicted to be 0.
  ASSERT_EQ(res, 0.f);
}

}  // namespace terrier::optimizer


