#include "optimizer/statistics/selectivity_util.h"

#include <memory>

#include "execution/sql/value.h"
#include "gtest/gtest.h"
#include "test_util/test_harness.h"

namespace terrier::optimizer {
class SelectivityUtilTests : public TerrierTest {
 protected:
  NewColumnStats<execution::sql::Real> column_stats_obj_1_;
  NewColumnStats<execution::sql::Integer> column_stats_obj_2_;
  SelectivityUtil selectivity_util;

  void SetUp() override {
    // Floating point type column.
    column_stats_obj_1_ =
        NewColumnStats<execution::sql::Real>(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(1), 10,
                                             8, 0.2, {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    // Integer type column.
    column_stats_obj_2_ =
        NewColumnStats<execution::sql::Integer>(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(3),
                                                +10, 8, 0.2, {3, 4, 5}, {2, 2, 2}, {1, 5}, true);
  }
};

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestFloatLessThan) {
  // TEST PART 1
  // Create a constant value expression to pass to ValueCondition.
  // Floating point numbers have DECIMAL type and are represented as Reals in execution layer.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(6.f));

  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(catalog::col_oid_t(1), parser::ExpressionType::COMPARE_LESS_THAN,
                                 std::move(const_value_expr_ptr));

  double res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_1_), value_condition);

  // The value 6 goes past the last bucket in the histogram and so selectivity must be predicted to be 1.
  ASSERT_FLOAT_EQ(res, 1.f);

  // TEST PART 2
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(3.f));

  // Create a value condition to pass to SelectivityUtil.
  value_condition =
      ValueCondition(catalog::col_oid_t(1), parser::ExpressionType::COMPARE_LESS_THAN, std::move(const_value_expr_ptr));

  res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_1_), value_condition);

  // The value 3 falls in the last bucket of the histogram (with 2 buckets) and so selectivity is predicted to be 0.5.
  ASSERT_FLOAT_EQ(res, 0.5f);

  // TEST PART 3
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(0.f));

  // Create a value condition to pass to SelectivityUtil.
  value_condition =
      ValueCondition(catalog::col_oid_t(1), parser::ExpressionType::COMPARE_LESS_THAN, std::move(const_value_expr_ptr));

  res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_1_), value_condition);

  // The value 0 falls in the first bucket of the histogram (with 2 buckets) and so selectivity is predicted to be 0.
  ASSERT_FLOAT_EQ(res, 0.f);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestIntegerLessThan) {
  // Create a constant value expression to pass to ValueCondition.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(6));
  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_LESS_THAN,
                                 std::move(const_value_expr_ptr));
  double res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);
  // The value 6 goes past the last bucket in the histogram and so selectivity must be predicted to be 1.
  ASSERT_FLOAT_EQ(res, 1);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestTinyIntLessThan) {
  // Create a constant value expression to pass to ValueCondition.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::TINYINT, execution::sql::Integer(6));
  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_LESS_THAN,
                                 std::move(const_value_expr_ptr));
  double res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);
  // The value 6 goes past the last bucket in the histogram and so selectivity must be predicted to be 1.
  ASSERT_FLOAT_EQ(res, 1);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestIntegerEqual) {
  // Create a constant value expression to pass to ValueCondition.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(4));
  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_EQUAL,
                                 std::move(const_value_expr_ptr));
  double res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);
  // The value 4 occurs in MCV and has a frequency of 2.
  ASSERT_FLOAT_EQ(res, 0.2);

  // Create a constant value expression to pass to ValueCondition.
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(0));

  // Create a value condition to pass to SelectivityUtil.
  value_condition =
      ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_EQUAL, std::move(const_value_expr_ptr));

  res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);

  // The value 0 does not occur in MCV.
  ASSERT_FLOAT_EQ(res, 0.08);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestFloatGreaterThanEqual) {
  // TEST PART 1
  // Create a constant value expression to pass to ValueCondition.
  // Floating point numbers have DECIMAL type and are represented as Reals in execution layer.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(6.f));

  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(catalog::col_oid_t(1), parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO,
                                 std::move(const_value_expr_ptr));

  double res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_1_), value_condition);

  // The value 6 goes past the last bucket in the histogram and so selectivity must be predicted to be 0.
  ASSERT_FLOAT_EQ(res, 0.f);

  // TEST PART 2
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(3.f));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(catalog::col_oid_t(1), parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO,
                                   std::move(const_value_expr_ptr));

  res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_1_), value_condition);

  // The value 3 falls in the last bucket of the histogram (with 2 buckets) and so selectivity is predicted to be 0.5.
  ASSERT_FLOAT_EQ(res, 0.5f);

  // TEST PART 3
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(0.f));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(catalog::col_oid_t(1), parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO,
                                   std::move(const_value_expr_ptr));

  res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_1_), value_condition);
  // The value 0 falls in the first bucket of the histogram (with 2 buckets) and so selectivity is predicted to be 1.
  ASSERT_FLOAT_EQ(res, 1.f);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestIntegerLessThanEqual) {
  // TEST PART 1
  // Create a constant value expression to pass to ValueCondition.
  // Floating point numbers have DECIMAL type and are represented as Reals in execution layer.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(6));

  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO,
                                 std::move(const_value_expr_ptr));

  double res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);

  // The value 6 goes past the last bucket in the histogram.
  // Sel for < -> 1. Sel for = -> 0. Total sel : 1.
  ASSERT_FLOAT_EQ(res, 1.f);

  // TEST PART 2
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(3));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO,
                                   std::move(const_value_expr_ptr));

  res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);

  // The value 3 falls in the last bucket of the histogram (with 2 buckets).
  // Sel for  < -> 0.5 + Sel for = -> 0.2. Total : 0.7
  ASSERT_FLOAT_EQ(res, 0.7);

  // TEST PART 3
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(0));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO,
                                   std::move(const_value_expr_ptr));

  res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);
  // The value 0 falls in the first bucket (lower bound operation) on the histogram (with 2 buckets).
  // Sel for < -> 0. Sel for = -> 0.08. Total sel : 0.08
  ASSERT_FLOAT_EQ(res, 0.08);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestIntegerGreaterThan) {
  // TEST PART 1
  // Create a constant value expression to pass to ValueCondition.
  // Floating point numbers have DECIMAL type and are represented as Reals in execution layer.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(6));

  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_GREATER_THAN,
                                 std::move(const_value_expr_ptr));

  double res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);

  // Sel for <= -> 1. Sel = 1 - 1 = 0.
  ASSERT_FLOAT_EQ(res, 0.f);

  // TEST PART 2
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(3));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_GREATER_THAN,
                                   std::move(const_value_expr_ptr));

  res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);

  // Sel for <= -> 0.7. Sel = 1 - 0.7 = 0.3.
  ASSERT_FLOAT_EQ(res, 0.3);

  // TEST PART 3
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(0));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_GREATER_THAN,
                                   std::move(const_value_expr_ptr));

  res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);
  // Sel for <= -> 0.08. Sel = 1 - 0.08 = 0.92.
  ASSERT_FLOAT_EQ(res, 0.92);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestIntegerNotEqual) {
  // Create a constant value expression to pass to ValueCondition.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(4));
  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_NOT_EQUAL,
                                 std::move(const_value_expr_ptr));
  double res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);

  // The value 4 occurs in MCV and has a frequency of 2.
  // Sl for = -> 0.2. Sel = 1 - 0.2 = 0.8.
  ASSERT_FLOAT_EQ(res, 0.8);

  // Create a constant value expression to pass to ValueCondition.
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(0));

  // Create a value condition to pass to SelectivityUtil.
  value_condition =
      ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_NOT_EQUAL, std::move(const_value_expr_ptr));

  res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);

  // The value 0 does not occur in MCV.
  // Sl for = -> 0.08. Sel = 1 - 0.08 = 0.92.
  ASSERT_FLOAT_EQ(res, 0.92);
}
}  // namespace terrier::optimizer
