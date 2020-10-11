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
  NewColumnStats<execution::sql::Real> column_stats_obj_3_;
  NewColumnStats<execution::sql::Real> column_stats_obj_4_;

  SelectivityUtil selectivity_util;

  void SetUp() override {
    // Floating point type column.
    column_stats_obj_1_ = NewColumnStats<execution::sql::Real>(
        catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(1), 10, 8, 0.2, 0, 10, {1.0, 5.0}, true);
    // Construct Top k variable.
    column_stats_obj_1_.GetTopK()->Increment(3, 2);
    column_stats_obj_1_.GetTopK()->Increment(4, 2);
    column_stats_obj_1_.GetTopK()->Increment(5, 2);
    column_stats_obj_1_.GetTopK()->Increment(0, 1);
    column_stats_obj_1_.GetTopK()->Increment(0, 7);

    // Integer type column.
    column_stats_obj_2_ = NewColumnStats<execution::sql::Integer>(
        catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(3), 10, 8, 0.2, 0, 10, {1, 5}, true);
    // Construct Top k variable.
    column_stats_obj_2_.GetTopK()->Increment(3, 2);
    column_stats_obj_2_.GetTopK()->Increment(4, 2);
    column_stats_obj_2_.GetTopK()->Increment(5, 2);
    column_stats_obj_2_.GetTopK()->Increment(0, 1);
    column_stats_obj_2_.GetTopK()->Increment(7, 1);

    // Floating point type column.
    column_stats_obj_3_ = NewColumnStats<execution::sql::Real>(
        catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(2), 1000, 900, 0.1, 0, 10, {1.0, 5.0}, true);
    // Construct Top k variable.
    column_stats_obj_3_.GetTopK()->Increment(1.0, 500);
    column_stats_obj_3_.GetTopK()->Increment(2.0, 250);
    column_stats_obj_3_.GetTopK()->Increment(3.0, 100);
    column_stats_obj_3_.GetTopK()->Increment(4.0, 20);
    column_stats_obj_3_.GetTopK()->Increment(5.0, 5);
    column_stats_obj_3_.GetTopK()->Increment(6.0, 5);
    column_stats_obj_3_.GetTopK()->Increment(7.0, 5);
    column_stats_obj_3_.GetTopK()->Increment(8.0, 5);
    column_stats_obj_3_.GetTopK()->Increment(9.0, 2);
    column_stats_obj_3_.GetTopK()->Increment(10.0, 2);
    column_stats_obj_3_.GetTopK()->Increment(11.0, 2);
    column_stats_obj_3_.GetTopK()->Increment(12.0, 2);
    column_stats_obj_3_.GetTopK()->Increment(13.0, 2);

    // Floating point type column.
    column_stats_obj_4_ =
        NewColumnStats<execution::sql::Real>(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(4),
                                             600000, 500500, 0.1658, 0, 500, {1.0, 5.0}, true);
    // Assume entry with value i occurs i times in the table.
    for (int i = 1; i <= 1000; ++i) column_stats_obj_4_.GetTopK()->Increment(static_cast<float>(i), i);
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
  ASSERT_DOUBLE_EQ(res, 1.f);

  // TEST PART 2
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(3.f));

  // Create a value condition to pass to SelectivityUtil.
  value_condition =
      ValueCondition(catalog::col_oid_t(1), parser::ExpressionType::COMPARE_LESS_THAN, std::move(const_value_expr_ptr));

  res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_1_), value_condition);

  // The value 3 falls in the last bucket of the histogram (with 2 buckets) and so selectivity is predicted to be 0.5.
  ASSERT_DOUBLE_EQ(0.5f, res);

  // TEST PART 3
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(0.f));

  // Create a value condition to pass to SelectivityUtil.
  value_condition =
      ValueCondition(catalog::col_oid_t(1), parser::ExpressionType::COMPARE_LESS_THAN, std::move(const_value_expr_ptr));

  res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_1_), value_condition);

  // The value 0 falls in the first bucket of the histogram (with 2 buckets) and so selectivity is predicted to be 0.
  ASSERT_DOUBLE_EQ(0.f, res);
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
  ASSERT_DOUBLE_EQ(1, res);
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
  ASSERT_DOUBLE_EQ(1, res);
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
  // The value 4 occurs in top k variable and has a frequency of 2.
  ASSERT_DOUBLE_EQ(0.2, res);

  // Create a constant value expression to pass to ValueCondition.
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(0));

  // Create a value condition to pass to SelectivityUtil.
  value_condition =
      ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_EQUAL, std::move(const_value_expr_ptr));

  res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);

  // The value 0 does not occur in Top k variable.
  ASSERT_DOUBLE_EQ(0.1, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestFloatEqual) {
  // Create a constant value expression to pass to ValueCondition.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(1.0));
  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(catalog::col_oid_t(2), parser::ExpressionType::COMPARE_EQUAL,
                                 std::move(const_value_expr_ptr));
  double res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_3_), value_condition);
  // The value 1.0 occurs in topk and has a frequency of 500.
  ASSERT_DOUBLE_EQ(0.5, res);

  // Create a constant value expression to pass to ValueCondition.
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(5.0));
  // Create a value condition to pass to SelectivityUtil.
  value_condition =
      ValueCondition(catalog::col_oid_t(2), parser::ExpressionType::COMPARE_EQUAL, std::move(const_value_expr_ptr));
  res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_3_), value_condition);
  // The value 5.0 does not occur in top-k and has a frequency of 5.
  ASSERT_DOUBLE_EQ(0.005, res);

  for (int i = 1; i <= 1000; ++i) {
    // Create a constant value expression to pass to ValueCondition.
    const_value_expr_ptr = std::make_unique<parser::ConstantValueExpression>(
        type::TypeId::DECIMAL, execution::sql::Real(static_cast<float>(i)));
    // Create a value condition to pass to SelectivityUtil.
    value_condition =
        ValueCondition(catalog::col_oid_t(4), parser::ExpressionType::COMPARE_EQUAL, std::move(const_value_expr_ptr));
    res = selectivity_util.ComputeSelectivity(
        common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_4_), value_condition);

    // Count min sketch can over-estimate the number of matching columns.
    ASSERT_LE(i / static_cast<double>(600000), res);
  }
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
  ASSERT_DOUBLE_EQ(0.f, res);

  // TEST PART 2
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(3.f));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(catalog::col_oid_t(1), parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO,
                                   std::move(const_value_expr_ptr));

  res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_1_), value_condition);

  // The value 3 falls in the last bucket of the histogram (with 2 buckets) and so selectivity is predicted to be 0.5.
  ASSERT_DOUBLE_EQ(0.5f, res);

  // TEST PART 3
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(0.f));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(catalog::col_oid_t(1), parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO,
                                   std::move(const_value_expr_ptr));

  res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_1_), value_condition);
  // The value 0 falls in the first bucket of the histogram (with 2 buckets) and so selectivity is predicted to be 1.
  ASSERT_DOUBLE_EQ(1.f, res);
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
  ASSERT_DOUBLE_EQ(1.f, res);

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
  ASSERT_DOUBLE_EQ(0.7, res);

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
  ASSERT_DOUBLE_EQ(0.1, res);
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
  ASSERT_DOUBLE_EQ(0.f, res);

  // TEST PART 2
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(3));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_GREATER_THAN,
                                   std::move(const_value_expr_ptr));

  res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);

  // Sel for <= -> 0.7. Sel = 1 - 0.7 = 0.3.
  ASSERT_DOUBLE_EQ(0.3, res);

  // TEST PART 3
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(0));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_GREATER_THAN,
                                   std::move(const_value_expr_ptr));

  res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);

  ASSERT_FLOAT_EQ(0.9, res);
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

  // The value 4 occurs in topK and has a frequency of 2.
  // Sl for = -> 0.2. Sel = 1 - 0.2 = 0.8.
  ASSERT_DOUBLE_EQ(res, 0.8);

  // Create a constant value expression to pass to ValueCondition.
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(0));

  // Create a value condition to pass to SelectivityUtil.
  value_condition =
      ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_NOT_EQUAL, std::move(const_value_expr_ptr));

  res = selectivity_util.ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);

  ASSERT_DOUBLE_EQ(0.9, res);
}
}  // namespace terrier::optimizer
