#include "optimizer/statistics/selectivity_util.h"

#include <memory>

#include "execution/sql/value.h"
#include "gtest/gtest.h"
#include "test_util/test_harness.h"

namespace noisepage::optimizer {
class SelectivityUtilTests : public TerrierTest {
 protected:
  NewColumnStats<execution::sql::Real> column_stats_obj_1_;
  NewColumnStats<execution::sql::Integer> column_stats_obj_2_;
  NewColumnStats<execution::sql::Real> column_stats_obj_3_;
  NewColumnStats<execution::sql::Real> column_stats_obj_4_;
  NewColumnStats<execution::sql::BoolVal> column_stats_obj_5_;
  NewColumnStats<execution::sql::StringVal> column_stats_obj_6_;

  void SetUp() override {
    // Floating point type column.
    column_stats_obj_1_ = NewColumnStats<execution::sql::Real>(catalog::db_oid_t(1), catalog::table_oid_t(1),
                                                               catalog::col_oid_t(1), 10, 8, 0.2, 10, 10, 10, true);

    // Values and frequencies.
    std::vector<std::pair<double, int>> vals_1 = {{3, 2}, {4, 2}, {5, 2}, {0, 1}, {1, 1}};

    // Construct Top k.
    for (auto &entry : vals_1) {
      column_stats_obj_1_.GetTopK()->Increment(entry.first, entry.second);
    }

    // Construct histogram
    for (auto &entry : vals_1) {
      for (int j = 0; j < entry.second; ++j) {
        column_stats_obj_1_.GetHistogram()->Increment(entry.first);
      }
    }

    // INTEGER type column.
    column_stats_obj_2_ = NewColumnStats<execution::sql::Integer>(catalog::db_oid_t(1), catalog::table_oid_t(1),
                                                                  catalog::col_oid_t(3), 10, 8, 0.2, 10, 10, 10, true);

    // Values and frequencies.
    std::vector<std::pair<int64_t, int>> vals_2 = {{3, 2}, {4, 2}, {5, 2}, {0, 1}, {7, 1}};

    // Construct Top K.
    for (auto &entry : vals_2) {
      column_stats_obj_2_.GetTopK()->Increment(entry.first, entry.second);
    }

    // Construct histogram.
    for (auto &entry : vals_2) {
      for (int j = 0; j < entry.second; ++j) {
        column_stats_obj_2_.GetHistogram()->Increment(entry.first);
      }
    }

    // DECIMAL column.
    column_stats_obj_3_ = NewColumnStats<execution::sql::Real>(catalog::db_oid_t(1), catalog::table_oid_t(1),
                                                               catalog::col_oid_t(2), 1000, 900, 0.1, 10, 10, 10, true);

    // Values and frequencies.
    std::vector<std::pair<double, int>> vals_3 = {{1.0, 500}, {2.0, 250}, {3.0, 100}, {4.0, 20}, {5.0, 5},
                                                  {6.0, 5},   {7.0, 5},   {8.0, 5},   {9.0, 2},  {10.0, 2},
                                                  {11.0, 2},  {12.0, 2},  {13.0, 2}};
    // Construct Top k variable.
    for (auto &entry : vals_3) {
      column_stats_obj_3_.GetTopK()->Increment(entry.first, entry.second);
    }

    // Construct histogram.
    for (auto &entry : vals_3) {
      for (int j = 0; j < entry.second; ++j) {
        column_stats_obj_3_.GetHistogram()->Increment(entry.first);
      }
    }

    // DECIMAL column.
    column_stats_obj_4_ =
        NewColumnStats<execution::sql::Real>(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(4),
                                             600000, 500500, 0.1658, 10, 500, 10, true);
    // Assume entry with value i occurs i times in the table.
    for (int i = 1; i <= 1000; ++i) column_stats_obj_4_.GetTopK()->Increment(static_cast<float>(i), i);
    // Construct histogram.
    for (int i = 0; i <= 1000; ++i) {
      for (int j = 0; j < i; ++j) {
        column_stats_obj_4_.GetHistogram()->Increment(i);
      }
    }

    // BOOLEAN column.
    column_stats_obj_5_ = NewColumnStats<execution::sql::BoolVal>(
        catalog::db_oid_t(5), catalog::table_oid_t(5), catalog::col_oid_t(5), 100, 80, 0.2, 10, 10, 10, true);
    // 60 true, 20 false, 20 null.
    column_stats_obj_5_.GetTopK()->Increment(true, 60);
    column_stats_obj_5_.GetTopK()->Increment(false, 20);

    // TODO(arvindsk) The histogram class currently does not support VARCHAR entries. Templatize the histogram class.
    //    column_stats_obj_6_ = NewColumnStats<execution::sql::StringVal>(
    //        catalog::db_oid_t(6), catalog::table_oid_t(6), catalog::col_oid_t (6), 10, 5, 0.5, 10, 10, 10, true);
    //    // Values and frequencies.
    //    std::vector<std::pair<std::string_view, int>> vals_6 = {{"hello", 2}, {"abc", 1}, {"def", 1}, {"xyz", 1}};
    //
    //    // Construct Top k variable.
    //    for (int i = 0; i < vals_6.size(); ++i) {
    //      storage::VarlenEntry value;
    //      value.Create(vals_6[i].first);
    //      column_stats_obj_6_.GetTopK()->Increment(value, vals_6[i].second);
    //    }
    //
    //    // Construct histogram.
    //    for (int i = 0; i < vals_3.size(); ++i) {
    //      for (int j = 0; j < vals_3[i].second; ++j) {
    //        column_stats_obj_3_.GetHistogram()->Increment(vals_3[i].first);
    //      }
    //    }
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

  // Compute selectivity for col < 6.
  double res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_1_), value_condition);

  ASSERT_DOUBLE_EQ(res, 0.8);

  // TEST PART 2
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(3.f));

  // Create a value condition to pass to SelectivityUtil.
  value_condition =
      ValueCondition(catalog::col_oid_t(1), parser::ExpressionType::COMPARE_LESS_THAN, std::move(const_value_expr_ptr));

  // Compute selectivity for col < 3.
  res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_1_), value_condition);

  // True value is 0.2 but some error is tolerated.
  ASSERT_DOUBLE_EQ(0.1, res);

  // TEST PART 3
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(0.f));

  // Create a value condition to pass to SelectivityUtil.
  value_condition =
      ValueCondition(catalog::col_oid_t(1), parser::ExpressionType::COMPARE_LESS_THAN, std::move(const_value_expr_ptr));

  // Compute selectivity for col < 0.
  res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_1_), value_condition);

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
  // Compute selectivity for col < 6.
  double res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);
  // True value is 0.7 but some error is tolerated.
  ASSERT_DOUBLE_EQ(0.6875, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestTinyIntLessThan) {
  // Create a constant value expression to pass to ValueCondition.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::TINYINT, execution::sql::Integer(6));
  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_LESS_THAN,
                                 std::move(const_value_expr_ptr));
  // Compute selectivity for col < 6.
  double res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);
  // True value is 0.7 but some error is tolerated.
  ASSERT_DOUBLE_EQ(0.6875, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestTinyIntegerEqual) {
  // Create a constant value expression to pass to ValueCondition.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(4));
  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_EQUAL,
                                 std::move(const_value_expr_ptr));

  // Compute selectivity for col = 4.
  double res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);
  // The value 4 occurs in top k variable and has a frequency of 2.
  ASSERT_DOUBLE_EQ(0.2, res);

  // Create a constant value expression to pass to ValueCondition.
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(0));

  // Create a value condition to pass to SelectivityUtil.
  value_condition =
      ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_EQUAL, std::move(const_value_expr_ptr));

  // Compute selectivity for col = 0.
  res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);

  ASSERT_DOUBLE_EQ(0.1, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestFloatEqual) {
  // TEST PART 1
  // Create a constant value expression to pass to ValueCondition.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(1.0));
  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(catalog::col_oid_t(2), parser::ExpressionType::COMPARE_EQUAL,
                                 std::move(const_value_expr_ptr));
  // Compute selectivity for col = 1.0.
  double res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_3_), value_condition);
  // The value 1.0 occurs in topk and has a frequency of 500.
  ASSERT_DOUBLE_EQ(0.5, res);

  // TEST PART 2
  // Create a constant value expression to pass to ValueCondition.
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(5.0));
  // Create a value condition to pass to SelectivityUtil.
  value_condition =
      ValueCondition(catalog::col_oid_t(2), parser::ExpressionType::COMPARE_EQUAL, std::move(const_value_expr_ptr));
  // Compute selectivity for col = 5.0.
  res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_3_), value_condition);
  // The value 5.0 does not occur in top-k and has a frequency of 5.
  ASSERT_DOUBLE_EQ(0.005, res);

  // TEST PART 3
  for (int i = 1; i <= 1000; ++i) {
    // Create a constant value expression to pass to ValueCondition.
    const_value_expr_ptr = std::make_unique<parser::ConstantValueExpression>(
        type::TypeId::DECIMAL, execution::sql::Real(static_cast<float>(i)));
    // Create a value condition to pass to SelectivityUtil.
    value_condition =
        ValueCondition(catalog::col_oid_t(4), parser::ExpressionType::COMPARE_EQUAL, std::move(const_value_expr_ptr));
    res = SelectivityUtil::ComputeSelectivity(
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
  // Compute selectivity for col >= 6.0.
  double res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_1_), value_condition);

  // The value 6 goes past the last bucket in the histogram and so selectivity must be predicted to be 0.
  ASSERT_DOUBLE_EQ(0.f, res);

  // TEST PART 2
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(3.f));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(catalog::col_oid_t(1), parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO,
                                   std::move(const_value_expr_ptr));

  // Compute selectivity for col >= 3.0.
  res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_1_), value_condition);

  ASSERT_DOUBLE_EQ(0.7, res);

  // TEST PART 3
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(0.f));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(catalog::col_oid_t(1), parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO,
                                   std::move(const_value_expr_ptr));

  // Compute selectivity for col >= 0.0.
  res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Real>>(&column_stats_obj_1_), value_condition);

  ASSERT_DOUBLE_EQ(0.8, res);
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
  // Compute selectivity for col <= 6.
  double res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);

  ASSERT_DOUBLE_EQ(0.6875, res);

  // TEST PART 2
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(3));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO,
                                   std::move(const_value_expr_ptr));
  // Compute selectivity for col <= 3.
  res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);

  // True value should be 0.3. TODO(arvindsaik) Can make '<=' better here.
  ASSERT_DOUBLE_EQ(0.2, res);

  // TEST PART 3
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(0));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO,
                                   std::move(const_value_expr_ptr));

  res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);

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
  // Compute selectivity for col > 6.
  double res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);

  // True value is 0.1. Acceptable error.
  ASSERT_DOUBLE_EQ(0.11249999999999999, res);

  // TEST PART 2
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(3));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_GREATER_THAN,
                                   std::move(const_value_expr_ptr));
  // Compute selectivity for col > 3.
  res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);

  // True value is 0.5. TODO(arvindsk) Making '<=' better will fix this.
  ASSERT_DOUBLE_EQ(0.6, res);

  // TEST PART 3
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(0));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_GREATER_THAN,
                                   std::move(const_value_expr_ptr));
  // Compute selectivity for col > 0.
  res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);

  ASSERT_FLOAT_EQ(0.7, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestIntegerNotEqual) {
  // Create a constant value expression to pass to ValueCondition.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(4));
  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_NOT_EQUAL,
                                 std::move(const_value_expr_ptr));
  // Compute selectivity for col != 4.
  double res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);

  ASSERT_DOUBLE_EQ(0.6, res);

  // Create a constant value expression to pass to ValueCondition.
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(0));

  // Create a value condition to pass to SelectivityUtil.
  value_condition =
      ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_NOT_EQUAL, std::move(const_value_expr_ptr));

  // Compute selectivity for col != 0.
  res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(&column_stats_obj_2_), value_condition);

  ASSERT_DOUBLE_EQ(0.7, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestBoolEqual) {
  // TEST PART 1
  // Create a constant value expression to pass to ValueCondition.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(catalog::col_oid_t(5), parser::ExpressionType::COMPARE_EQUAL,
                                 std::move(const_value_expr_ptr));

  // Compute selectivity for col = true.
  double res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::BoolVal>>(&column_stats_obj_5_), value_condition);

  ASSERT_DOUBLE_EQ(0.6, res);

  // TEST PART 2
  // Create a constant value expression to pass to ValueCondition.
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(false));
  // Create a value condition to pass to SelectivityUtil.
  value_condition =
      ValueCondition(catalog::col_oid_t(5), parser::ExpressionType::COMPARE_EQUAL, std::move(const_value_expr_ptr));

  // Compute selectivity for col = true.
  res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::BoolVal>>(&column_stats_obj_5_), value_condition);

  ASSERT_DOUBLE_EQ(0.2, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestNullColStats) {
  // TEST for DEFAULT_SELECTIVITY when column stats is nullptr with different operators.
  // Create a constant value expression to pass to ValueCondition.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(4));
  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_NOT_EQUAL,
                                 std::move(const_value_expr_ptr));
  // Compute selectivity for col != 4.
  double res = SelectivityUtil::ComputeSelectivity(
      common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(nullptr), value_condition);
  ASSERT_DOUBLE_EQ(DEFAULT_SELECTIVITY_VALUE, res);

  // Create a constant value expression to pass to ValueCondition.
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(4));
  // Create a value condition to pass to SelectivityUtil.
  value_condition =
      ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_EQUAL, std::move(const_value_expr_ptr));
  res = SelectivityUtil::ComputeSelectivity(common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(nullptr),
                                            value_condition);
  ASSERT_DOUBLE_EQ(DEFAULT_SELECTIVITY_VALUE, res);

  // Create a constant value expression to pass to ValueCondition.
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(4));
  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_GREATER_THAN,
                                   std::move(const_value_expr_ptr));
  res = SelectivityUtil::ComputeSelectivity(common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(nullptr),
                                            value_condition);
  ASSERT_DOUBLE_EQ(DEFAULT_SELECTIVITY_VALUE, res);

  // Create a constant value expression to pass to ValueCondition.
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(4));
  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO,
                                   std::move(const_value_expr_ptr));
  res = SelectivityUtil::ComputeSelectivity(common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(nullptr),
                                            value_condition);
  ASSERT_DOUBLE_EQ(DEFAULT_SELECTIVITY_VALUE, res);

  // Create a constant value expression to pass to ValueCondition.
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(4));
  // Create a value condition to pass to SelectivityUtil.
  value_condition =
      ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_LESS_THAN, std::move(const_value_expr_ptr));
  res = SelectivityUtil::ComputeSelectivity(common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(nullptr),
                                            value_condition);
  ASSERT_DOUBLE_EQ(DEFAULT_SELECTIVITY_VALUE, res);

  // Create a constant value expression to pass to ValueCondition.
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(4));
  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(catalog::col_oid_t(3), parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO,
                                   std::move(const_value_expr_ptr));
  res = SelectivityUtil::ComputeSelectivity(common::ManagedPointer<NewColumnStats<execution::sql::Integer>>(nullptr),
                                            value_condition);
  ASSERT_DOUBLE_EQ(DEFAULT_SELECTIVITY_VALUE, res);
}
}  // namespace noisepage::optimizer
