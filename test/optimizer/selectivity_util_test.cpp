#include "optimizer/statistics/selectivity_util.h"

#include <memory>

#include "execution/sql/value.h"
#include "gtest/gtest.h"
#include "test_util/test_harness.h"

namespace noisepage::optimizer {
class SelectivityUtilTests : public TerrierTest {
 protected:
  catalog::db_oid_t db_oid_{1};

  // Real and Integer
  TableStats table_stats_1_;
  catalog::table_oid_t table1_oid_{1};
  catalog::col_oid_t table1_real_col_oid_{1};
  catalog::col_oid_t table1_int_col_oid_{3};

  // Real
  TableStats table_stats_2_;
  catalog::table_oid_t table2_oid_{2};
  catalog::col_oid_t table2_real_col_oid_{2};

  // Real
  TableStats table_stats_3_;
  catalog::table_oid_t table3_oid_{3};
  catalog::col_oid_t table3_real_col_oid_{4};

  // Boolean
  TableStats table_stats_4_;
  catalog::table_oid_t table4_oid_{4};
  catalog::col_oid_t table4_bool_col_oid_{5};

  // Date and Timestamp
  TableStats table_stats_5_;
  catalog::table_oid_t table5_oid_{5};
  catalog::col_oid_t table5_date_col_oid_{1};
  catalog::col_oid_t table5_timestamp_col_oid_{2};

  // Varchar
  TableStats table_stats_6_;
  catalog::table_oid_t table6_oid_{6};
  catalog::col_oid_t table6_varchar_col_oid_{1};

  // Empty
  TableStats table_stats_7_;
  catalog::table_oid_t table7_oid_{7};
  catalog::col_oid_t table7_empty_col_oid_{1};

  void CreateTable1() {
    // Floating point type column.
    auto real_column_stats = std::make_unique<ColumnStats<execution::sql::Real>>(
        db_oid_, table1_oid_, table1_real_col_oid_, 10, 8, 8, std::make_unique<TopKElements<double>>(),
        std::make_unique<Histogram<double>>(), type::TypeId::REAL);

    // Values and frequencies.
    std::vector<std::pair<double, int>> real_vals = {{3, 2}, {4, 2}, {5, 2}, {0, 1}, {1, 1}};

    // Construct Top k.
    for (auto &entry : real_vals) {
      real_column_stats->GetTopK()->Increment(entry.first, entry.second);
    }

    // Construct histogram
    for (auto &entry : real_vals) {
      for (int j = 0; j < entry.second; ++j) {
        real_column_stats->GetHistogram()->Increment(entry.first);
      }
    }

    // INTEGER type column.
    auto integet_col_stats = std::make_unique<ColumnStats<execution::sql::Integer>>(
        db_oid_, table1_oid_, table1_int_col_oid_, 10, 8, 8, std::make_unique<TopKElements<int64_t>>(),
        std::make_unique<Histogram<int64_t>>(), type::TypeId::INTEGER);

    // Values and frequencies.
    std::vector<std::pair<int64_t, int>> int_vals = {{3, 2}, {4, 2}, {5, 2}, {0, 1}, {7, 1}};

    // Construct Top K.
    for (auto &entry : int_vals) {
      integet_col_stats->GetTopK()->Increment(entry.first, entry.second);
    }

    // Construct histogram.
    for (auto &entry : int_vals) {
      for (int j = 0; j < entry.second; ++j) {
        integet_col_stats->GetHistogram()->Increment(entry.first);
      }
    }

    std::vector<std::unique_ptr<ColumnStatsBase>> column_stats;
    column_stats.emplace_back(std::move(real_column_stats));
    column_stats.emplace_back(std::move(integet_col_stats));
    table_stats_1_ = TableStats(db_oid_, catalog::table_oid_t(1), &column_stats);
  }

  void CreateTable2() {
    // DECIMAL column.
    auto real_col_stats = std::make_unique<ColumnStats<execution::sql::Real>>(
        db_oid_, table2_oid_, table2_real_col_oid_, 1000, 900, 900, std::make_unique<TopKElements<double>>(),
        std::make_unique<Histogram<double>>(), type::TypeId::REAL);

    // Values and frequencies.
    std::vector<std::pair<double, int>> real_vals = {{1.0, 500}, {2.0, 250}, {3.0, 100}, {4.0, 20}, {5.0, 5},
                                                     {6.0, 5},   {7.0, 5},   {8.0, 5},   {9.0, 2},  {10.0, 2},
                                                     {11.0, 2},  {12.0, 2},  {13.0, 2}};
    // Construct Top k variable.
    for (auto &entry : real_vals) {
      real_col_stats->GetTopK()->Increment(entry.first, entry.second);
    }

    // Construct histogram.
    for (auto &entry : real_vals) {
      for (int j = 0; j < entry.second; ++j) {
        real_col_stats->GetHistogram()->Increment(entry.first);
      }
    }

    std::vector<std::unique_ptr<ColumnStatsBase>> column_stats;
    column_stats.emplace_back(std::move(real_col_stats));
    table_stats_2_ = TableStats(db_oid_, table2_oid_, &column_stats);
  }

  void CreateTable3() {
    // DECIMAL column.
    auto real_col_stats = std::make_unique<ColumnStats<execution::sql::Real>>(
        db_oid_, table3_oid_, table3_real_col_oid_, 600000, 500500, 500500, std::make_unique<TopKElements<double>>(),
        std::make_unique<Histogram<double>>(), type::TypeId::REAL);
    // Assume entry with value i occurs i times in the table.
    for (int i = 1; i <= 1000; ++i) real_col_stats->GetTopK()->Increment(static_cast<float>(i), i);
    // Construct histogram.
    for (int i = 0; i <= 1000; ++i) {
      for (int j = 0; j < i; ++j) {
        real_col_stats->GetHistogram()->Increment(i);
      }
    }
    std::vector<std::unique_ptr<ColumnStatsBase>> column_stats;
    column_stats.emplace_back(std::move(real_col_stats));
    table_stats_3_ = TableStats(db_oid_, table3_oid_, &column_stats);
  }

  void CreateTable4() {
    // BOOLEAN column.
    auto bool_col_stats = std::make_unique<ColumnStats<execution::sql::BoolVal>>(
        db_oid_, table4_oid_, table4_bool_col_oid_, 100, 80, 80, std::make_unique<TopKElements<bool>>(),
        std::make_unique<Histogram<bool>>(), type::TypeId::BOOLEAN);
    // 60 true, 20 false, 20 null.
    bool_col_stats->GetTopK()->Increment(true, 60);
    bool_col_stats->GetTopK()->Increment(false, 20);
    std::vector<std::unique_ptr<ColumnStatsBase>> column_stats;
    column_stats.emplace_back(std::move(bool_col_stats));
    table_stats_4_ = TableStats(db_oid_, table4_oid_, &column_stats);
  }

  void CreateTable5() {
    // DATE column
    auto date_col_stats = std::make_unique<ColumnStats<execution::sql::DateVal>>(
        db_oid_, table5_oid_, table5_date_col_oid_, 5, 5, 4, std::make_unique<TopKElements<execution::sql::Date>>(),
        std::make_unique<Histogram<execution::sql::Date>>(), type::TypeId::DATE);

    std::vector<execution::sql::Date> date_vals;
    date_vals.emplace_back(execution::sql::Date::FromYMD(1995, 8, 6));
    date_vals.emplace_back(execution::sql::Date::FromYMD(1995, 8, 6));
    date_vals.emplace_back(execution::sql::Date::FromYMD(2020, 4, 20));
    date_vals.emplace_back(execution::sql::Date::FromYMD(2000, 1, 1));
    date_vals.emplace_back(execution::sql::Date::FromYMD(1900, 5, 5));
    for (auto date : date_vals) {
      date_col_stats->GetTopK()->Increment(date, 1);
      date_col_stats->GetHistogram()->Increment(date);
    }

    // Timestamp column
    auto timestamp_col_stats = std::make_unique<ColumnStats<execution::sql::TimestampVal>>(
        db_oid_, table5_oid_, table5_timestamp_col_oid_, 5, 5, 4,
        std::make_unique<TopKElements<execution::sql::Timestamp>>(),
        std::make_unique<Histogram<execution::sql::Timestamp>>(), type::TypeId::TIMESTAMP);

    std::vector<execution::sql::Timestamp> timestamp_vals;
    timestamp_vals.emplace_back(execution::sql::Timestamp::FromYMDHMS(1995, 8, 6, 15, 2, 40));
    timestamp_vals.emplace_back(execution::sql::Timestamp::FromYMDHMS(1995, 8, 6, 12, 43, 2));
    timestamp_vals.emplace_back(execution::sql::Timestamp::FromYMDHMS(2020, 4, 20, 3, 20, 56));
    timestamp_vals.emplace_back(execution::sql::Timestamp::FromYMDHMS(2000, 1, 1, 20, 42, 30));
    timestamp_vals.emplace_back(execution::sql::Timestamp::FromYMDHMS(1900, 5, 5, 5, 5, 5));
    for (auto timestamp : timestamp_vals) {
      timestamp_col_stats->GetTopK()->Increment(timestamp, 1);
      timestamp_col_stats->GetHistogram()->Increment(timestamp);
    }
    std::vector<std::unique_ptr<ColumnStatsBase>> column_stats;
    column_stats.emplace_back(std::move(date_col_stats));
    column_stats.emplace_back(std::move(timestamp_col_stats));
    table_stats_5_ = TableStats(db_oid_, table5_oid_, &column_stats);
  }

  void CreateTable6() {
    // Varchar column
    auto varchar_col_stats = std::make_unique<ColumnStats<execution::sql::StringVal>>(
        db_oid_, table6_oid_, table6_varchar_col_oid_, 2, 2, 2, std::make_unique<TopKElements<storage::VarlenEntry>>(),
        std::make_unique<Histogram<storage::VarlenEntry>>(), type::TypeId::VARCHAR);

    std::vector<storage::VarlenEntry> varchar_vals;
    varchar_vals.emplace_back(storage::VarlenEntry::Create("Foo"));
    varchar_vals.emplace_back(storage::VarlenEntry::Create("Bar"));
    for (auto varchar : varchar_vals) {
      varchar_col_stats->GetTopK()->Increment(varchar, 1);
      varchar_col_stats->GetHistogram()->Increment(varchar);
    }
    std::vector<std::unique_ptr<ColumnStatsBase>> column_stats;
    column_stats.emplace_back(std::move(varchar_col_stats));
    table_stats_6_ = TableStats(db_oid_, table6_oid_, &column_stats);
  }

  void CreateTable7() {
    // Empty column
    auto empty_col_stats = std::make_unique<ColumnStats<execution::sql::Integer>>(
        db_oid_, table7_oid_, table7_empty_col_oid_, 0, 0, 0, std::make_unique<TopKElements<int64_t>>(),
        std::make_unique<Histogram<int64_t>>(), type::TypeId::INTEGER);

    std::vector<std::unique_ptr<ColumnStatsBase>> column_stats;
    column_stats.emplace_back(std::move(empty_col_stats));
    table_stats_7_ = TableStats(db_oid_, table7_oid_, &column_stats);
  }

  void SetUp() override {
    CreateTable1();
    CreateTable2();
    CreateTable3();
    CreateTable4();
    CreateTable5();
    CreateTable6();
    CreateTable7();
  }
};

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestFloatLessThan) {
  // TEST PART 1
  // Create a constant value expression to pass to ValueCondition.
  // Floating point numbers have DECIMAL type and are represented as Reals in execution layer.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::REAL, execution::sql::Real(6.f));

  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(table1_real_col_oid_, "", parser::ExpressionType::COMPARE_LESS_THAN,
                                 std::move(const_value_expr_ptr));

  // Compute selectivity for col < 6.
  double res = SelectivityUtil::ComputeSelectivity(table_stats_1_, value_condition);

  ASSERT_DOUBLE_EQ(res, 0.8);

  // TEST PART 2
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::REAL, execution::sql::Real(3.f));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(table1_real_col_oid_, "", parser::ExpressionType::COMPARE_LESS_THAN,
                                   std::move(const_value_expr_ptr));

  // Compute selectivity for col < 3.
  res = SelectivityUtil::ComputeSelectivity(table_stats_1_, value_condition);

  // True value is 0.2 but some error is tolerated.
  ASSERT_DOUBLE_EQ(0.1, res);

  // TEST PART 3
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::REAL, execution::sql::Real(0.f));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(table1_real_col_oid_, "", parser::ExpressionType::COMPARE_LESS_THAN,
                                   std::move(const_value_expr_ptr));

  // Compute selectivity for col < 0.
  res = SelectivityUtil::ComputeSelectivity(table_stats_1_, value_condition);

  ASSERT_DOUBLE_EQ(0.f, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestIntegerLessThan) {
  // Create a constant value expression to pass to ValueCondition.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(6));
  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(table1_int_col_oid_, "", parser::ExpressionType::COMPARE_LESS_THAN,
                                 std::move(const_value_expr_ptr));
  // Compute selectivity for col < 6.
  double res = SelectivityUtil::ComputeSelectivity(table_stats_1_, value_condition);
  // True value is 0.7 but some error is tolerated.
  ASSERT_DOUBLE_EQ(0.6875, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestTinyIntLessThan) {
  // Create a constant value expression to pass to ValueCondition.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::TINYINT, execution::sql::Integer(6));
  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(table1_int_col_oid_, "", parser::ExpressionType::COMPARE_LESS_THAN,
                                 std::move(const_value_expr_ptr));
  // Compute selectivity for col < 6.
  double res = SelectivityUtil::ComputeSelectivity(table_stats_1_, value_condition);
  // True value is 0.7 but some error is tolerated.
  ASSERT_DOUBLE_EQ(0.6875, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestTinyIntegerEqual) {
  // Create a constant value expression to pass to ValueCondition.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(4));
  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(table1_int_col_oid_, "", parser::ExpressionType::COMPARE_EQUAL,
                                 std::move(const_value_expr_ptr));

  // Compute selectivity for col = 4.
  double res = SelectivityUtil::ComputeSelectivity(table_stats_1_, value_condition);
  // The value 4 occurs in top k variable and has a frequency of 2.
  ASSERT_DOUBLE_EQ(0.2, res);

  // Create a constant value expression to pass to ValueCondition.
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(0));

  // Create a value condition to pass to SelectivityUtil.
  value_condition =
      ValueCondition(catalog::col_oid_t(3), "", parser::ExpressionType::COMPARE_EQUAL, std::move(const_value_expr_ptr));

  // Compute selectivity for col = 0.
  res = SelectivityUtil::ComputeSelectivity(table_stats_1_, value_condition);

  ASSERT_DOUBLE_EQ(0.1, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestFloatEqual) {
  // TEST PART 1
  // Create a constant value expression to pass to ValueCondition.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::REAL, execution::sql::Real(1.0));
  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(table2_real_col_oid_, "", parser::ExpressionType::COMPARE_EQUAL,
                                 std::move(const_value_expr_ptr));
  // Compute selectivity for col = 1.0.
  double res = SelectivityUtil::ComputeSelectivity(table_stats_2_, value_condition);
  // The value 1.0 occurs in topk and has a frequency of 500.
  ASSERT_DOUBLE_EQ(0.5, res);

  // TEST PART 2
  // Create a constant value expression to pass to ValueCondition.
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::REAL, execution::sql::Real(5.0));
  // Create a value condition to pass to SelectivityUtil.
  value_condition =
      ValueCondition(table2_real_col_oid_, "", parser::ExpressionType::COMPARE_EQUAL, std::move(const_value_expr_ptr));
  // Compute selectivity for col = 5.0.
  res = SelectivityUtil::ComputeSelectivity(table_stats_2_, value_condition);
  // The value 5.0 does not occur in top-k and has a frequency of 5.
  ASSERT_DOUBLE_EQ(0.005, res);

  // TEST PART 3
  for (int i = 1; i <= 1000; ++i) {
    // Create a constant value expression to pass to ValueCondition.
    const_value_expr_ptr = std::make_unique<parser::ConstantValueExpression>(
        type::TypeId::REAL, execution::sql::Real(static_cast<float>(i)));
    // Create a value condition to pass to SelectivityUtil.
    value_condition = ValueCondition(table3_real_col_oid_, "", parser::ExpressionType::COMPARE_EQUAL,
                                     std::move(const_value_expr_ptr));
    res = SelectivityUtil::ComputeSelectivity(table_stats_3_, value_condition);

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
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::REAL, execution::sql::Real(6.f));

  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(table1_real_col_oid_, "", parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO,
                                 std::move(const_value_expr_ptr));
  // Compute selectivity for col >= 6.0.
  double res = SelectivityUtil::ComputeSelectivity(table_stats_1_, value_condition);

  // The value 6 goes past the last bucket in the histogram and so selectivity must be predicted to be 0.
  ASSERT_DOUBLE_EQ(0.f, res);

  // TEST PART 2
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::REAL, execution::sql::Real(3.f));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(table1_real_col_oid_, "", parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO,
                                   std::move(const_value_expr_ptr));

  // Compute selectivity for col >= 3.0.
  res = SelectivityUtil::ComputeSelectivity(table_stats_1_, value_condition);

  ASSERT_DOUBLE_EQ(0.7, res);

  // TEST PART 3
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::REAL, execution::sql::Real(0.f));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(table1_real_col_oid_, "", parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO,
                                   std::move(const_value_expr_ptr));

  // Compute selectivity for col >= 0.0.
  res = SelectivityUtil::ComputeSelectivity(table_stats_1_, value_condition);

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
  ValueCondition value_condition(table1_int_col_oid_, "", parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO,
                                 std::move(const_value_expr_ptr));
  // Compute selectivity for col <= 6.
  double res = SelectivityUtil::ComputeSelectivity(table_stats_1_, value_condition);

  ASSERT_DOUBLE_EQ(0.6875, res);

  // TEST PART 2
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(3));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(table1_int_col_oid_, "", parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO,
                                   std::move(const_value_expr_ptr));
  // Compute selectivity for col <= 3.
  res = SelectivityUtil::ComputeSelectivity(table_stats_1_, value_condition);

  // True value should be 0.3. TODO(arvindsaik) Can make '<=' better here.
  ASSERT_DOUBLE_EQ(0.2, res);

  // TEST PART 3
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(0));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(table1_int_col_oid_, "", parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO,
                                   std::move(const_value_expr_ptr));

  res = SelectivityUtil::ComputeSelectivity(table_stats_1_, value_condition);

  ASSERT_DOUBLE_EQ(0.1, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestFloatNull) {
  ValueCondition value_condition(catalog::col_oid_t(2), "", parser::ExpressionType::OPERATOR_IS_NULL, nullptr);
  double res = SelectivityUtil::ComputeSelectivity(table_stats_2_, value_condition);
  ASSERT_DOUBLE_EQ(1 - 900.0 / 1000.0, res);

  value_condition = ValueCondition(table2_real_col_oid_, "", parser::ExpressionType::OPERATOR_IS_NOT_NULL, nullptr);
  res = SelectivityUtil::ComputeSelectivity(table_stats_2_, value_condition);
  ASSERT_DOUBLE_EQ(900.0 / 1000.0, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestIntegerGreaterThan) {
  // TEST PART 1
  // Create a constant value expression to pass to ValueCondition.
  // Floating point numbers have DECIMAL type and are represented as Reals in execution layer.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(6));

  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(table1_int_col_oid_, "", parser::ExpressionType::COMPARE_GREATER_THAN,
                                 std::move(const_value_expr_ptr));
  // Compute selectivity for col > 6.
  double res = SelectivityUtil::ComputeSelectivity(table_stats_1_, value_condition);

  // True value is 0.1. Acceptable error.
  ASSERT_DOUBLE_EQ(0.11249999999999999, res);

  // TEST PART 2
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(3));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(table1_int_col_oid_, "", parser::ExpressionType::COMPARE_GREATER_THAN,
                                   std::move(const_value_expr_ptr));
  // Compute selectivity for col > 3.
  res = SelectivityUtil::ComputeSelectivity(table_stats_1_, value_condition);

  // True value is 0.5. TODO(arvindsk) Making '<=' better will fix this.
  ASSERT_DOUBLE_EQ(0.6, res);

  // TEST PART 3
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(0));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(table1_int_col_oid_, "", parser::ExpressionType::COMPARE_GREATER_THAN,
                                   std::move(const_value_expr_ptr));
  // Compute selectivity for col > 0.
  res = SelectivityUtil::ComputeSelectivity(table_stats_1_, value_condition);

  ASSERT_FLOAT_EQ(0.7, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestIntegerNotEqual) {
  // Create a constant value expression to pass to ValueCondition.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(4));
  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(table1_int_col_oid_, "", parser::ExpressionType::COMPARE_NOT_EQUAL,
                                 std::move(const_value_expr_ptr));
  // Compute selectivity for col != 4.
  double res = SelectivityUtil::ComputeSelectivity(table_stats_1_, value_condition);

  ASSERT_DOUBLE_EQ(0.6, res);

  // Create a constant value expression to pass to ValueCondition.
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(0));

  // Create a value condition to pass to SelectivityUtil.
  value_condition = ValueCondition(table1_int_col_oid_, "", parser::ExpressionType::COMPARE_NOT_EQUAL,
                                   std::move(const_value_expr_ptr));

  // Compute selectivity for col != 0.
  res = SelectivityUtil::ComputeSelectivity(table_stats_1_, value_condition);

  ASSERT_DOUBLE_EQ(0.7, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestIntegerNull) {
  ValueCondition value_condition(table1_int_col_oid_, "", parser::ExpressionType::OPERATOR_IS_NULL, nullptr);
  double res = SelectivityUtil::ComputeSelectivity(table_stats_1_, value_condition);
  ASSERT_DOUBLE_EQ(1 - 8.0 / 10.0, res);

  value_condition = ValueCondition(table1_int_col_oid_, "", parser::ExpressionType::OPERATOR_IS_NOT_NULL, nullptr);
  res = SelectivityUtil::ComputeSelectivity(table_stats_1_, value_condition);
  ASSERT_DOUBLE_EQ(8.0 / 10.0, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestBoolEqual) {
  // TEST PART 1
  // Create a constant value expression to pass to ValueCondition.
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  // Create a value condition to pass to SelectivityUtil.
  ValueCondition value_condition(table4_bool_col_oid_, "", parser::ExpressionType::COMPARE_EQUAL,
                                 std::move(const_value_expr_ptr));

  // Compute selectivity for col = true.
  double res = SelectivityUtil::ComputeSelectivity(table_stats_4_, value_condition);

  ASSERT_DOUBLE_EQ(0.6, res);

  // TEST PART 2
  // Create a constant value expression to pass to ValueCondition.
  const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(false));
  // Create a value condition to pass to SelectivityUtil.
  value_condition =
      ValueCondition(table4_bool_col_oid_, "", parser::ExpressionType::COMPARE_EQUAL, std::move(const_value_expr_ptr));

  // Compute selectivity for col = true.
  res = SelectivityUtil::ComputeSelectivity(table_stats_4_, value_condition);

  ASSERT_DOUBLE_EQ(0.2, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestBoolNull) {
  ValueCondition value_condition(table4_bool_col_oid_, "", parser::ExpressionType::OPERATOR_IS_NULL, nullptr);
  double res = SelectivityUtil::ComputeSelectivity(table_stats_4_, value_condition);
  ASSERT_DOUBLE_EQ(1 - 80.0 / 100.0, res);

  value_condition = ValueCondition(table4_bool_col_oid_, "", parser::ExpressionType::OPERATOR_IS_NOT_NULL, nullptr);
  res = SelectivityUtil::ComputeSelectivity(table_stats_4_, value_condition);
  ASSERT_DOUBLE_EQ(80.0 / 100.0, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestDateEqual) {
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(
          type::TypeId::DATE, execution::sql::DateVal(execution::sql::Date::FromYMD(1995, 8, 6)));
  ValueCondition value_condition(table5_date_col_oid_, "", parser::ExpressionType::COMPARE_EQUAL,
                                 std::move(const_value_expr_ptr));
  double res = SelectivityUtil::ComputeSelectivity(table_stats_5_, value_condition);
  ASSERT_DOUBLE_EQ(2.0 / 5.0, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestTimestampEqual) {
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(
          type::TypeId::TIMESTAMP,
          execution::sql::TimestampVal(execution::sql::Timestamp::FromYMDHMS(2020, 4, 20, 3, 20, 56)));
  ValueCondition value_condition(table5_timestamp_col_oid_, "", parser::ExpressionType::COMPARE_EQUAL,
                                 std::move(const_value_expr_ptr));
  double res = SelectivityUtil::ComputeSelectivity(table_stats_5_, value_condition);
  ASSERT_DOUBLE_EQ(1.0 / 5.0, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestVarcharEqual) {
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::VARCHAR,
                                                        execution::sql::StringVal(storage::VarlenEntry::Create("Foo")));
  ValueCondition value_condition(catalog::col_oid_t(1), "", parser::ExpressionType::COMPARE_EQUAL,
                                 std::move(const_value_expr_ptr));
  double res = SelectivityUtil::ComputeSelectivity(table_stats_6_, value_condition);
  ASSERT_DOUBLE_EQ(1.0 / 2.0, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestVarcharLessThan) {
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::VARCHAR,
                                                        execution::sql::StringVal(storage::VarlenEntry::Create("Foo")));
  ValueCondition value_condition(catalog::col_oid_t(1), "", parser::ExpressionType::COMPARE_LESS_THAN,
                                 std::move(const_value_expr_ptr));
  double res = SelectivityUtil::ComputeSelectivity(table_stats_6_, value_condition);
  ASSERT_DOUBLE_EQ(1.0 / 2.0, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestVarcharGreaterThanOrEqualTo) {
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::VARCHAR,
                                                        execution::sql::StringVal(storage::VarlenEntry::Create("Bar")));
  ValueCondition value_condition(catalog::col_oid_t(1), "", parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO,
                                 std::move(const_value_expr_ptr));
  double res = SelectivityUtil::ComputeSelectivity(table_stats_6_, value_condition);
  ASSERT_DOUBLE_EQ(1.0, res);
}

// NOLINTNEXTLINE
TEST_F(SelectivityUtilTests, TestEmptyEqual) {
  std::unique_ptr<parser::ConstantValueExpression> const_value_expr_ptr =
      std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(666));
  ValueCondition value_condition(table6_varchar_col_oid_, "", parser::ExpressionType::COMPARE_EQUAL,
                                 std::move(const_value_expr_ptr));
  double res = SelectivityUtil::ComputeSelectivity(table_stats_7_, value_condition);
  ASSERT_DOUBLE_EQ(0, res);
}

}  // namespace noisepage::optimizer
