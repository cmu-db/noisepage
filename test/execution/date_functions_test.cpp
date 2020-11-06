#include <string>

#include "execution/sql/functions/comparison_functions.h"
#include "execution/sql/value.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::sql::test {
class DateFunctionsTests : public TplTest {};

#define CHECK_COMPARISON_NULL(comp, res, lhs, rhs)    \
  ComparisonFunctions::comp##DateVal(&res, lhs, rhs); \
  ASSERT_TRUE(res.is_null_)

#define CHECK_COMPARISON_TRUE(comp, res, lhs, rhs)    \
  ComparisonFunctions::comp##DateVal(&res, lhs, rhs); \
  ASSERT_TRUE(!res.is_null_ && res.val_)

#define CHECK_COMPARISON_FALSE(comp, res, lhs, rhs)   \
  ComparisonFunctions::comp##DateVal(&res, lhs, rhs); \
  ASSERT_TRUE(!res.is_null_ && !res.val_)

// NOLINTNEXTLINE
TEST_F(DateFunctionsTests, DateNilTest) {
  DateVal nil_date1(DateVal::Null());
  DateVal nil_date2(DateVal::Null());
  DateVal date(Date::FromYMD(2019, 8, 11));
  BoolVal res{false};

  // Nil comparison should return nil
  // Nil is lhs
  CHECK_COMPARISON_NULL(Eq, res, nil_date1, date);
  CHECK_COMPARISON_NULL(Ne, res, nil_date1, date);
  CHECK_COMPARISON_NULL(Le, res, nil_date1, date);
  CHECK_COMPARISON_NULL(Lt, res, nil_date1, date);
  CHECK_COMPARISON_NULL(Ge, res, nil_date1, date);
  CHECK_COMPARISON_NULL(Gt, res, nil_date1, date);

  // Nil is rhs
  CHECK_COMPARISON_NULL(Eq, res, date, nil_date1);
  CHECK_COMPARISON_NULL(Ne, res, date, nil_date1);
  CHECK_COMPARISON_NULL(Le, res, date, nil_date1);
  CHECK_COMPARISON_NULL(Lt, res, date, nil_date1);
  CHECK_COMPARISON_NULL(Ge, res, date, nil_date1);
  CHECK_COMPARISON_NULL(Gt, res, date, nil_date1);

  // Nil in both arguments
  CHECK_COMPARISON_NULL(Eq, res, nil_date1, nil_date2);
  CHECK_COMPARISON_NULL(Ne, res, nil_date1, nil_date2);
  CHECK_COMPARISON_NULL(Le, res, nil_date1, nil_date2);
  CHECK_COMPARISON_NULL(Lt, res, nil_date1, nil_date2);
  CHECK_COMPARISON_NULL(Ge, res, nil_date1, nil_date2);
  CHECK_COMPARISON_NULL(Gt, res, nil_date1, nil_date2);
}

// NOLINTNEXTLINE
TEST_F(DateFunctionsTests, EqualDateTest) {
  DateVal base1(Date::FromYMD(2019, 8, 11));
  DateVal base2(Date::FromYMD(2019, 8, 11));

  BoolVal res{false};
  // ==, <=, >= should be true
  CHECK_COMPARISON_TRUE(Eq, res, base1, base2);
  CHECK_COMPARISON_TRUE(Le, res, base1, base2);
  CHECK_COMPARISON_TRUE(Ge, res, base1, base2);
  // !=, <, > should be false
  CHECK_COMPARISON_FALSE(Ne, res, base1, base2);
  CHECK_COMPARISON_FALSE(Lt, res, base1, base2);
  CHECK_COMPARISON_FALSE(Gt, res, base1, base2);
}

// NOLINTNEXTLINE
TEST_F(DateFunctionsTests, DifferentDatesTest) {
  DateVal base(Date::FromYMD(2019, 8, 11));
  DateVal past_day(Date::FromYMD(2019, 8, 10));
  DateVal past_month(Date::FromYMD(2019, 7, 28));
  DateVal past_year(Date::FromYMD(2018, 12, 28));

  BoolVal res{false};
  // >, >=, != should be true if base is lhs
  CHECK_COMPARISON_TRUE(Gt, res, base, past_day);
  CHECK_COMPARISON_TRUE(Gt, res, base, past_month);
  CHECK_COMPARISON_TRUE(Gt, res, base, past_year);
  CHECK_COMPARISON_TRUE(Ge, res, base, past_day);
  CHECK_COMPARISON_TRUE(Ge, res, base, past_month);
  CHECK_COMPARISON_TRUE(Ge, res, base, past_year);
  CHECK_COMPARISON_TRUE(Ne, res, base, past_day);
  CHECK_COMPARISON_TRUE(Ne, res, base, past_month);
  CHECK_COMPARISON_TRUE(Ne, res, base, past_year);

  // ==, <, <= should be false if base is lhs
  CHECK_COMPARISON_FALSE(Eq, res, base, past_day);
  CHECK_COMPARISON_FALSE(Eq, res, base, past_month);
  CHECK_COMPARISON_FALSE(Eq, res, base, past_year);
  CHECK_COMPARISON_FALSE(Lt, res, base, past_day);
  CHECK_COMPARISON_FALSE(Lt, res, base, past_month);
  CHECK_COMPARISON_FALSE(Lt, res, base, past_year);
  CHECK_COMPARISON_FALSE(Le, res, base, past_day);
  CHECK_COMPARISON_FALSE(Le, res, base, past_month);
  CHECK_COMPARISON_FALSE(Le, res, base, past_year);

  // <, <=, != should be true if base is rhs
  CHECK_COMPARISON_TRUE(Lt, res, past_day, base);
  CHECK_COMPARISON_TRUE(Lt, res, past_month, base);
  CHECK_COMPARISON_TRUE(Lt, res, past_year, base);
  CHECK_COMPARISON_TRUE(Le, res, past_day, base);
  CHECK_COMPARISON_TRUE(Le, res, past_month, base);
  CHECK_COMPARISON_TRUE(Le, res, past_year, base);
  CHECK_COMPARISON_TRUE(Ne, res, past_day, base);
  CHECK_COMPARISON_TRUE(Ne, res, past_month, base);
  CHECK_COMPARISON_TRUE(Ne, res, past_year, base);

  // >, >=, == should be false id base is rhs
  CHECK_COMPARISON_FALSE(Gt, res, past_day, base);
  CHECK_COMPARISON_FALSE(Gt, res, past_month, base);
  CHECK_COMPARISON_FALSE(Gt, res, past_year, base);
  CHECK_COMPARISON_FALSE(Ge, res, past_day, base);
  CHECK_COMPARISON_FALSE(Ge, res, past_month, base);
  CHECK_COMPARISON_FALSE(Ge, res, past_year, base);
  CHECK_COMPARISON_FALSE(Eq, res, past_day, base);
  CHECK_COMPARISON_FALSE(Eq, res, past_month, base);
  CHECK_COMPARISON_FALSE(Eq, res, past_year, base);
}

// NOLINTNEXTLINE
TEST_F(DateFunctionsTests, DateExtractTest) {
  DateVal date(Date::FromYMD(2019, 8, 11));
  ASSERT_EQ(date.val_.ExtractYear(), 2019);
  ASSERT_EQ(date.val_.ExtractMonth(), 8);
  ASSERT_EQ(date.val_.ExtractDay(), 11);
}
}  // namespace noisepage::execution::sql::test
