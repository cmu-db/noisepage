#include <string>

#include "execution/tpl_test.h"

#include "execution/sql/functions/comparison_functions.h"
#include "execution/sql/value.h"

namespace terrier::execution::sql::test {
class DateFunctionsTests : public TplTest {};

#define CHECK_COMPARSION_NULL(comp, res, lhs, rhs) \
  ComparisonFunctions::comp##Date(&res, lhs, rhs); \
  ASSERT_TRUE(res.is_null_)

#define CHECK_COMPARSION_TRUE(comp, res, lhs, rhs) \
  ComparisonFunctions::comp##Date(&res, lhs, rhs); \
  ASSERT_TRUE(!res.is_null_ && res.val_)

#define CHECK_COMPARSION_FALSE(comp, res, lhs, rhs) \
  ComparisonFunctions::comp##Date(&res, lhs, rhs);  \
  ASSERT_TRUE(!res.is_null_ && !res.val_)

// NOLINTNEXTLINE
TEST_F(DateFunctionsTests, DateNilTest) {
  Date nil_date1(Date::Null());
  Date nil_date2(Date::Null());
  Date date(2019, 8, 11);
  BoolVal res{false};

  // Nil comparison should return nil
  // Nil is lhs
  CHECK_COMPARSION_NULL(Eq, res, nil_date1, date);
  CHECK_COMPARSION_NULL(Ne, res, nil_date1, date);
  CHECK_COMPARSION_NULL(Le, res, nil_date1, date);
  CHECK_COMPARSION_NULL(Lt, res, nil_date1, date);
  CHECK_COMPARSION_NULL(Ge, res, nil_date1, date);
  CHECK_COMPARSION_NULL(Gt, res, nil_date1, date);

  // Nil is rhs
  CHECK_COMPARSION_NULL(Eq, res, date, nil_date1);
  CHECK_COMPARSION_NULL(Ne, res, date, nil_date1);
  CHECK_COMPARSION_NULL(Le, res, date, nil_date1);
  CHECK_COMPARSION_NULL(Lt, res, date, nil_date1);
  CHECK_COMPARSION_NULL(Ge, res, date, nil_date1);
  CHECK_COMPARSION_NULL(Gt, res, date, nil_date1);

  // Nil in both arguments
  CHECK_COMPARSION_NULL(Eq, res, nil_date1, nil_date2);
  CHECK_COMPARSION_NULL(Ne, res, nil_date1, nil_date2);
  CHECK_COMPARSION_NULL(Le, res, nil_date1, nil_date2);
  CHECK_COMPARSION_NULL(Lt, res, nil_date1, nil_date2);
  CHECK_COMPARSION_NULL(Ge, res, nil_date1, nil_date2);
  CHECK_COMPARSION_NULL(Gt, res, nil_date1, nil_date2);
}

// NOLINTNEXTLINE
TEST_F(DateFunctionsTests, EqualDateTest) {
  Date base1(2019, 8, 11);
  Date base2(2019, 8, 11);

  BoolVal res{false};
  // ==, <=, >= should be true
  CHECK_COMPARSION_TRUE(Eq, res, base1, base2);
  CHECK_COMPARSION_TRUE(Le, res, base1, base2);
  CHECK_COMPARSION_TRUE(Ge, res, base1, base2);
  // !=, <, > should be false
  CHECK_COMPARSION_FALSE(Ne, res, base1, base2);
  CHECK_COMPARSION_FALSE(Lt, res, base1, base2);
  CHECK_COMPARSION_FALSE(Gt, res, base1, base2);
}

// NOLINTNEXTLINE
TEST_F(DateFunctionsTests, DifferentDatesTest) {
  Date base(2019, 8, 11);
  Date past_day(2019, 8, 10);
  Date past_month(2019, 7, 28);
  Date past_year(2018, 12, 28);

  BoolVal res{false};
  // >, >=, != should be true if base is lhs
  CHECK_COMPARSION_TRUE(Gt, res, base, past_day);
  CHECK_COMPARSION_TRUE(Gt, res, base, past_month);
  CHECK_COMPARSION_TRUE(Gt, res, base, past_year);
  CHECK_COMPARSION_TRUE(Ge, res, base, past_day);
  CHECK_COMPARSION_TRUE(Ge, res, base, past_month);
  CHECK_COMPARSION_TRUE(Ge, res, base, past_year);
  CHECK_COMPARSION_TRUE(Ne, res, base, past_day);
  CHECK_COMPARSION_TRUE(Ne, res, base, past_month);
  CHECK_COMPARSION_TRUE(Ne, res, base, past_year);

  // ==, <, <= should be false if base is lhs
  CHECK_COMPARSION_FALSE(Eq, res, base, past_day);
  CHECK_COMPARSION_FALSE(Eq, res, base, past_month);
  CHECK_COMPARSION_FALSE(Eq, res, base, past_year);
  CHECK_COMPARSION_FALSE(Lt, res, base, past_day);
  CHECK_COMPARSION_FALSE(Lt, res, base, past_month);
  CHECK_COMPARSION_FALSE(Lt, res, base, past_year);
  CHECK_COMPARSION_FALSE(Le, res, base, past_day);
  CHECK_COMPARSION_FALSE(Le, res, base, past_month);
  CHECK_COMPARSION_FALSE(Le, res, base, past_year);

  // <, <=, != should be true if base is rhs
  CHECK_COMPARSION_TRUE(Lt, res, past_day, base);
  CHECK_COMPARSION_TRUE(Lt, res, past_month, base);
  CHECK_COMPARSION_TRUE(Lt, res, past_year, base);
  CHECK_COMPARSION_TRUE(Le, res, past_day, base);
  CHECK_COMPARSION_TRUE(Le, res, past_month, base);
  CHECK_COMPARSION_TRUE(Le, res, past_year, base);
  CHECK_COMPARSION_TRUE(Ne, res, past_day, base);
  CHECK_COMPARSION_TRUE(Ne, res, past_month, base);
  CHECK_COMPARSION_TRUE(Ne, res, past_year, base);

  // >, >=, == should be false id base is rhs
  CHECK_COMPARSION_FALSE(Gt, res, past_day, base);
  CHECK_COMPARSION_FALSE(Gt, res, past_month, base);
  CHECK_COMPARSION_FALSE(Gt, res, past_year, base);
  CHECK_COMPARSION_FALSE(Ge, res, past_day, base);
  CHECK_COMPARSION_FALSE(Ge, res, past_month, base);
  CHECK_COMPARSION_FALSE(Ge, res, past_year, base);
  CHECK_COMPARSION_FALSE(Eq, res, past_day, base);
  CHECK_COMPARSION_FALSE(Eq, res, past_month, base);
  CHECK_COMPARSION_FALSE(Eq, res, past_year, base);
}

// NOLINTNEXTLINE
TEST_F(DateFunctionsTests, DateExtractTest) {
  Date date(2019, 8, 11);
  ASSERT_EQ(ValUtil::ExtractYear(date), 2019);
  ASSERT_EQ(ValUtil::ExtractMonth(date), 8);
  ASSERT_EQ(ValUtil::ExtractDay(date), 11);
}
}  // namespace terrier::execution::sql::test
