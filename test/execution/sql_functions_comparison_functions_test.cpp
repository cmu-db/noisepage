#include <limits>
#include <memory>
#include <random>
#include <vector>

#include "execution/exec/execution_settings.h"
#include "execution/sql/functions/comparison_functions.h"
#include "execution/sql/value.h"
#include "execution/tpl_test.h"
#include "execution/util/timer.h"

namespace noisepage::execution::sql::test {

class ComparisonFunctionsTests : public TplTest {};

// NOLINTNEXTLINE
TEST_F(ComparisonFunctionsTests, NullComparison) {
// Nulls
#define CHECK_NULL(TYPE, OP, INITIAL)             \
  {                                               \
    TYPE a = TYPE::Null(), b(INITIAL);            \
    BoolVal result(false);                        \
    ComparisonFunctions::OP##TYPE(&result, a, b); \
    EXPECT_TRUE(result.is_null_);                 \
  }
#define CHECK_NULL_FOR_ALL_COMPARISONS(TYPE, INITIAL) \
  CHECK_NULL(TYPE, Eq, INITIAL)                       \
  CHECK_NULL(TYPE, Ge, INITIAL)                       \
  CHECK_NULL(TYPE, Gt, INITIAL)                       \
  CHECK_NULL(TYPE, Le, INITIAL)                       \
  CHECK_NULL(TYPE, Lt, INITIAL)                       \
  CHECK_NULL(TYPE, Ne, INITIAL)

  CHECK_NULL_FOR_ALL_COMPARISONS(BoolVal, true);
  CHECK_NULL_FOR_ALL_COMPARISONS(Integer, 0);
  CHECK_NULL_FOR_ALL_COMPARISONS(Real, 0.0);
  CHECK_NULL_FOR_ALL_COMPARISONS(StringVal, "");

#undef CHECK_NULL_FOR_ALL_COMPARISONS
#undef CHECK_NULL
}

#define CHECK_OP(TYPE, OP, INPUT1, INPUT2, EXPECTED) \
  {                                                  \
    TYPE a(INPUT1), b(INPUT2);                       \
    BoolVal result(false);                           \
    ComparisonFunctions::OP##TYPE(&result, a, b);    \
    EXPECT_FALSE(result.is_null_);                   \
    EXPECT_EQ(EXPECTED, result.val_);                \
  }
#define CHECK_ALL_COMPARISONS(TYPE, INPUT1, INPUT2)      \
  CHECK_OP(TYPE, Eq, INPUT1, INPUT2, (INPUT1 == INPUT2)) \
  CHECK_OP(TYPE, Ge, INPUT1, INPUT2, (INPUT1 >= INPUT2)) \
  CHECK_OP(TYPE, Gt, INPUT1, INPUT2, (INPUT1 > INPUT2))  \
  CHECK_OP(TYPE, Le, INPUT1, INPUT2, (INPUT1 <= INPUT2)) \
  CHECK_OP(TYPE, Lt, INPUT1, INPUT2, (INPUT1 < INPUT2))  \
  CHECK_OP(TYPE, Ne, INPUT1, INPUT2, (INPUT1 != INPUT2))

// NOLINTNEXTLINE
TEST_F(ComparisonFunctionsTests, IntegerComparison) {
  CHECK_ALL_COMPARISONS(Integer, 10, 20);
  CHECK_ALL_COMPARISONS(Integer, -10, 20);
  CHECK_ALL_COMPARISONS(Integer, 0, 0);
  CHECK_ALL_COMPARISONS(Integer, -213, -376);
}

// NOLINTNEXTLINE
TEST_F(ComparisonFunctionsTests, RealComparison) {
  CHECK_ALL_COMPARISONS(Real, 0.0, 0.0);
  CHECK_ALL_COMPARISONS(Real, 1.0, 0.0);
  CHECK_ALL_COMPARISONS(Real, -1.0, 0.0);
  CHECK_ALL_COMPARISONS(Real, 1.0, -2.0);
}

// NOLINTNEXTLINE
TEST_F(ComparisonFunctionsTests, BoolValComparison) {
  CHECK_ALL_COMPARISONS(BoolVal, false, false);
  CHECK_ALL_COMPARISONS(BoolVal, true, false);
  CHECK_ALL_COMPARISONS(BoolVal, false, true);
  CHECK_ALL_COMPARISONS(BoolVal, true, true);
}

#undef CHECK_ALL_COMPARISONS
#undef CHECK_NULL

// NOLINTNEXTLINE
TEST_F(ComparisonFunctionsTests, StringComparison) {
#define CHECK(INPUT1, INPUT2, OP, EXPECTED)            \
  {                                                    \
    BoolVal result = BoolVal::Null();                  \
    StringVal x(INPUT1), y(INPUT2);                    \
    ComparisonFunctions::OP##StringVal(&result, x, y); \
    EXPECT_FALSE(result.is_null_);                     \
    EXPECT_EQ(EXPECTED, result.val_);                  \
  }

  // Same sizes
  CHECK("test", "test", Eq, true);
  CHECK("test", "test", Ge, true);
  CHECK("test", "test", Gt, false);
  CHECK("test", "test", Le, true);
  CHECK("test", "test", Lt, false);
  CHECK("test", "test", Ne, false);

  // Different sizes
  CHECK("test", "testholla", Eq, false);
  CHECK("test", "testholla", Ge, false);
  CHECK("test", "testholla", Gt, false);
  CHECK("test", "testholla", Le, true);
  CHECK("test", "testholla", Lt, true);
  CHECK("test", "testholla", Ne, true);

  // Different sizes
  CHECK("testholla", "test", Eq, false);
  CHECK("testholla", "test", Ge, true);
  CHECK("testholla", "test", Gt, true);
  CHECK("testholla", "test", Le, false);
  CHECK("testholla", "test", Lt, false);
  CHECK("testholla", "test", Ne, true);

  CHECK("testholla", "", Eq, false);
  CHECK("testholla", "", Ge, true);
  CHECK("testholla", "", Gt, true);
  CHECK("testholla", "", Le, false);
  CHECK("testholla", "", Lt, false);
  CHECK("testholla", "", Ne, true);

  CHECK("", "", Eq, true);
  CHECK("", "", Ge, true);
  CHECK("", "", Gt, false);
  CHECK("", "", Le, true);
  CHECK("", "", Lt, false);
  CHECK("", "", Ne, false);

#undef CHECK
}

}  // namespace noisepage::execution::sql::test
