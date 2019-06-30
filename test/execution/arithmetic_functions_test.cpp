#include <limits>
#include <memory>
#include <random>
#include <vector>

#include "execution/tpl_test.h"  // NOLINT

#include "execution/sql/functions/arithmetic_functions.h"
#include "execution/sql/value.h"
#include "execution/util/timer.h"

namespace tpl::sql::test {

class ArithmeticFunctionsTests : public TplTest {
 protected:
  inline double cotan(const double arg) { return (1.0 / std::tan(arg)); }
};

// NOLINTNEXTLINE
TEST_F(ArithmeticFunctionsTests, IntegerValue) {
  // Nulls
  {
    Integer a(0), b = Integer::Null(), result(0);

    ArithmeticFunctions::Add(&result, a, b);
    EXPECT_TRUE(result.is_null);

    result = Integer(0);
    ArithmeticFunctions::Sub(&result, a, b);
    EXPECT_TRUE(result.is_null);

    result = Integer(0);
    ArithmeticFunctions::Mul(&result, a, b);
    EXPECT_TRUE(result.is_null);

    bool div_by_zero = false;
    result = Integer(0);
    ArithmeticFunctions::IntDiv(&result, a, b, &div_by_zero);
    EXPECT_TRUE(result.is_null);
  }

  // Proper
  {
    const auto aval = 10, bval = 4;
    Integer a(aval), b(bval), result(0);

    ArithmeticFunctions::Add(&result, a, b);
    EXPECT_FALSE(result.is_null);
    EXPECT_EQ(aval + bval, result.val);

    result = Integer(0);
    ArithmeticFunctions::Sub(&result, a, b);
    EXPECT_FALSE(result.is_null);
    EXPECT_EQ(aval - bval, result.val);

    result = Integer(0);
    ArithmeticFunctions::Mul(&result, a, b);
    EXPECT_FALSE(result.is_null);
    EXPECT_EQ(aval * bval, result.val);

    bool div_by_zero = false;
    result = Integer(0);
    ArithmeticFunctions::IntDiv(&result, a, b, &div_by_zero);
    EXPECT_FALSE(result.is_null);
    EXPECT_EQ(aval / bval, result.val);
  }

  // Overflow
  {
    const auto aval = std::numeric_limits<i64>::max() - 1, bval = 4l;
    Integer a(aval), b(bval), result(0);

    bool overflow = false;
    ArithmeticFunctions::Add(&result, a, b, &overflow);
    EXPECT_FALSE(result.is_null);
    EXPECT_TRUE(overflow);
  }

  {
    const auto aval = std::numeric_limits<i64>::min() + 1, bval = 4l;
    Integer a(aval), b(bval), result(0);

    bool overflow = false;
    ArithmeticFunctions::Sub(&result, a, b, &overflow);
    EXPECT_FALSE(result.is_null);
    EXPECT_TRUE(overflow);
  }

  {
    const auto aval = std::numeric_limits<i64>::max() - 1, bval = aval;
    Integer a(aval), b(bval), result(0);

    bool overflow = false;
    ArithmeticFunctions::Mul(&result, a, b, &overflow);
    EXPECT_FALSE(result.is_null);
    EXPECT_TRUE(overflow);
  }
}

// NOLINTNEXTLINE
TEST_F(ArithmeticFunctionsTests, RealValue) {
  // Nulls
  {
    Real a(0.0), b = Real::Null(), result(0.0);

    ArithmeticFunctions::Add(&result, a, b);
    EXPECT_TRUE(result.is_null);

    result = Real(0.0);
    ArithmeticFunctions::Sub(&result, a, b);
    EXPECT_TRUE(result.is_null);

    result = Real(0.0);
    ArithmeticFunctions::Mul(&result, a, b);
    EXPECT_TRUE(result.is_null);

    bool div_by_zero = false;
    result = Real(0.0);
    ArithmeticFunctions::Div(&result, a, b, &div_by_zero);
    EXPECT_TRUE(result.is_null);
  }

  // Proper
  {
    const auto aval = 10.0, bval = 4.0;
    Real a(aval), b(bval), result(0.0);

    ArithmeticFunctions::Add(&result, a, b);
    EXPECT_FALSE(result.is_null);
    EXPECT_EQ(aval + bval, result.val);

    result = Real(0.0);
    ArithmeticFunctions::Sub(&result, a, b);
    EXPECT_FALSE(result.is_null);
    EXPECT_EQ(aval - bval, result.val);

    result = Real(0.0);
    ArithmeticFunctions::Mul(&result, a, b);
    EXPECT_FALSE(result.is_null);
    EXPECT_EQ(aval * bval, result.val);

    bool div_by_zero = false;
    result = Real(0.0);
    ArithmeticFunctions::Div(&result, a, b, &div_by_zero);
    EXPECT_FALSE(result.is_null);
    EXPECT_EQ(aval / bval, result.val);
  }
}

// NOLINTNEXTLINE
TEST_F(ArithmeticFunctionsTests, PiAndE) {
  {
    Real pi(0.0);
    ArithmeticFunctions::Pi(&pi);
    EXPECT_FALSE(pi.is_null);
    EXPECT_DOUBLE_EQ(M_PI, pi.val);
  }

  {
    Real e(0.0);
    ArithmeticFunctions::E(&e);
    EXPECT_FALSE(e.is_null);
    EXPECT_DOUBLE_EQ(M_E, e.val);
  }
}

// NOLINTNEXTLINE
TEST_F(ArithmeticFunctionsTests, TrigFunctions) {
  std::vector<double> inputs, arc_inputs;

  std::mt19937 gen;
  std::uniform_real_distribution dist(1.0, 1000.0);
  std::uniform_real_distribution arc_dist(-1.0, 1.0);
  for (u32 i = 0; i < 100; i++) {
    inputs.push_back(dist(gen));
    arc_inputs.push_back(arc_dist(gen));
  }

#define CHECK_HANDLES_NULL(TPL_FUNC, C_FUNC)  \
  {                                           \
    Real arg = Real::Null();                  \
    Real ret(0.0);                            \
    ArithmeticFunctions::TPL_FUNC(&ret, arg); \
    EXPECT_TRUE(ret.is_null);                 \
  }
#define CHECK_HANDLES_NONNULL(TPL_FUNC, C_FUNC) \
  {                                             \
    Real arg(input);                            \
    Real ret(0.0);                              \
    ArithmeticFunctions::TPL_FUNC(&ret, arg);   \
    EXPECT_FALSE(ret.is_null);                  \
    EXPECT_DOUBLE_EQ(C_FUNC(input), ret.val);   \
  }

#define CHECK_SQL_FUNC(TPL_FUNC, C_FUNC) \
  CHECK_HANDLES_NULL(TPL_FUNC, C_FUNC)   \
  CHECK_HANDLES_NONNULL(TPL_FUNC, C_FUNC)

  // Check some of the trig functions on all inputs
  for (const auto input : inputs) {
    CHECK_SQL_FUNC(Cos, std::cos);
    CHECK_SQL_FUNC(Cot, cotan);
    CHECK_SQL_FUNC(Sin, std::sin);
    CHECK_SQL_FUNC(Tan, std::tan);
    CHECK_SQL_FUNC(Cosh, std::cosh);
    CHECK_SQL_FUNC(Tanh, std::tanh);
    CHECK_SQL_FUNC(Sinh, std::sinh);
  }

  for (const auto input : arc_inputs) {
    CHECK_SQL_FUNC(Acos, std::acos);
    CHECK_SQL_FUNC(Asin, std::asin);
    CHECK_SQL_FUNC(Atan, std::atan);
  }

#undef CHECK_SQL_FUNC
#undef CHECK_HANDLES_NONNULL
#undef CHECK_HANDLES_NULL
}

// NOLINTNEXTLINE
TEST_F(ArithmeticFunctionsTests, MathFuncs) {
#define CHECK_HANDLES_NULL(TPL_FUNC, C_FUNC)  \
  {                                           \
    Real arg = Real::Null();                  \
    Real ret(0.0);                            \
    ArithmeticFunctions::TPL_FUNC(&ret, arg); \
    EXPECT_TRUE(ret.is_null);                 \
  }
#define CHECK_HANDLES_NONNULL(TPL_FUNC, C_FUNC, INPUT) \
  {                                                    \
    Real arg(INPUT);                                   \
    Real ret(0.0);                                     \
    ArithmeticFunctions::TPL_FUNC(&ret, arg);          \
    EXPECT_FALSE(ret.is_null);                         \
    EXPECT_DOUBLE_EQ(C_FUNC(INPUT), ret.val);          \
  }

#define CHECK_SQL_FUNC(TPL_FUNC, C_FUNC, INPUT) \
  CHECK_HANDLES_NULL(TPL_FUNC, C_FUNC)          \
  CHECK_HANDLES_NONNULL(TPL_FUNC, C_FUNC, INPUT)

  CHECK_SQL_FUNC(Abs, std::fabs, -4.4);
  CHECK_SQL_FUNC(Abs, std::fabs, 1.10);

  CHECK_SQL_FUNC(Sqrt, std::sqrt, 4.0);
  CHECK_SQL_FUNC(Sqrt, std::sqrt, 1.0);

  CHECK_SQL_FUNC(Cbrt, std::cbrt, 4.0);
  CHECK_SQL_FUNC(Cbrt, std::cbrt, 1.0);

  CHECK_SQL_FUNC(Exp, std::exp, 4.0);
  CHECK_SQL_FUNC(Exp, std::exp, 1.0);

  CHECK_SQL_FUNC(Ceil, std::ceil, 4.4);
  CHECK_SQL_FUNC(Ceil, std::ceil, 1.2);
  CHECK_SQL_FUNC(Ceil, std::ceil, -100.1);
  CHECK_SQL_FUNC(Ceil, std::ceil, -100.34234);

  CHECK_SQL_FUNC(Floor, std::floor, 4.4);
  CHECK_SQL_FUNC(Floor, std::floor, 1.2);
  CHECK_SQL_FUNC(Floor, std::floor, 50.1);
  CHECK_SQL_FUNC(Floor, std::floor, 100.234);

  CHECK_SQL_FUNC(Ln, std::log, 4.4);
  CHECK_SQL_FUNC(Ln, std::log, 1.2);
  CHECK_SQL_FUNC(Ln, std::log, 50.1);
  CHECK_SQL_FUNC(Ln, std::log, 100.234);

  CHECK_SQL_FUNC(Log2, std::log2, 4.4);
  CHECK_SQL_FUNC(Log2, std::log2, 1.2);
  CHECK_SQL_FUNC(Log2, std::log2, 50.1);
  CHECK_SQL_FUNC(Log2, std::log2, 100.234);

  CHECK_SQL_FUNC(Log10, std::log10, 4.4);
  CHECK_SQL_FUNC(Log10, std::log10, 1.10);
  CHECK_SQL_FUNC(Log10, std::log10, 50.123);
  CHECK_SQL_FUNC(Log10, std::log10, 100.234);

#undef CHECK_SQL_FUNC
#undef CHECK_HANDLES_NONNULL
#undef CHECK_HANDLES_NULL

  // Sign
  {
    Real input = Real::Null(), result = Real::Null();
    ArithmeticFunctions::Sign(&result, input);
    EXPECT_TRUE(result.is_null);

    input = Real(13523.0);
    ArithmeticFunctions::Sign(&result, input);
    EXPECT_FALSE(result.is_null);
    EXPECT_DOUBLE_EQ(1.0, result.val);

    input = Real(-1231.0);
    ArithmeticFunctions::Sign(&result, input);
    EXPECT_FALSE(result.is_null);
    EXPECT_DOUBLE_EQ(-1.0, result.val);

    input = Real(0.0f);
    ArithmeticFunctions::Sign(&result, input);
    EXPECT_FALSE(result.is_null);
    EXPECT_DOUBLE_EQ(0.0, result.val);
  };
}

}  // namespace tpl::sql::test
