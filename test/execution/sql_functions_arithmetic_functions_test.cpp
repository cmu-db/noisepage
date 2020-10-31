#include <limits>
#include <memory>
#include <random>
#include <vector>

#include "common/error/error_code.h"
#include "common/error/exception.h"
#include "execution/sql/functions/arithmetic_functions.h"
#include "execution/sql/value.h"
#include "execution/tpl_test.h"
#include "execution/util/timer.h"

namespace noisepage::execution::sql::test {

class ArithmeticFunctionsTests : public TplTest {
 protected:
  static double Cotan(const double arg) { return Cot<double>{}(arg); }
};

// NOLINTNEXTLINE
TEST_F(ArithmeticFunctionsTests, IntegerValue) {
  // Nulls
  {
    Integer a(0), b = Integer::Null(), result(0);

    ArithmeticFunctions::Add(&result, a, b);
    EXPECT_TRUE(result.is_null_);

    result = Integer(0);
    ArithmeticFunctions::Sub(&result, a, b);
    EXPECT_TRUE(result.is_null_);

    result = Integer(0);
    ArithmeticFunctions::Mul(&result, a, b);
    EXPECT_TRUE(result.is_null_);

    bool div_by_zero = false;
    result = Integer(0);
    ArithmeticFunctions::IntDiv(&result, a, b, &div_by_zero);
    EXPECT_TRUE(result.is_null_);
  }

  // Proper
  {
    const auto aval = 10, bval = 4;
    Integer a(aval), b(bval), result(0);

    ArithmeticFunctions::Add(&result, a, b);
    EXPECT_FALSE(result.is_null_);
    EXPECT_EQ(aval + bval, result.val_);

    result = Integer(0);
    ArithmeticFunctions::Sub(&result, a, b);
    EXPECT_FALSE(result.is_null_);
    EXPECT_EQ(aval - bval, result.val_);

    result = Integer(0);
    ArithmeticFunctions::Mul(&result, a, b);
    EXPECT_FALSE(result.is_null_);
    EXPECT_EQ(aval * bval, result.val_);

    bool div_by_zero = false;
    result = Integer(0);
    ArithmeticFunctions::IntDiv(&result, a, b, &div_by_zero);
    EXPECT_FALSE(result.is_null_);
    EXPECT_EQ(aval / bval, result.val_);

    result = Integer(0);
    ArithmeticFunctions::Abs(&result, a);
    EXPECT_FALSE(result.is_null_);
    EXPECT_EQ(aval, result.val_);

    const auto cval = -4, dval = 4;
    Integer c(cval), d(dval);

    result = Integer(0);
    ArithmeticFunctions::Abs(&result, c);
    EXPECT_FALSE(result.is_null_);
    EXPECT_EQ(dval, result.val_);
  }

  // Overflow
  {
    const auto aval = std::numeric_limits<int64_t>::max() - 1;
    const auto bval = 4l;
    Integer a(aval), b(bval), result(0);

    bool overflow = false;
    ArithmeticFunctions::Add(&result, a, b, &overflow);
    EXPECT_FALSE(result.is_null_);
    EXPECT_TRUE(overflow);
  }

  {
    const auto aval = std::numeric_limits<int64_t>::min() + 1;
    const auto bval = 4l;
    Integer a(aval), b(bval), result(0);

    bool overflow = false;
    ArithmeticFunctions::Sub(&result, a, b, &overflow);
    EXPECT_FALSE(result.is_null_);
    EXPECT_TRUE(overflow);
  }

  {
    const auto aval = std::numeric_limits<int64_t>::max() - 1, bval = aval;
    Integer a(aval), b(bval), result(0);

    bool overflow = false;
    ArithmeticFunctions::Mul(&result, a, b, &overflow);
    EXPECT_FALSE(result.is_null_);
    EXPECT_TRUE(overflow);
  }
}

// NOLINTNEXTLINE
TEST_F(ArithmeticFunctionsTests, RealValue) {
  // Nulls
  {
    Real a(0.0), b = Real::Null(), result(0.0);

    ArithmeticFunctions::Add(&result, a, b);
    EXPECT_TRUE(result.is_null_);

    result = Real(0.0);
    ArithmeticFunctions::Sub(&result, a, b);
    EXPECT_TRUE(result.is_null_);

    result = Real(0.0);
    ArithmeticFunctions::Mul(&result, a, b);
    EXPECT_TRUE(result.is_null_);

    bool div_by_zero = false;
    result = Real(0.0);
    ArithmeticFunctions::Div(&result, a, b, &div_by_zero);
    EXPECT_TRUE(result.is_null_);
  }

  // Proper
  {
    const auto aval = 10.0, bval = 4.0;
    Real a(aval), b(bval), result(0.0);

    ArithmeticFunctions::Add(&result, a, b);
    EXPECT_FALSE(result.is_null_);
    EXPECT_EQ(aval + bval, result.val_);

    result = Real(0.0);
    ArithmeticFunctions::Sub(&result, a, b);
    EXPECT_FALSE(result.is_null_);
    EXPECT_EQ(aval - bval, result.val_);

    result = Real(0.0);
    ArithmeticFunctions::Mul(&result, a, b);
    EXPECT_FALSE(result.is_null_);
    EXPECT_EQ(aval * bval, result.val_);

    bool div_by_zero = false;
    result = Real(0.0);
    ArithmeticFunctions::Div(&result, a, b, &div_by_zero);
    EXPECT_FALSE(result.is_null_);
    EXPECT_EQ(aval / bval, result.val_);

    result = Real(0.0);
    ArithmeticFunctions::Abs(&result, a);
    EXPECT_FALSE(result.is_null_);
    EXPECT_EQ(aval, result.val_);

    const auto cval = -4.8, dval = 4.8;
    Real c(cval);

    result = Real(0.0);
    ArithmeticFunctions::Abs(&result, c);
    EXPECT_FALSE(result.is_null_);
    EXPECT_EQ(dval, result.val_);
  }
}

// NOLINTNEXTLINE
TEST_F(ArithmeticFunctionsTests, PiAndE) {
  {
    Real pi(0.0);
    ArithmeticFunctions::Pi(&pi);
    EXPECT_FALSE(pi.is_null_);
    EXPECT_DOUBLE_EQ(M_PI, pi.val_);
  }

  {
    Real e(0.0);
    ArithmeticFunctions::E(&e);
    EXPECT_FALSE(e.is_null_);
    EXPECT_DOUBLE_EQ(M_E, e.val_);
  }
}

// NOLINTNEXTLINE
TEST_F(ArithmeticFunctionsTests, TrigFunctions) {
  std::vector<double> inputs, arc_inputs;

  std::mt19937 gen;
  std::uniform_real_distribution dist(1.0, 1000.0);
  std::uniform_real_distribution arc_dist(-1.0, 1.0);
  for (uint32_t i = 0; i < 100; i++) {
    inputs.push_back(dist(gen));
    arc_inputs.push_back(arc_dist(gen));
  }

#define CHECK_HANDLES_NULL(TPL_FUNC, C_FUNC)  \
  {                                           \
    Real arg = Real::Null();                  \
    Real ret(0.0);                            \
    ArithmeticFunctions::TPL_FUNC(&ret, arg); \
    EXPECT_TRUE(ret.is_null_);                \
  }
#define CHECK_HANDLES_NONNULL(TPL_FUNC, C_FUNC) \
  {                                             \
    Real arg(input);                            \
    Real ret(0.0);                              \
    ArithmeticFunctions::TPL_FUNC(&ret, arg);   \
    EXPECT_FALSE(ret.is_null_);                 \
    EXPECT_DOUBLE_EQ(C_FUNC(input), ret.val_);  \
  }

#define CHECK_SQL_FUNC(TPL_FUNC, C_FUNC) \
  CHECK_HANDLES_NULL(TPL_FUNC, C_FUNC)   \
  CHECK_HANDLES_NONNULL(TPL_FUNC, C_FUNC)

  // Check some of the trig functions on all inputs
  for (const auto input : inputs) {
    CHECK_SQL_FUNC(Cos, std::cos);
    CHECK_SQL_FUNC(Cot, Cotan);
    CHECK_SQL_FUNC(Sin, std::sin);
    CHECK_SQL_FUNC(Tan, std::tan);
    CHECK_SQL_FUNC(Cosh, std::cosh);
    CHECK_SQL_FUNC(Tanh, std::tanh);
    CHECK_SQL_FUNC(Sinh, std::sinh);
    CHECK_SQL_FUNC(Round, std::round);
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
    EXPECT_TRUE(ret.is_null_);                \
  }
#define CHECK_HANDLES_NONNULL(TPL_FUNC, C_FUNC, INPUT) \
  {                                                    \
    Real arg(INPUT);                                   \
    Real ret(0.0);                                     \
    ArithmeticFunctions::TPL_FUNC(&ret, arg);          \
    EXPECT_FALSE(ret.is_null_);                        \
    EXPECT_DOUBLE_EQ(C_FUNC(INPUT), ret.val_);         \
  }

#define CHECK_SQL_FUNC(TPL_FUNC, C_FUNC, INPUT) \
  CHECK_HANDLES_NULL(TPL_FUNC, C_FUNC)          \
  CHECK_HANDLES_NONNULL(TPL_FUNC, C_FUNC, INPUT)

  CHECK_SQL_FUNC(Abs, std::fabs, -4.4);
  CHECK_SQL_FUNC(Abs, std::fabs, 1.10);

  CHECK_SQL_FUNC(Sqrt, std::sqrt, 4.0);
  CHECK_SQL_FUNC(Sqrt, std::sqrt, 1.0);
  CHECK_SQL_FUNC(Sqrt, std::sqrt, 50.1);
  CHECK_SQL_FUNC(Sqrt, std::sqrt, 100.234);

  CHECK_SQL_FUNC(Cbrt, std::cbrt, 4.0);
  CHECK_SQL_FUNC(Cbrt, std::cbrt, -1.0);
  CHECK_SQL_FUNC(Cbrt, std::cbrt, 50.1);
  CHECK_SQL_FUNC(Cbrt, std::cbrt, -100.234);

  CHECK_SQL_FUNC(Exp, std::exp, 4.0);
  CHECK_SQL_FUNC(Exp, std::exp, 1.0);

  CHECK_SQL_FUNC(Truncate, std::trunc, 4.4);
  CHECK_SQL_FUNC(Truncate, std::trunc, 1.2);
  CHECK_SQL_FUNC(Truncate, std::trunc, -100.1);
  CHECK_SQL_FUNC(Truncate, std::trunc, -100.34234);

  CHECK_SQL_FUNC(Ceil, std::ceil, 4.5);
  CHECK_SQL_FUNC(Ceil, std::ceil, -100.34234);

  CHECK_SQL_FUNC(Floor, std::floor, 4.4);
  CHECK_SQL_FUNC(Floor, std::floor, 100.234);

  CHECK_SQL_FUNC(Ln, std::log, 4.4);
  CHECK_SQL_FUNC(Ln, std::log, 100.234);

  CHECK_SQL_FUNC(Log2, std::log2, 4.4);
  CHECK_SQL_FUNC(Log2, std::log2, 100.234);

  CHECK_SQL_FUNC(Log10, std::log10, 4.4);
  CHECK_SQL_FUNC(Log10, std::log10, 100.234);

  CHECK_SQL_FUNC(Round, std::round, 100.4);
  CHECK_SQL_FUNC(Round, std::round, 100.5);
  CHECK_SQL_FUNC(Round, std::round, -100.4);
  CHECK_SQL_FUNC(Round, std::round, -100.5);

#undef CHECK_SQL_FUNC
#undef CHECK_HANDLES_NONNULL
#undef CHECK_HANDLES_NULL
}

// NOLINTNEXTLINE
TEST_F(ArithmeticFunctionsTests, Sign) {
  // Sign
  Real input = Real::Null(), result = Real::Null();
  ArithmeticFunctions::Sign(&result, input);
  EXPECT_TRUE(result.is_null_);

  input = Real(13523.0);
  ArithmeticFunctions::Sign(&result, input);
  EXPECT_FALSE(result.is_null_);
  EXPECT_DOUBLE_EQ(1.0, result.val_);

  input = Real(-1231.0);
  ArithmeticFunctions::Sign(&result, input);
  EXPECT_FALSE(result.is_null_);
  EXPECT_DOUBLE_EQ(-1.0, result.val_);

  input = Real(0.0f);
  ArithmeticFunctions::Sign(&result, input);
  EXPECT_FALSE(result.is_null_);
  EXPECT_DOUBLE_EQ(0.0, result.val_);
}

// Round2
TEST_F(ArithmeticFunctionsTests, Round) {
  Real input = Real::Null(), result = Real::Null();
  Integer precision = Integer::Null();

  input = Real::Null();
  precision = Integer(2);
  ArithmeticFunctions::Round2(&result, input, precision);
  EXPECT_TRUE(result.is_null_);

  input = Real(12.345);
  precision = Integer::Null();
  ArithmeticFunctions::Round2(&result, input, precision);
  EXPECT_TRUE(result.is_null_);

  input = Real(12.345);
  precision = Integer(2);
  ArithmeticFunctions::Round2(&result, input, precision);
  EXPECT_FALSE(result.is_null_);
  EXPECT_DOUBLE_EQ(12.35, result.val_);

  input = Real(12.344);
  precision = Integer(2);
  ArithmeticFunctions::Round2(&result, input, precision);
  EXPECT_FALSE(result.is_null_);
  EXPECT_DOUBLE_EQ(12.34, result.val_);

  input = Real(-12.345);
  precision = Integer(2);
  ArithmeticFunctions::Round2(&result, input, precision);
  EXPECT_FALSE(result.is_null_);
  EXPECT_DOUBLE_EQ(-12.35, result.val_);

  input = Real(-12.344);
  precision = Integer(2);
  ArithmeticFunctions::Round2(&result, input, precision);
  EXPECT_FALSE(result.is_null_);
  EXPECT_DOUBLE_EQ(-12.34, result.val_);
}

TEST_F(ArithmeticFunctionsTests, OutOfRangeTest) {
#define OUT_OF_RANGE(TPL_FUNC)                                                   \
  {                                                                              \
    try {                                                                        \
      Real result = Real::Null();                                                \
      ArithmeticFunctions::TPL_FUNC(&result, Real(42.0));                        \
      FAIL();                                                                    \
    } catch (ExecutionException & e) {                                           \
      EXPECT_EQ(common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, e.code_); \
    }                                                                            \
  }
  OUT_OF_RANGE(Asin)
  OUT_OF_RANGE(Acos)
}

}  // namespace noisepage::execution::sql::test
