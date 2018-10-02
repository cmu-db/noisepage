#include "gtest/gtest.h"
#include "type/expression/conjunction_expression.h"
#include "type/expression/constant_value_expression.h"

namespace terrier::type::expression {

TEST(ExpressionTests, BasicTest) {

  // constant Booleans
  auto expr_b_1 = new ConstantValueExpression(Value((boolean_t) true));
  auto expr_b_2 = new ConstantValueExpression(Value((boolean_t) false));
  auto expr_b_3 = new ConstantValueExpression(Value((boolean_t) true));

  EXPECT_FALSE(*expr_b_1 == *expr_b_2);
  EXPECT_TRUE(*expr_b_1 == *expr_b_3);

  // != is based on ==, so exercise it here, don't need to do with all types
  EXPECT_TRUE(*expr_b_1 != *expr_b_2);
  EXPECT_FALSE(*expr_b_1 != *expr_b_3);

  // constant tinyints
  auto expr_ti_1 = new ConstantValueExpression(Value((int8_t) 1));
  auto expr_ti_2 = new ConstantValueExpression(Value((int8_t) 1));
  auto expr_ti_3 = new ConstantValueExpression(Value((int8_t) 127));

  EXPECT_TRUE(*expr_ti_1 == *expr_ti_2);
  EXPECT_FALSE(*expr_ti_1 == *expr_ti_3);

  // constant smallints
  auto expr_si_1 = new ConstantValueExpression(Value((int16_t) 1));
  auto expr_si_2 = new ConstantValueExpression(Value((int16_t) 1));
  auto expr_si_3 = new ConstantValueExpression(Value((int16_t) 32767));

  EXPECT_TRUE(*expr_si_1 == *expr_si_2);
  EXPECT_FALSE(*expr_si_1 == *expr_si_3);

  // constant ints
  auto expr_i_1 = new ConstantValueExpression(Value((int32_t) 1));
  auto expr_i_2 = new ConstantValueExpression(Value((int32_t) 1));
  auto expr_i_3 = new ConstantValueExpression(Value((int32_t) 32768));

  EXPECT_TRUE(*expr_i_1 == *expr_i_2);
  EXPECT_FALSE(*expr_i_1 == *expr_i_3);

  // constant bigints
  auto expr_bi_1 = new ConstantValueExpression(Value((int32_t) 1));
  auto expr_bi_2 = new ConstantValueExpression(Value((int32_t) 1));
  auto expr_bi_3 = new ConstantValueExpression(Value((int32_t) 32768));

  EXPECT_TRUE(*expr_bi_1 == *expr_bi_2);
  EXPECT_FALSE(*expr_bi_1 == *expr_bi_3);

  // constant double/decimal
  auto expr_d_1 = new ConstantValueExpression(Value((double) 1));
  auto expr_d_2 = new ConstantValueExpression(Value((double) 1));
  auto expr_d_3 = new ConstantValueExpression(Value((double) 32768));

  EXPECT_TRUE( *expr_d_1== *expr_d_2);
  EXPECT_FALSE( *expr_d_1 == *expr_d_3);

  // constant timestamp
  auto expr_ts_1 = new ConstantValueExpression(Value((timestamp_t) 1));
  auto expr_ts_2 = new ConstantValueExpression(Value((timestamp_t) 1));
  auto expr_ts_3 = new ConstantValueExpression(Value((timestamp_t) 32768));

  EXPECT_TRUE(*expr_ts_1== *expr_ts_2);
  EXPECT_FALSE(*expr_ts_1 == *expr_ts_3);

  // constant date
  auto expr_date_1 = new ConstantValueExpression(Value((date_t) 1));
  auto expr_date_2 = new ConstantValueExpression(Value((date_t) 1));
  auto expr_date_3 = new ConstantValueExpression(Value((date_t) 32768));

  EXPECT_TRUE(*expr_date_1== *expr_date_2);
  EXPECT_FALSE(*expr_date_1 == *expr_date_3);

  // check types are differentiated
  EXPECT_FALSE(*expr_b_1 == *expr_date_1);
}

TEST(ExpressionTests, ConjunctionTest) {
  auto c_expr_1 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND,
      new ConstantValueExpression(Value((boolean_t) true)),
      new ConstantValueExpression(Value((boolean_t) false)));

  auto c_expr_2 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND,
      new ConstantValueExpression(Value((boolean_t) true)),
      new ConstantValueExpression(Value((boolean_t) false)));

  auto c_expr_3 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND,
      new ConstantValueExpression(Value((boolean_t) true)),
      new ConstantValueExpression(Value((boolean_t) true)));

  EXPECT_TRUE(*c_expr_1 == *c_expr_2);
  EXPECT_FALSE(*c_expr_1 == *c_expr_3);

}

}  // namespace terrier::type::expression