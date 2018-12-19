#include <memory>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "parser/expression/conjunction_expression.h"
#include "parser/expression/constant_value_expression.h"
// TODO(Tianyu): They are included here so they will get compiled and statically analyzed despite not being used
#include "parser/expression/abstract_expression.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/case_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/parameter_value_expression.h"
#include "parser/expression/star_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/expression/tuple_value_expression.h"
#include "parser/parameter.h"

#include "type/value.h"
#include "type/value_factory.h"

namespace terrier::parser::expression {

// NOLINTNEXTLINE
TEST(ExpressionTests, BasicTest) {
  // NULL
  auto expr_null = new ConstantValueExpression(type::ValueFactory::GetNullValue());
  EXPECT_TRUE(expr_null->GetValue().GetType() == terrier::type::TypeId::NULL_TYPE);
  delete expr_null;

  // constant Booleans
  auto expr_b_1 = new ConstantValueExpression(type::ValueFactory::GetBooleanValue(true));
  auto expr_b_2 = new ConstantValueExpression(type::ValueFactory::GetBooleanValue(false));
  auto expr_b_3 = new ConstantValueExpression(type::ValueFactory::GetBooleanValue(true));

  EXPECT_FALSE(*expr_b_1 == *expr_b_2);
  EXPECT_TRUE(*expr_b_1 == *expr_b_3);

  // != is based on ==, so exercise it here, don't need to do with all types
  EXPECT_TRUE(*expr_b_1 != *expr_b_2);
  EXPECT_FALSE(*expr_b_1 != *expr_b_3);

  delete expr_b_2;
  delete expr_b_3;

  // constant tinyints
  auto expr_ti_1 = new ConstantValueExpression(type::ValueFactory::GetTinyIntValue(1));
  auto expr_ti_2 = new ConstantValueExpression(type::ValueFactory::GetTinyIntValue(1));
  auto expr_ti_3 = new ConstantValueExpression(type::ValueFactory::GetTinyIntValue(127));

  EXPECT_TRUE(*expr_ti_1 == *expr_ti_2);
  EXPECT_FALSE(*expr_ti_1 == *expr_ti_3);

  delete expr_ti_1;
  delete expr_ti_2;
  delete expr_ti_3;

  // constant smallints
  auto expr_si_1 = new ConstantValueExpression(type::ValueFactory::GetSmallIntValue(1));
  auto expr_si_2 = new ConstantValueExpression(type::ValueFactory::GetSmallIntValue(1));
  auto expr_si_3 = new ConstantValueExpression(type::ValueFactory::GetSmallIntValue(32767));

  EXPECT_TRUE(*expr_si_1 == *expr_si_2);
  EXPECT_FALSE(*expr_si_1 == *expr_si_3);

  delete expr_si_1;
  delete expr_si_2;
  delete expr_si_3;

  // constant ints
  auto expr_i_1 = new ConstantValueExpression(type::ValueFactory::GetIntegerValue(1));
  auto expr_i_2 = new ConstantValueExpression(type::ValueFactory::GetIntegerValue(1));
  auto expr_i_3 = new ConstantValueExpression(type::ValueFactory::GetIntegerValue(32768));

  EXPECT_TRUE(*expr_i_1 == *expr_i_2);
  EXPECT_FALSE(*expr_i_1 == *expr_i_3);

  delete expr_i_1;
  delete expr_i_2;
  delete expr_i_3;

  // constant bigints
  auto expr_bi_1 = new ConstantValueExpression(type::ValueFactory::GetBigIntValue(1));
  auto expr_bi_2 = new ConstantValueExpression(type::ValueFactory::GetBigIntValue(1));
  auto expr_bi_3 = new ConstantValueExpression(type::ValueFactory::GetBigIntValue(32768));

  EXPECT_TRUE(*expr_bi_1 == *expr_bi_2);
  EXPECT_FALSE(*expr_bi_1 == *expr_bi_3);

  delete expr_bi_1;
  delete expr_bi_2;
  delete expr_bi_3;

  // constant double/decimal
  auto expr_d_1 = new ConstantValueExpression(type::ValueFactory::GetDecimalValue(1));
  auto expr_d_2 = new ConstantValueExpression(type::ValueFactory::GetDecimalValue(1));
  auto expr_d_3 = new ConstantValueExpression(type::ValueFactory::GetDecimalValue(32768));

  EXPECT_TRUE(*expr_d_1 == *expr_d_2);
  EXPECT_FALSE(*expr_d_1 == *expr_d_3);

  delete expr_d_1;
  delete expr_d_2;
  delete expr_d_3;

  // constant timestamp
  auto expr_ts_1 = new ConstantValueExpression(type::ValueFactory::GetTimeStampValue(type::timestamp_t(1)));
  auto expr_ts_2 = new ConstantValueExpression(type::ValueFactory::GetTimeStampValue(type::timestamp_t(1)));
  auto expr_ts_3 = new ConstantValueExpression(type::ValueFactory::GetTimeStampValue(type::timestamp_t(32768)));

  EXPECT_TRUE(*expr_ts_1 == *expr_ts_2);
  EXPECT_FALSE(*expr_ts_1 == *expr_ts_3);

  delete expr_ts_1;
  delete expr_ts_2;
  delete expr_ts_3;

  // constant date
  auto expr_date_1 = new ConstantValueExpression(type::ValueFactory::GetDateValue(type::date_t(1)));
  auto expr_date_2 = new ConstantValueExpression(type::ValueFactory::GetDateValue(type::date_t(1)));
  auto expr_date_3 = new ConstantValueExpression(type::ValueFactory::GetDateValue(type::date_t(32768)));

  EXPECT_TRUE(*expr_date_1 == *expr_date_2);
  EXPECT_FALSE(*expr_date_1 == *expr_date_3);

  // check types are differentiated
  EXPECT_FALSE(*expr_b_1 == *expr_date_1);

  delete expr_date_1;
  delete expr_date_2;
  delete expr_date_3;

  delete expr_b_1;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ConjunctionTest) {
  std::vector<std::shared_ptr<AbstractExpression>> children1;
  children1.emplace_back(std::make_shared<ConstantValueExpression>(type::ValueFactory::GetBooleanValue(true)));
  children1.emplace_back(std::make_shared<ConstantValueExpression>(type::ValueFactory::GetBooleanValue(false)));
  auto c_expr_1 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, std::move(children1));

  std::vector<std::shared_ptr<AbstractExpression>> children2;
  children2.emplace_back(std::make_shared<ConstantValueExpression>(type::ValueFactory::GetBooleanValue(true)));
  children2.emplace_back(std::make_shared<ConstantValueExpression>(type::ValueFactory::GetBooleanValue(false)));
  auto c_expr_2 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, std::move(children2));

  std::vector<std::shared_ptr<AbstractExpression>> children3;
  children3.emplace_back(std::make_shared<ConstantValueExpression>(type::ValueFactory::GetBooleanValue(true)));
  children3.emplace_back(std::make_shared<ConstantValueExpression>(type::ValueFactory::GetBooleanValue(true)));
  auto c_expr_3 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, std::move(children3));

  EXPECT_TRUE(*c_expr_1 == *c_expr_2);
  EXPECT_FALSE(*c_expr_1 == *c_expr_3);

  delete c_expr_1;
  delete c_expr_2;
  delete c_expr_3;
}

}  // namespace terrier::parser::expression
