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

#include "type/transient_value.h"
#include "type/transient_value_factory.h"
#include "type/type_id.h"

namespace terrier::parser::expression {

// NOLINTNEXTLINE
TEST(ExpressionTests, BasicTest) {
  // constant Booleans
  auto expr_b_1 = new ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));
  auto expr_b_3 = new ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));

  EXPECT_FALSE(*expr_b_1 == *expr_b_2);
  EXPECT_TRUE(*expr_b_1 == *expr_b_3);

  // != is based on ==, so exercise it here, don't need to do with all types
  EXPECT_TRUE(*expr_b_1 != *expr_b_2);
  EXPECT_FALSE(*expr_b_1 != *expr_b_3);

  delete expr_b_2;
  delete expr_b_3;

  // constant tinyints
  auto expr_ti_1 = new ConstantValueExpression(type::TransientValueFactory::GetTinyInt(1));
  auto expr_ti_2 = new ConstantValueExpression(type::TransientValueFactory::GetTinyInt(1));
  auto expr_ti_3 = new ConstantValueExpression(type::TransientValueFactory::GetTinyInt(127));

  EXPECT_TRUE(*expr_ti_1 == *expr_ti_2);
  EXPECT_FALSE(*expr_ti_1 == *expr_ti_3);

  delete expr_ti_1;
  delete expr_ti_2;
  delete expr_ti_3;

  // constant smallints
  auto expr_si_1 = new ConstantValueExpression(type::TransientValueFactory::GetSmallInt(1));
  auto expr_si_2 = new ConstantValueExpression(type::TransientValueFactory::GetSmallInt(1));
  auto expr_si_3 = new ConstantValueExpression(type::TransientValueFactory::GetSmallInt(32767));

  EXPECT_TRUE(*expr_si_1 == *expr_si_2);
  EXPECT_FALSE(*expr_si_1 == *expr_si_3);

  delete expr_si_1;
  delete expr_si_2;
  delete expr_si_3;

  // constant ints
  auto expr_i_1 = new ConstantValueExpression(type::TransientValueFactory::GetInteger(1));
  auto expr_i_2 = new ConstantValueExpression(type::TransientValueFactory::GetInteger(1));
  auto expr_i_3 = new ConstantValueExpression(type::TransientValueFactory::GetInteger(32768));

  EXPECT_TRUE(*expr_i_1 == *expr_i_2);
  EXPECT_FALSE(*expr_i_1 == *expr_i_3);

  delete expr_i_1;
  delete expr_i_2;
  delete expr_i_3;

  // constant bigints
  auto expr_bi_1 = new ConstantValueExpression(type::TransientValueFactory::GetBigInt(1));
  auto expr_bi_2 = new ConstantValueExpression(type::TransientValueFactory::GetBigInt(1));
  auto expr_bi_3 = new ConstantValueExpression(type::TransientValueFactory::GetBigInt(32768));

  EXPECT_TRUE(*expr_bi_1 == *expr_bi_2);
  EXPECT_FALSE(*expr_bi_1 == *expr_bi_3);

  delete expr_bi_1;
  delete expr_bi_2;
  delete expr_bi_3;

  // constant double/decimal
  auto expr_d_1 = new ConstantValueExpression(type::TransientValueFactory::GetDecimal(1));
  auto expr_d_2 = new ConstantValueExpression(type::TransientValueFactory::GetDecimal(1));
  auto expr_d_3 = new ConstantValueExpression(type::TransientValueFactory::GetDecimal(32768));

  EXPECT_TRUE(*expr_d_1 == *expr_d_2);
  EXPECT_FALSE(*expr_d_1 == *expr_d_3);

  delete expr_d_1;
  delete expr_d_2;
  delete expr_d_3;

  // constant timestamp
  auto expr_ts_1 = new ConstantValueExpression(type::TransientValueFactory::GetTimestamp(type::timestamp_t(1)));
  auto expr_ts_2 = new ConstantValueExpression(type::TransientValueFactory::GetTimestamp(type::timestamp_t(1)));
  auto expr_ts_3 = new ConstantValueExpression(type::TransientValueFactory::GetTimestamp(type::timestamp_t(32768)));

  EXPECT_TRUE(*expr_ts_1 == *expr_ts_2);
  EXPECT_FALSE(*expr_ts_1 == *expr_ts_3);

  delete expr_ts_1;
  delete expr_ts_2;
  delete expr_ts_3;

  // constant date
  auto expr_date_1 = new ConstantValueExpression(type::TransientValueFactory::GetDate(type::date_t(1)));
  auto expr_date_2 = new ConstantValueExpression(type::TransientValueFactory::GetDate(type::date_t(1)));
  auto expr_date_3 = new ConstantValueExpression(type::TransientValueFactory::GetDate(type::date_t(32768)));

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
  children1.emplace_back(std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
  children1.emplace_back(std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(false)));
  auto c_expr_1 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, std::move(children1));

  std::vector<std::shared_ptr<AbstractExpression>> children2;
  children2.emplace_back(std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
  children2.emplace_back(std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(false)));
  auto c_expr_2 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, std::move(children2));

  std::vector<std::shared_ptr<AbstractExpression>> children3;
  children3.emplace_back(std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
  children3.emplace_back(std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
  auto c_expr_3 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, std::move(children3));

  EXPECT_TRUE(*c_expr_1 == *c_expr_2);
  EXPECT_FALSE(*c_expr_1 == *c_expr_3);

  delete c_expr_1;
  delete c_expr_2;
  delete c_expr_3;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, AggregateExpressionJsonTest) {
  // Create expression
  std::vector<std::shared_ptr<AbstractExpression>> children;
  auto child_expr = std::make_shared<StarExpression>();
  children.push_back(std::move(child_expr));
  std::shared_ptr<AggregateExpression> original_expr =
      std::make_shared<AggregateExpression>(ExpressionType::AGGREGATE_COUNT, std::move(children), true /* distinct */);

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  EXPECT_EQ(original_expr->IsDistinct(),
            static_cast<AggregateExpression *>(deserialized_expression.get())->IsDistinct());
}

// NOLINTNEXTLINE
TEST(ExpressionTests, FunctionExpressionJsonTest) {
  // Create expression
  std::vector<std::shared_ptr<AbstractExpression>> children;
  auto fn_ret_type = type::TypeId::VARCHAR;
  auto original_expr = std::make_shared<FunctionExpression>("Funhouse", fn_ret_type, std::move(children));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  EXPECT_EQ(static_cast<FunctionExpression *>(deserialized_expression.get())->GetFuncName(), "Funhouse");
  EXPECT_EQ(static_cast<FunctionExpression *>(deserialized_expression.get())->GetReturnValueType(), fn_ret_type);
}

// NOLINTNEXTLINE
TEST(ExpressionTests, OperatorExpressionJsonTest) {
  auto operators = {
      ExpressionType::OPERATOR_UNARY_MINUS, ExpressionType::OPERATOR_PLUS,   ExpressionType::OPERATOR_MINUS,
      ExpressionType::OPERATOR_MULTIPLY,    ExpressionType::OPERATOR_DIVIDE, ExpressionType::OPERATOR_CONCAT,
      ExpressionType::OPERATOR_MOD,         ExpressionType::OPERATOR_NOT,    ExpressionType::OPERATOR_IS_NULL,
      ExpressionType::OPERATOR_IS_NOT_NULL, ExpressionType::OPERATOR_EXISTS};

  for (const auto &op : operators) {
    // Create expression
    std::vector<std::shared_ptr<AbstractExpression>> children;
    auto op_ret_type = type::TypeId::BOOLEAN;
    auto original_expr = std::make_shared<OperatorExpression>(op, op_ret_type, std::move(children));

    // Serialize expression
    auto json = original_expr->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize expression
    auto deserialized_expression = DeserializeExpression(json);
    EXPECT_EQ(*original_expr, *deserialized_expression);
    EXPECT_EQ(static_cast<OperatorExpression *>(deserialized_expression.get())->GetExpressionType(), op);
    EXPECT_EQ(static_cast<OperatorExpression *>(deserialized_expression.get())->GetReturnValueType(), op_ret_type);
  }
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ParameterValueExpressionJsonTest) {
  // Create expression
  std::shared_ptr<ParameterValueExpression> original_expr =
      std::make_shared<ParameterValueExpression>(42 /* value_idx */);

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  EXPECT_EQ(original_expr->GetValueIdx(),
            static_cast<ParameterValueExpression *>(deserialized_expression.get())->GetValueIdx());
}

// NOLINTNEXTLINE
TEST(ExpressionTests, TupleValueExpressionJsonTest) {
  // Create expression
  std::shared_ptr<TupleValueExpression> original_expr =
      std::make_shared<TupleValueExpression>("column_name", "table_name");

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  auto *expr = static_cast<TupleValueExpression *>(deserialized_expression.get());
  EXPECT_EQ(original_expr->GetColumnName(), expr->GetColumnName());
  EXPECT_EQ(original_expr->GetTableName(), expr->GetTableName());
}

}  // namespace terrier::parser::expression
