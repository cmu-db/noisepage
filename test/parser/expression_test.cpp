#include <memory>
#include <random>
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
#include "parser/expression/type_cast_expression.h"
#include "parser/parameter.h"
#include "parser/postgresparser.h"

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
  std::vector<parser::AbstractExpression *> children1;
  children1.emplace_back(new ConstantValueExpression(type::TransientValueFactory::GetBoolean(true)));
  children1.emplace_back(new ConstantValueExpression(type::TransientValueFactory::GetBoolean(false)));
  auto c_expr_1 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, std::move(children1));

  std::vector<parser::AbstractExpression *> children2;
  children2.emplace_back(new ConstantValueExpression(type::TransientValueFactory::GetBoolean(true)));
  children2.emplace_back(new ConstantValueExpression(type::TransientValueFactory::GetBoolean(false)));
  auto c_expr_2 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, std::move(children2));

  std::vector<parser::AbstractExpression *> children3;
  children3.emplace_back(new ConstantValueExpression(type::TransientValueFactory::GetBoolean(true)));
  children3.emplace_back(new ConstantValueExpression(type::TransientValueFactory::GetBoolean(true)));
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
  std::vector<parser::AbstractExpression *> children;
  auto child_expr = new StarExpression();
  children.push_back(child_expr);
  auto *original_expr =
      new AggregateExpression(ExpressionType::AGGREGATE_COUNT, std::move(children), true /* distinct */);

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto *deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  EXPECT_EQ(original_expr->IsDistinct(), static_cast<AggregateExpression *>(deserialized_expression)->IsDistinct());

  delete original_expr;
  delete deserialized_expression;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, CaseExpressionTest) {
  // Create expression
  auto *cond_expr = new StarExpression();
  auto *then_expr = new StarExpression();
  std::vector<CaseExpression::WhenClause> when_clauses;
  when_clauses.emplace_back(cond_expr, then_expr);
  auto *default_expr = new StarExpression();
  auto *original_expr = new CaseExpression(type::TypeId::BOOLEAN, when_clauses, default_expr);

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  auto *deserialized_case_expr = static_cast<CaseExpression *>(deserialized_expression);
  EXPECT_EQ(original_expr->GetReturnValueType(), deserialized_case_expr->GetReturnValueType());
  EXPECT_TRUE(deserialized_case_expr->GetDefaultClause() != nullptr);
  EXPECT_EQ(default_expr->GetExpressionType(), deserialized_case_expr->GetDefaultClause()->GetExpressionType());

  delete original_expr;
  delete deserialized_expression;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, FunctionExpressionJsonTest) {
  // Create expression
  std::vector<parser::AbstractExpression *> children;
  auto fn_ret_type = type::TypeId::VARCHAR;
  auto original_expr = new FunctionExpression("Funhouse", fn_ret_type, std::move(children));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  EXPECT_EQ(static_cast<FunctionExpression *>(deserialized_expression)->GetFuncName(), "Funhouse");
  EXPECT_EQ(static_cast<FunctionExpression *>(deserialized_expression)->GetReturnValueType(), fn_ret_type);

  delete original_expr;
  delete deserialized_expression;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ConstantValueExpressionJsonTest) {
  // Create expression
  std::default_random_engine generator_;
  auto value = type::TransientValueFactory::GetVarChar("ConstantValueExpressionJsonTest");
  auto original_expr = new ConstantValueExpression(value);

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  EXPECT_EQ(static_cast<ConstantValueExpression *>(deserialized_expression)->GetValue(), value);

  delete original_expr;
  delete deserialized_expression;
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
    std::vector<parser::AbstractExpression *> children;
    auto op_ret_type = type::TypeId::BOOLEAN;
    auto original_expr = new OperatorExpression(op, op_ret_type, std::move(children));

    // Serialize expression
    auto json = original_expr->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize expression
    auto deserialized_expression = DeserializeExpression(json);
    EXPECT_EQ(*original_expr, *deserialized_expression);
    EXPECT_EQ(static_cast<OperatorExpression *>(deserialized_expression)->GetExpressionType(), op);
    EXPECT_EQ(static_cast<OperatorExpression *>(deserialized_expression)->GetReturnValueType(), op_ret_type);

    delete original_expr;
    delete deserialized_expression;
  }
}

// NOLINTNEXTLINE
TEST(ExpressionTests, TypeCastExpressionJsonTest) {
  // Create expression
  std::vector<parser::AbstractExpression *> children;
  auto child_expr = new StarExpression();
  children.push_back(child_expr);
  auto *original_expr = new TypeCastExpression(type::TypeId::SMALLINT, std::move(children));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  EXPECT_EQ(original_expr->GetType(), static_cast<TypeCastExpression *>(deserialized_expression)->GetType());

  delete original_expr;
  delete deserialized_expression;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ParameterValueExpressionJsonTest) {
  // Create expression
  auto *original_expr = new ParameterValueExpression(42 /* value_idx */);

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  EXPECT_EQ(original_expr->GetValueIdx(),
            static_cast<ParameterValueExpression *>(deserialized_expression)->GetValueIdx());

  delete original_expr;
  delete deserialized_expression;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, TupleValueExpressionJsonTest) {
  // Create expression
  auto *original_expr = new TupleValueExpression("column_name", "table_name");

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  auto *expr = static_cast<TupleValueExpression *>(deserialized_expression);
  EXPECT_EQ(original_expr->GetColumnName(), expr->GetColumnName());
  EXPECT_EQ(original_expr->GetTableName(), expr->GetTableName());

  delete original_expr;
  delete deserialized_expression;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ComparisonExpressionJsonTest) {
  std::vector<parser::AbstractExpression *> children;
  children.emplace_back(new ConstantValueExpression(type::TransientValueFactory::GetInteger(1)));
  children.emplace_back(new ConstantValueExpression(type::TransientValueFactory::GetInteger(2)));

  // Create expression
  auto *original_expr = new ComparisonExpression(ExpressionType::COMPARE_EQUAL, std::move(children));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);

  delete original_expr;
  delete deserialized_expression;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ConjunctionExpressionJsonTest) {
  // Create expression
  std::vector<parser::AbstractExpression *> children;
  children.emplace_back(new ConstantValueExpression(type::TransientValueFactory::GetBoolean(true)));
  children.emplace_back(new ConstantValueExpression(type::TransientValueFactory::GetBoolean(true)));
  auto *original_expr = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, std::move(children));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);

  delete original_expr;
  delete deserialized_expression;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, SimpleSubqueryExpressionJsonTest) {
  // Create expression
  PostgresParser pgparser;
  auto stmts = pgparser.BuildParseTree("SELECT * FROM foo;");
  EXPECT_EQ(stmts.size(), 1);
  EXPECT_EQ(stmts[0]->GetType(), StatementType::SELECT);

  auto select = std::shared_ptr<SelectStatement>(reinterpret_cast<SelectStatement *>(stmts[0].release()));
  auto *original_expr = new SubqueryExpression(select);

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  auto *deserialized_subquery_expr = static_cast<SubqueryExpression *>(deserialized_expression);
  EXPECT_TRUE(deserialized_subquery_expr->GetSubselect() != nullptr);
  EXPECT_TRUE(deserialized_subquery_expr->GetSubselect()->GetSelectTable() != nullptr);
  EXPECT_EQ(original_expr->GetSubselect()->GetSelectTable()->GetTableName(),
            deserialized_subquery_expr->GetSubselect()->GetSelectTable()->GetTableName());
  EXPECT_EQ(original_expr->GetSubselect()->GetSelectColumns().size(),
            deserialized_subquery_expr->GetSubselect()->GetSelectColumns().size());
  EXPECT_EQ(1, deserialized_subquery_expr->GetSubselect()->GetSelectColumns().size());
  EXPECT_EQ(original_expr->GetSubselect()->GetSelectColumns()[0]->GetExpressionType(),
            deserialized_subquery_expr->GetSubselect()->GetSelectColumns()[0]->GetExpressionType());

  delete original_expr;
  delete deserialized_expression;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ComplexSubqueryExpressionJsonTest) {
  // Create expression
  PostgresParser pgparser;
  auto stmts = pgparser.BuildParseTree(
      "SELECT * FROM foo INNER JOIN bar ON foo.a = bar.a GROUP BY foo.b ORDER BY bar.b ASC LIMIT 5;");
  EXPECT_EQ(stmts.size(), 1);
  EXPECT_EQ(stmts[0]->GetType(), StatementType::SELECT);

  auto select = std::shared_ptr<SelectStatement>(reinterpret_cast<SelectStatement *>(stmts[0].release()));
  auto *original_expr = new SubqueryExpression(select);

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  auto *deserialized_subquery_expr = static_cast<SubqueryExpression *>(deserialized_expression);

  // Check Limit
  auto subselect = deserialized_subquery_expr->GetSubselect();
  EXPECT_TRUE(subselect != nullptr);
  EXPECT_TRUE(subselect->GetSelectLimit() != nullptr);
  EXPECT_EQ(original_expr->GetSubselect()->GetSelectLimit()->GetLimit(), subselect->GetSelectLimit()->GetLimit());
  EXPECT_EQ(original_expr->GetSubselect()->GetSelectLimit()->GetOffset(), subselect->GetSelectLimit()->GetOffset());

  // Check Order By
  EXPECT_TRUE(subselect->GetSelectOrderBy() != nullptr);
  EXPECT_EQ(original_expr->GetSubselect()->GetSelectOrderBy()->GetOrderByTypes(),
            subselect->GetSelectOrderBy()->GetOrderByTypes());

  // Check Group By
  EXPECT_TRUE(subselect->GetSelectGroupBy() != nullptr);

  // Check SELECT *
  EXPECT_EQ(original_expr->GetSubselect()->GetSelectColumns().size(), subselect->GetSelectColumns().size());
  EXPECT_EQ(1, subselect->GetSelectColumns().size());

  // Check join
  EXPECT_TRUE(subselect->GetSelectTable() != nullptr);
  EXPECT_TRUE(subselect->GetSelectTable()->GetJoin() != nullptr);
  EXPECT_EQ(original_expr->GetSubselect()->GetSelectTable()->GetJoin()->GetJoinType(),
            subselect->GetSelectTable()->GetJoin()->GetJoinType());
  EXPECT_EQ(original_expr->GetSubselect()->GetSelectTable()->GetJoin()->GetLeftTable()->GetTableName(),
            subselect->GetSelectTable()->GetJoin()->GetLeftTable()->GetTableName());
  EXPECT_EQ(original_expr->GetSubselect()->GetSelectTable()->GetJoin()->GetRightTable()->GetTableName(),
            subselect->GetSelectTable()->GetJoin()->GetRightTable()->GetTableName());
  EXPECT_EQ(*original_expr->GetSubselect()->GetSelectTable()->GetJoin()->GetJoinCondition(),
            *subselect->GetSelectTable()->GetJoin()->GetJoinCondition());

  delete original_expr;
  delete deserialized_expression;
}

}  // namespace terrier::parser::expression
