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
#include "parser/expression/column_value_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/default_value_expression.h"
#include "parser/expression/derived_value_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/parameter_value_expression.h"
#include "parser/expression/star_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/expression/type_cast_expression.h"
#include "parser/parameter.h"
#include "parser/postgresparser.h"

#include "type/transient_value.h"
#include "type/transient_value_factory.h"
#include "type/type_id.h"

namespace terrier::parser::expression {

// NOLINTNEXTLINE
TEST(ExpressionTests, ConstantValueExpressionTest) {
  // constant Booleans
  auto expr_b_1 = new ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));
  auto expr_b_3 = new ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));

  EXPECT_FALSE(*expr_b_1 == *expr_b_2);
  EXPECT_NE(expr_b_1->Hash(), expr_b_2->Hash());
  EXPECT_TRUE(*expr_b_1 == *expr_b_3);
  EXPECT_EQ(expr_b_1->Hash(), expr_b_3->Hash());

  // != is based on ==, so exercise it here, don't need to do with all types
  EXPECT_TRUE(*expr_b_1 != *expr_b_2);
  EXPECT_FALSE(*expr_b_1 != *expr_b_3);

  expr_b_1->DeriveExpressionName();
  EXPECT_EQ(expr_b_1->GetExpressionName(), "BOOLEAN");

  delete expr_b_2;
  delete expr_b_3;

  // constant tinyints
  auto expr_ti_1 = new ConstantValueExpression(type::TransientValueFactory::GetTinyInt(1));
  auto expr_ti_2 = new ConstantValueExpression(type::TransientValueFactory::GetTinyInt(1));
  auto expr_ti_3 = new ConstantValueExpression(type::TransientValueFactory::GetTinyInt(127));

  EXPECT_TRUE(*expr_ti_1 == *expr_ti_2);
  EXPECT_EQ(expr_ti_1->Hash(), expr_ti_2->Hash());
  EXPECT_FALSE(*expr_ti_1 == *expr_ti_3);
  EXPECT_NE(expr_ti_1->Hash(), expr_ti_3->Hash());

  EXPECT_EQ(expr_ti_1->GetExpressionType(), ExpressionType::VALUE_CONSTANT);
  EXPECT_EQ(expr_ti_1->GetValue(), type::TransientValueFactory::GetTinyInt(1));
  // There is no need to deduce the return_value_type of constant value expression
  // and calling this function essentially does nothing
  // Only test if we can call it without error.
  expr_ti_1->DeriveReturnValueType();
  EXPECT_EQ(expr_ti_1->GetReturnValueType(), type::TransientValueFactory::GetTinyInt(1).Type());
  EXPECT_EQ(expr_ti_1->GetChildrenSize(), 0);
  EXPECT_EQ(expr_ti_1->GetChildren(), std::vector<std::shared_ptr<AbstractExpression>>());
  // Private members depth will be initialized as -1 and has_subquery as false.
  EXPECT_EQ(expr_ti_1->GetDepth(), -1);
  // Private members expression name will be initialized as empty string
  EXPECT_EQ(expr_ti_1->GetExpressionName(), "");
  expr_ti_1->DeriveExpressionName();
  EXPECT_EQ(expr_ti_1->GetExpressionName(), "TINYINT");
  // depth is still -1 after deriveDepth, as the depth is set in binder
  EXPECT_EQ(expr_ti_1->DeriveDepth(), -1);
  EXPECT_FALSE(expr_ti_1->HasSubquery());

  delete expr_ti_1;
  delete expr_ti_2;
  delete expr_ti_3;

  // constant smallints
  auto expr_si_1 = new ConstantValueExpression(type::TransientValueFactory::GetSmallInt(1));
  auto expr_si_2 = new ConstantValueExpression(type::TransientValueFactory::GetSmallInt(1));
  auto expr_si_3 = new ConstantValueExpression(type::TransientValueFactory::GetSmallInt(32767));

  EXPECT_TRUE(*expr_si_1 == *expr_si_2);
  EXPECT_EQ(expr_si_1->Hash(), expr_si_2->Hash());
  EXPECT_FALSE(*expr_si_1 == *expr_si_3);
  EXPECT_NE(expr_si_1->Hash(), expr_si_3->Hash());

  delete expr_si_1;
  delete expr_si_2;
  delete expr_si_3;

  // constant ints
  auto expr_i_1 = new ConstantValueExpression(type::TransientValueFactory::GetInteger(1));
  auto expr_i_2 = new ConstantValueExpression(type::TransientValueFactory::GetInteger(1));
  auto expr_i_3 = new ConstantValueExpression(type::TransientValueFactory::GetInteger(32768));

  EXPECT_TRUE(*expr_i_1 == *expr_i_2);
  EXPECT_EQ(expr_i_1->Hash(), expr_i_2->Hash());
  EXPECT_FALSE(*expr_i_1 == *expr_i_3);
  EXPECT_NE(expr_i_1->Hash(), expr_i_3->Hash());

  delete expr_i_1;
  delete expr_i_2;
  delete expr_i_3;

  // constant bigints
  auto expr_bi_1 = new ConstantValueExpression(type::TransientValueFactory::GetBigInt(1));
  auto expr_bi_2 = new ConstantValueExpression(type::TransientValueFactory::GetBigInt(1));
  auto expr_bi_3 = new ConstantValueExpression(type::TransientValueFactory::GetBigInt(32768));

  EXPECT_TRUE(*expr_bi_1 == *expr_bi_2);
  EXPECT_EQ(expr_bi_1->Hash(), expr_bi_2->Hash());
  EXPECT_FALSE(*expr_bi_1 == *expr_bi_3);
  EXPECT_NE(expr_bi_1->Hash(), expr_bi_3->Hash());

  delete expr_bi_1;
  delete expr_bi_2;
  delete expr_bi_3;

  // constant double/decimalGetDecimal
  auto expr_d_1 = new ConstantValueExpression(type::TransientValueFactory::GetDecimal(1));
  auto expr_d_2 = new ConstantValueExpression(type::TransientValueFactory::GetDecimal(1));
  auto expr_d_3 = new ConstantValueExpression(type::TransientValueFactory::GetDecimal(32768));

  EXPECT_TRUE(*expr_d_1 == *expr_d_2);
  EXPECT_EQ(expr_d_1->Hash(), expr_d_2->Hash());
  EXPECT_FALSE(*expr_d_1 == *expr_d_3);
  EXPECT_NE(expr_d_1->Hash(), expr_d_3->Hash());

  delete expr_d_1;
  delete expr_d_2;
  delete expr_d_3;

  // constant timestamp
  auto expr_ts_1 = new ConstantValueExpression(type::TransientValueFactory::GetTimestamp(type::timestamp_t(1)));
  auto expr_ts_2 = new ConstantValueExpression(type::TransientValueFactory::GetTimestamp(type::timestamp_t(1)));
  auto expr_ts_3 = new ConstantValueExpression(type::TransientValueFactory::GetTimestamp(type::timestamp_t(32768)));

  EXPECT_TRUE(*expr_ts_1 == *expr_ts_2);
  EXPECT_EQ(expr_ts_1->Hash(), expr_ts_2->Hash());
  EXPECT_FALSE(*expr_ts_1 == *expr_ts_3);
  EXPECT_NE(expr_ts_1->Hash(), expr_ts_3->Hash());

  delete expr_ts_1;
  delete expr_ts_2;
  delete expr_ts_3;

  // constant date
  auto expr_date_1 = new ConstantValueExpression(type::TransientValueFactory::GetDate(type::date_t(1)));
  auto expr_date_2 = new ConstantValueExpression(type::TransientValueFactory::GetDate(type::date_t(1)));
  auto expr_date_3 = new ConstantValueExpression(type::TransientValueFactory::GetDate(type::date_t(32768)));

  EXPECT_TRUE(*expr_date_1 == *expr_date_2);
  EXPECT_EQ(expr_date_1->Hash(), expr_date_2->Hash());
  EXPECT_FALSE(*expr_date_1 == *expr_date_3);
  EXPECT_NE(expr_date_1->Hash(), expr_date_3->Hash());

  // check types are differentiated
  EXPECT_FALSE(*expr_b_1 == *expr_date_1);
  EXPECT_NE(expr_b_1->Hash(), expr_date_1->Hash());

  delete expr_date_1;
  delete expr_date_2;
  delete expr_date_3;

  delete expr_b_1;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ConstantValueExpressionJsonTest) {
  // Create expression
  auto original_expr = std::make_shared<ConstantValueExpression>(
      type::TransientValueFactory::GetVarChar("ConstantValueExpressionJsonTest"));

  EXPECT_EQ(*original_expr, *(original_expr->Copy()));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  const auto from_json_expr = new ConstantValueExpression();
  from_json_expr->FromJson(json);
  EXPECT_TRUE(*original_expr == *from_json_expr);

  delete from_json_expr;

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  EXPECT_EQ(static_cast<ConstantValueExpression *>(deserialized_expression.get())->GetValue(),
            type::TransientValueFactory::GetVarChar("ConstantValueExpressionJsonTest"));
}

// NOLINTNEXTLINE
TEST(ExpressionTests, NullConstantValueExpressionJsonTest) {
  // Create expression
  auto original_expr =
      std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetNull(type::TypeId::VARCHAR));

  EXPECT_EQ(*original_expr, *(original_expr->Copy()));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  const auto from_json_expr = new ConstantValueExpression();
  from_json_expr->FromJson(json);
  EXPECT_TRUE(*original_expr == *from_json_expr);

  delete from_json_expr;

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  EXPECT_TRUE(static_cast<ConstantValueExpression *>(deserialized_expression.get())->GetValue().Null());
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ConjunctionExpressionTest) {
  std::vector<std::shared_ptr<AbstractExpression>> children1;
  children1.emplace_back(std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
  children1.emplace_back(std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(false)));
  auto children1cp = children1;
  auto c_expr_1 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, std::move(children1));

  std::vector<std::shared_ptr<AbstractExpression>> children2;
  children2.emplace_back(std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
  children2.emplace_back(std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(false)));
  auto c_expr_2 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, std::move(children2));

  std::vector<std::shared_ptr<AbstractExpression>> children3;
  children3.emplace_back(std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
  children3.emplace_back(std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
  auto c_expr_3 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, std::move(children3));

  std::vector<std::shared_ptr<AbstractExpression>> children4;
  children4.emplace_back(std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
  children4.emplace_back(std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(false)));
  auto c_expr_4 = new ConjunctionExpression(ExpressionType::CONJUNCTION_OR, std::move(children4));

  EXPECT_TRUE(*c_expr_1 == *c_expr_2);
  EXPECT_FALSE(*c_expr_1 == *c_expr_3);
  EXPECT_FALSE(*c_expr_1 == *c_expr_4);

  EXPECT_EQ(c_expr_1->Hash(), c_expr_2->Hash());
  EXPECT_NE(c_expr_1->Hash(), c_expr_3->Hash());
  EXPECT_NE(c_expr_1->Hash(), c_expr_4->Hash());

  EXPECT_EQ(c_expr_1->GetExpressionType(), ExpressionType::CONJUNCTION_AND);
  // There is no need to deduce the return_value_type of constant value expression
  // and calling this function essentially does nothing
  // Only test if we can call it without error.
  c_expr_1->DeriveReturnValueType();
  EXPECT_EQ(c_expr_1->GetReturnValueType(), type::TypeId::BOOLEAN);
  EXPECT_EQ(c_expr_1->GetChildrenSize(), children1cp.size());
  EXPECT_EQ(c_expr_1->GetChildren(), children1cp);
  // Private members depth will be initialized as -1 and has_subquery as false.
  EXPECT_EQ(c_expr_1->GetDepth(), -1);
  // depth is still -1 after deriveDepth, as the depth is set in binder
  EXPECT_EQ(c_expr_1->DeriveDepth(), -1);
  EXPECT_FALSE(c_expr_1->HasSubquery());
  c_expr_1->DeriveExpressionName();
  EXPECT_EQ(c_expr_1->GetExpressionName(), "AND BOOLEAN AND BOOLEAN");

  delete c_expr_1;
  delete c_expr_2;
  delete c_expr_3;
  delete c_expr_4;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ConjunctionExpressionJsonTest) {
  // Create expression
  std::vector<std::shared_ptr<AbstractExpression>> children1;
  children1.emplace_back(std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
  children1.emplace_back(std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(false)));
  auto c_expr_1 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, std::move(children1));

  EXPECT_EQ(*c_expr_1, *(c_expr_1->Copy()));

  // Serialize expression
  auto json = c_expr_1->ToJson();
  EXPECT_FALSE(json.is_null());

  const auto from_json_expr = new ConjunctionExpression();
  from_json_expr->FromJson(json);
  EXPECT_TRUE(*c_expr_1 == *from_json_expr);

  delete from_json_expr;

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*c_expr_1, *deserialized_expression);
  auto *deserialized_c_expr_1 = static_cast<ConjunctionExpression *>(deserialized_expression.get());
  EXPECT_EQ(c_expr_1->GetReturnValueType(), deserialized_c_expr_1->GetReturnValueType());

  delete c_expr_1;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, AggregateExpressionTest) {
  // Create expression 1
  std::vector<std::shared_ptr<AbstractExpression>> children_1;
  auto child_expr_1 = std::make_shared<StarExpression>();
  children_1.push_back(std::move(child_expr_1));
  auto childrent_1_cp = children_1;
  auto agg_expr_1 = new AggregateExpression(ExpressionType::AGGREGATE_COUNT, std::move(children_1), true);

  // Create expression 2
  std::vector<std::shared_ptr<AbstractExpression>> children_2;
  auto child_expr_2 = std::make_shared<StarExpression>();
  children_2.push_back(std::move(child_expr_2));
  auto agg_expr_2 = new AggregateExpression(ExpressionType::AGGREGATE_COUNT, std::move(children_2), true);

  // Create expression 3, field distinct
  std::vector<std::shared_ptr<AbstractExpression>> children_3;
  auto child_expr_3 = std::make_shared<StarExpression>();
  children_3.push_back(std::move(child_expr_3));
  auto agg_expr_3 = new AggregateExpression(ExpressionType::AGGREGATE_COUNT, std::move(children_3), false);

  // Expresion type comparison and children comparison are implemented in the base class abstract expression
  //  testing them here once is enough

  // Create expression 4, field childsize
  std::vector<std::shared_ptr<AbstractExpression>> children_4;
  auto child_expr_4 = std::make_shared<StarExpression>();
  auto child_expr_4_2 = std::make_shared<StarExpression>();
  children_4.push_back(std::move(child_expr_4));
  children_4.push_back(std::move(child_expr_4_2));
  auto agg_expr_4 = new AggregateExpression(ExpressionType::AGGREGATE_COUNT, std::move(children_4), true);

  // Create expression 5, field child type
  std::vector<std::shared_ptr<AbstractExpression>> children_5;
  auto child_expr_5 = std::make_shared<ConstantValueExpression>();
  children_5.push_back(std::move(child_expr_5));
  auto agg_expr_5 = new AggregateExpression(ExpressionType::AGGREGATE_COUNT, std::move(children_5), true);

  EXPECT_TRUE(*agg_expr_1 == *agg_expr_2);
  EXPECT_FALSE(*agg_expr_1 == *agg_expr_3);
  EXPECT_FALSE(*agg_expr_1 == *agg_expr_4);
  EXPECT_FALSE(*agg_expr_1 == *agg_expr_5);

  EXPECT_EQ(agg_expr_1->Hash(), agg_expr_2->Hash());
  EXPECT_NE(agg_expr_1->Hash(), agg_expr_3->Hash());
  EXPECT_NE(agg_expr_1->Hash(), agg_expr_4->Hash());
  EXPECT_NE(agg_expr_1->Hash(), agg_expr_5->Hash());

  EXPECT_EQ(agg_expr_1->GetExpressionType(), ExpressionType::AGGREGATE_COUNT);
  // There is no need to deduce the return_value_type of constant value expression
  // and calling this function essentially does nothing
  // Only test if we can call it without error.
  agg_expr_1->DeriveReturnValueType();
  EXPECT_EQ(agg_expr_1->GetReturnValueType(), type::TypeId::INTEGER);
  EXPECT_EQ(agg_expr_1->GetChildrenSize(), 1);
  EXPECT_EQ(agg_expr_1->GetChildren(), childrent_1_cp);
  EXPECT_TRUE(agg_expr_1->IsDistinct());
  // Private members depth will be initialized as -1 and has_subquery as false.
  EXPECT_EQ(agg_expr_1->GetDepth(), -1);
  EXPECT_FALSE(agg_expr_1->HasSubquery());
  agg_expr_1->DeriveExpressionName();
  EXPECT_EQ(agg_expr_1->GetExpressionName(), "COUNT STAR");

  // Testing DeriveReturnValueType functionality
  auto children_6 = std::vector<std::shared_ptr<AbstractExpression>>{
      std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true))};
  auto agg_expr_6 = new AggregateExpression(ExpressionType::AGGREGATE_MAX, std::move(children_6), true);
  agg_expr_6->DeriveReturnValueType();

  EXPECT_FALSE(*agg_expr_1 == *agg_expr_6);
  EXPECT_NE(agg_expr_1->Hash(), agg_expr_6->Hash());
  EXPECT_EQ(agg_expr_6->GetReturnValueType(), type::TransientValueFactory::GetBoolean(true).Type());

  // Testing DeriveReturnValueType functionality
  auto children_7 = std::vector<std::shared_ptr<AbstractExpression>>{
      std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true))};
  auto agg_expr_7 = new AggregateExpression(ExpressionType::AGGREGATE_AVG, std::move(children_7), true);
  agg_expr_7->DeriveReturnValueType();

  EXPECT_FALSE(*agg_expr_1 == *agg_expr_7);
  EXPECT_NE(agg_expr_1->Hash(), agg_expr_7->Hash());
  EXPECT_EQ(agg_expr_7->GetReturnValueType(), type::TypeId::DECIMAL);

  // Testing DeriveReturnValueType functionality
  auto children_8 = std::vector<std::shared_ptr<AbstractExpression>>{
      std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true))};
  auto agg_expr_8 = new AggregateExpression(ExpressionType(100), std::move(children_8), true);
#ifndef NDEBUG
  EXPECT_THROW(agg_expr_8->DeriveReturnValueType(), ParserException);
#endif

  delete agg_expr_1;
  delete agg_expr_2;
  delete agg_expr_3;
  delete agg_expr_4;
  delete agg_expr_5;
  delete agg_expr_6;
  delete agg_expr_7;
  delete agg_expr_8;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, AggregateExpressionJsonTest) {
  // Create expression
  std::vector<std::shared_ptr<AbstractExpression>> children;
  auto child_expr = std::make_shared<StarExpression>();
  children.push_back(std::move(child_expr));
  std::shared_ptr<AggregateExpression> original_expr =
      std::make_shared<AggregateExpression>(ExpressionType::AGGREGATE_COUNT, std::move(children), true /* distinct */);

  EXPECT_EQ(*original_expr, *(original_expr->Copy()));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  const auto from_json_expr = new AggregateExpression();
  from_json_expr->FromJson(json);
  EXPECT_TRUE(*original_expr == *from_json_expr);

  delete from_json_expr;

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  EXPECT_EQ(original_expr->IsDistinct(),
            dynamic_cast<AggregateExpression *>(deserialized_expression.get())->IsDistinct());
}

// NOLINTNEXTLINE
TEST(ExpressionTests, CaseExpressionTest) {
  // Create expression 1
  std::shared_ptr<StarExpression> const_expr = std::make_shared<StarExpression>();
  std::vector<CaseExpression::WhenClause> when_clauses;
  CaseExpression::WhenClause when{const_expr, const_expr};
  when_clauses.push_back(when);
  auto case_expr = new CaseExpression(type::TypeId::BOOLEAN, std::move(when_clauses), const_expr);

  // Create expression 2
  std::shared_ptr<StarExpression> const_expr_2 = std::make_shared<StarExpression>();
  std::vector<CaseExpression::WhenClause> when_clauses_2;
  CaseExpression::WhenClause when_2{const_expr_2, const_expr_2};
  when_clauses_2.push_back(when_2);
  auto case_expr_2 = new CaseExpression(type::TypeId::BOOLEAN, std::move(when_clauses_2), const_expr_2);

  // Create expression 3
  std::shared_ptr<ConstantValueExpression> const_expr_3 = std::make_shared<ConstantValueExpression>();
  std::vector<CaseExpression::WhenClause> when_clauses_3;
  CaseExpression::WhenClause when_3{const_expr_3, const_expr_2};
  when_clauses_3.push_back(when_3);
  auto case_expr_3 = new CaseExpression(type::TypeId::BOOLEAN, std::move(when_clauses_3), const_expr_3);

  // Create expression 4
  std::shared_ptr<StarExpression> const_expr_4 = std::make_shared<StarExpression>();
  std::vector<CaseExpression::WhenClause> when_clauses_4;
  CaseExpression::WhenClause when_4{const_expr_4, const_expr_4};
  when_clauses_4.push_back(when_4);
  auto case_expr_4 = new CaseExpression(type::TypeId::INTEGER, std::move(when_clauses_4), const_expr_4);

  EXPECT_TRUE(*case_expr == *case_expr_2);
  EXPECT_FALSE(*case_expr == *case_expr_3);
  EXPECT_FALSE(*case_expr == *case_expr_4);
  EXPECT_EQ(case_expr->Hash(), case_expr_2->Hash());
  EXPECT_NE(case_expr->Hash(), case_expr_3->Hash());
  EXPECT_NE(case_expr->Hash(), case_expr_4->Hash());

  EXPECT_EQ(case_expr->GetExpressionType(), ExpressionType::OPERATOR_CASE_EXPR);
  // There is no need to deduce the return_value_type of constant value expression
  // and calling this function essentially does nothing
  // Only test if we can call it without error.
  case_expr->DeriveReturnValueType();
  EXPECT_EQ(case_expr->GetReturnValueType(), type::TypeId::BOOLEAN);
  EXPECT_EQ(case_expr->GetChildrenSize(), 0);
  EXPECT_EQ(case_expr->GetWhenClauseCondition(0), const_expr);
  EXPECT_EQ(case_expr->GetWhenClauseResult(0), const_expr);
  EXPECT_EQ(case_expr->GetDefaultClause(), const_expr);
  // Private members depth will be initialized as -1 and has_subquery as false.
  EXPECT_EQ(case_expr->GetDepth(), -1);
  EXPECT_FALSE(case_expr->HasSubquery());
  case_expr->DeriveSubqueryFlag();
  EXPECT_FALSE(case_expr->HasSubquery());
  case_expr->DeriveExpressionName();
  EXPECT_EQ(case_expr->GetExpressionName(), "OPERATOR_CASE_EXPR");

  delete case_expr;
  delete case_expr_2;
  delete case_expr_3;
  delete case_expr_4;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, CaseExpressionJsonTest) {
  // Create expression
  std::shared_ptr<StarExpression> const_expr = std::make_shared<StarExpression>();
  std::vector<CaseExpression::WhenClause> when_clauses;
  CaseExpression::WhenClause when{const_expr, const_expr};
  when_clauses.push_back(when);
  std::shared_ptr<CaseExpression> case_expr =
      std::make_shared<CaseExpression>(type::TypeId::BOOLEAN, std::move(when_clauses), const_expr);

  EXPECT_EQ(*case_expr, *(case_expr->Copy()));

  // Serialize expression
  auto json = case_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  const auto from_json_expr = new CaseExpression();
  from_json_expr->FromJson(json);
  EXPECT_TRUE(*case_expr == *from_json_expr);

  delete from_json_expr;

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*case_expr, *deserialized_expression);
  auto *deserialized_case_expr = static_cast<CaseExpression *>(deserialized_expression.get());
  EXPECT_EQ(case_expr->GetReturnValueType(), deserialized_case_expr->GetReturnValueType());
  EXPECT_TRUE(deserialized_case_expr->GetDefaultClause() != nullptr);
  EXPECT_EQ(const_expr->GetExpressionType(), deserialized_case_expr->GetDefaultClause()->GetExpressionType());
}

// NOLINTNEXTLINE
TEST(ExpressionTests, FunctionExpressionTest) {
  auto func_expr_1 = std::make_shared<FunctionExpression>("FullHouse", type::TypeId::VARCHAR,
                                                          std::vector<std::shared_ptr<AbstractExpression>>());
  auto func_expr_2 = std::make_shared<FunctionExpression>("FullHouse", type::TypeId::VARCHAR,
                                                          std::vector<std::shared_ptr<AbstractExpression>>());
  auto func_expr_3 = std::make_shared<FunctionExpression>("Flush", type::TypeId::VARCHAR,
                                                          std::vector<std::shared_ptr<AbstractExpression>>());
  auto func_expr_4 = std::make_shared<FunctionExpression>("FullHouse", type::TypeId::VARBINARY,
                                                          std::vector<std::shared_ptr<AbstractExpression>>());

  std::vector<std::shared_ptr<AbstractExpression>> children;
  auto child_expr = std::make_shared<StarExpression>();
  auto child_expr_2 = std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true));
  children.push_back(std::move(child_expr));
  children.push_back(std::move(child_expr_2));
  auto func_expr_5 = std::make_shared<FunctionExpression>("FullHouse", type::TypeId::VARCHAR, std::move(children));

  EXPECT_TRUE(*func_expr_1 == *func_expr_2);
  EXPECT_FALSE(*func_expr_1 == *func_expr_3);
  EXPECT_FALSE(*func_expr_1 == *func_expr_4);
  EXPECT_FALSE(*func_expr_1 == *func_expr_5);
  EXPECT_EQ(func_expr_1->Hash(), func_expr_2->Hash());
  EXPECT_NE(func_expr_1->Hash(), func_expr_3->Hash());
  EXPECT_NE(func_expr_1->Hash(), func_expr_4->Hash());
  EXPECT_NE(func_expr_1->Hash(), func_expr_5->Hash());
  func_expr_5->DeriveExpressionName();
  EXPECT_EQ(func_expr_5->GetExpressionName(), "FullHouse(STAR,BOOLEAN)");
  func_expr_1->DeriveExpressionName();
  EXPECT_EQ(func_expr_1->GetExpressionName(), "FullHouse()");
}

// NOLINTNEXTLINE
TEST(ExpressionTests, FunctionExpressionJsonTest) {
  // Create expression
  std::vector<std::shared_ptr<AbstractExpression>> children;
  auto fn_ret_type = type::TypeId::VARCHAR;
  auto original_expr = std::make_shared<FunctionExpression>("Funhouse", fn_ret_type, std::move(children));

  EXPECT_EQ(*original_expr, *(original_expr->Copy()));

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
TEST(ExpressionTests, OperatorExpressionTest) {
  // Most methods in the parent class (AbstractExpression Class) have already been tested
  // Following testcases will test only methods unique to the specific child class

  auto op_ret_type = type::TypeId::BOOLEAN;
  auto op_expr_1 = new OperatorExpression(ExpressionType::OPERATOR_NOT, op_ret_type,
                                          std::vector<std::shared_ptr<AbstractExpression>>());
  op_expr_1->DeriveReturnValueType();
  EXPECT_TRUE(op_expr_1->GetReturnValueType() == type::TypeId::BOOLEAN);
  op_expr_1->DeriveExpressionName();
  EXPECT_EQ(op_expr_1->GetExpressionName(), "OPERATOR_NOT");

  auto child1 = std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetDecimal(1));
  auto child2 = std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetBigInt(32768));
  auto children = std::vector<std::shared_ptr<AbstractExpression>>{child1, child2};
  auto children_cp = children;
  auto op_expr_2 = new OperatorExpression(ExpressionType::OPERATOR_PLUS, type::TypeId::INVALID, std::move(children));
  op_expr_2->DeriveReturnValueType();
  EXPECT_TRUE(op_expr_2->GetReturnValueType() == type::TransientValueFactory::GetDecimal(1).Type());
  op_expr_2->DeriveExpressionName();
  EXPECT_EQ(op_expr_2->GetExpressionName(), "+ DECIMAL + BIGINT");

  auto child3 = std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetDate(type::date_t(1)));
  children_cp.push_back(child3);
  auto op_expr_3 =
      new OperatorExpression(ExpressionType::OPERATOR_CONCAT, type::TypeId::INVALID, std::move(children_cp));

  op_expr_3->DeriveExpressionName();
  EXPECT_EQ(op_expr_3->GetExpressionName(), "OPERATOR_CONCAT DECIMAL OPERATOR_CONCAT BIGINT OPERATOR_CONCAT DATE");
  // Make sure that we catch when the deduced expression type suggests that invalid operand types
  // NOTE: We only do this for debug builds
#ifndef NDEBUG
  EXPECT_DEATH(op_expr_3->DeriveReturnValueType(), "Invalid operand type in Operator Expression.");
#endif

  delete op_expr_1;
  delete op_expr_2;
  delete op_expr_3;
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

    EXPECT_EQ(*original_expr, *(original_expr->Copy()));

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
TEST(ExpressionTests, TypeCastExpressionJsonTest) {
  // No generic TypeCastExpression test
  // Create expression
  std::vector<std::shared_ptr<AbstractExpression>> children;
  auto child_expr = std::make_shared<StarExpression>();
  children.push_back(std::move(child_expr));
  auto original_expr = new TypeCastExpression(type::TypeId::SMALLINT, std::move(children));
  EXPECT_EQ(original_expr->GetExpressionType(), ExpressionType::OPERATOR_CAST);
  original_expr->DeriveExpressionName();
  EXPECT_EQ(original_expr->GetExpressionName(), "OPERATOR_CAST STAR");
  EXPECT_EQ(*original_expr, *(original_expr->Copy()));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  EXPECT_EQ(static_cast<TypeCastExpression *>(deserialized_expression.get())->GetExpressionType(),
            ExpressionType::OPERATOR_CAST);
  EXPECT_EQ(original_expr->GetReturnValueType(),
            static_cast<TypeCastExpression *>(deserialized_expression.get())->GetReturnValueType());
  delete original_expr;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ParameterValueExpressionTest) {
  // Create expression
  auto param_expr_1 = new ParameterValueExpression(42);
  auto param_expr_2 = new ParameterValueExpression(42);
  auto param_expr_3 = new ParameterValueExpression(0);

  EXPECT_TRUE(*param_expr_1 == *param_expr_2);
  EXPECT_FALSE(*param_expr_1 == *param_expr_3);
  EXPECT_EQ(param_expr_1->Hash(), param_expr_2->Hash());
  EXPECT_NE(param_expr_1->Hash(), param_expr_3->Hash());
  EXPECT_EQ(param_expr_1->GetExpressionType(), ExpressionType::VALUE_PARAMETER);
  EXPECT_EQ(param_expr_1->GetReturnValueType(), type::TypeId::INTEGER);
  EXPECT_EQ(param_expr_1->GetChildrenSize(), 0);
  EXPECT_EQ(param_expr_1->GetValueIdx(), 42);
  param_expr_1->DeriveExpressionName();
  EXPECT_EQ(param_expr_1->GetExpressionName(), "VALUE_PARAMETER");

  delete param_expr_1;
  delete param_expr_2;
  delete param_expr_3;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ParameterValueExpressionJsonTest) {
  // Create expression
  std::shared_ptr<ParameterValueExpression> original_expr =
      std::make_shared<ParameterValueExpression>(42 /* value_idx */);

  EXPECT_EQ(*original_expr, *(original_expr->Copy()));

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
TEST(ExpressionTests, ColumnValueExpressionTest) {
  auto tve1 = new ColumnValueExpression("", "table_name", "column_name", "alias");
  auto tve2 = new ColumnValueExpression("", "table_name", "column_name", "alias");
  auto tve3 = new ColumnValueExpression("", "table_name2", "column_name", "alias");
  auto tve4 = new ColumnValueExpression("", "table_name", "column_name2", "alias");
  auto tve5 = new ColumnValueExpression("", "table_name", "column_name", "alias2");
  auto tve6 = new ColumnValueExpression("table_name", "column_name");
  auto tve7 = new ColumnValueExpression(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(3));
  auto tve8 = new ColumnValueExpression(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(3));
  auto tve9 = new ColumnValueExpression(catalog::db_oid_t(1), catalog::table_oid_t(4), catalog::col_oid_t(3));
  auto tve10 = new ColumnValueExpression(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(4));
  auto tve14 = new ColumnValueExpression(catalog::db_oid_t(4), catalog::table_oid_t(2), catalog::col_oid_t(3));
  auto tve11 = new ColumnValueExpression("namespace_name", "table_name", "column_name");
  auto tve12 = new ColumnValueExpression("namespace_name", "table_name", "column_name");
  auto tve13 = new ColumnValueExpression("namespace_name2", "table_name", "column_name");

  EXPECT_TRUE(*tve1 == *tve2);
  EXPECT_FALSE(*tve1 == *tve3);
  EXPECT_FALSE(*tve1 == *tve4);
  EXPECT_FALSE(*tve1 == *tve5);
  EXPECT_FALSE(*tve1 == *tve6);
  EXPECT_TRUE(*tve7 == *tve8);
  EXPECT_FALSE(*tve7 == *tve9);
  EXPECT_FALSE(*tve7 == *tve10);
  EXPECT_FALSE(*tve7 == *tve1);
  EXPECT_FALSE(*tve7 == *tve14);
  EXPECT_TRUE(*tve11 == *tve12);
  EXPECT_FALSE(*tve11 == *tve13);
  EXPECT_FALSE(*tve11 == *tve6);

  EXPECT_EQ(tve1->Hash(), tve2->Hash());
  EXPECT_NE(tve1->Hash(), tve3->Hash());
  EXPECT_NE(tve1->Hash(), tve4->Hash());
  EXPECT_NE(tve1->Hash(), tve5->Hash());
  EXPECT_NE(tve1->Hash(), tve6->Hash());
  EXPECT_EQ(tve7->Hash(), tve8->Hash());
  EXPECT_NE(tve7->Hash(), tve9->Hash());
  EXPECT_NE(tve7->Hash(), tve10->Hash());
  EXPECT_NE(tve7->Hash(), tve1->Hash());
  EXPECT_NE(tve7->Hash(), tve14->Hash());
  EXPECT_EQ(tve11->Hash(), tve12->Hash());
  EXPECT_NE(tve11->Hash(), tve13->Hash());
  EXPECT_NE(tve11->Hash(), tve6->Hash());

  EXPECT_EQ(tve1->GetExpressionType(), ExpressionType::COLUMN_VALUE);
  EXPECT_EQ(tve1->GetReturnValueType(), type::TypeId::INVALID);
  EXPECT_EQ(tve1->GetAlias(), "alias");
  EXPECT_EQ(tve1->GetNamespaceName(), "");
  EXPECT_EQ(tve1->GetTableName(), "table_name");
  EXPECT_EQ(tve1->GetColumnName(), "column_name");
  // Uninitialized OIDs set to 0; TODO(Ling): change to INVALID_*_OID after catalog completion
  EXPECT_EQ(tve1->GetTableOid(), catalog::table_oid_t(0));
  EXPECT_EQ(tve1->GetDatabaseOid(), catalog::db_oid_t(0));
  EXPECT_EQ(tve1->GetColumnOid(), catalog::col_oid_t(0));
  EXPECT_EQ(tve1->GetNamespaceName(), "");

  EXPECT_EQ(tve11->GetNamespaceName(), "namespace_name");

  EXPECT_EQ(tve11->GetAlias(), "");
  EXPECT_EQ(tve7->GetTableName(), "");
  EXPECT_EQ(tve7->GetColumnName(), "");
  EXPECT_EQ(tve7->GetColumnOid(), catalog::col_oid_t(3));
  EXPECT_EQ(tve7->GetTableOid(), catalog::table_oid_t(2));
  EXPECT_EQ(tve7->GetDatabaseOid(), catalog::db_oid_t(1));

  tve1->DeriveExpressionName();
  EXPECT_EQ(tve1->GetExpressionName(), "alias");
  tve6->DeriveExpressionName();
  EXPECT_EQ(tve6->GetExpressionName(), "column_name");
  tve7->DeriveExpressionName();
  EXPECT_EQ(tve7->GetExpressionName(), "");

  delete tve1;
  delete tve2;
  delete tve3;
  delete tve4;
  delete tve5;
  delete tve6;
  delete tve7;
  delete tve8;
  delete tve9;
  delete tve10;
  delete tve11;
  delete tve12;
  delete tve13;
  delete tve14;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ColumnValueExpressionJsonTest) {
  // Create expression
  std::shared_ptr<ColumnValueExpression> original_expr =
      std::make_shared<ColumnValueExpression>("", "table_name", "column_name", "alias");

  EXPECT_EQ(*original_expr, *(original_expr->Copy()));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  auto *expr = static_cast<ColumnValueExpression *>(deserialized_expression.get());
  EXPECT_EQ(original_expr->GetNamespaceName(), expr->GetNamespaceName());
  EXPECT_EQ(original_expr->GetColumnName(), expr->GetColumnName());
  EXPECT_EQ(original_expr->GetTableName(), expr->GetTableName());
  EXPECT_EQ(original_expr->GetAlias(), expr->GetAlias());

  // Create expression
  std::shared_ptr<ColumnValueExpression> original_expr_2 =
      std::make_shared<ColumnValueExpression>("table_name", "column_name");

  // Serialize expression
  auto json_2 = original_expr_2->ToJson();
  EXPECT_FALSE(json_2.is_null());

  // Deserialize expression
  auto deserialized_expression_2 = DeserializeExpression(json_2);
  EXPECT_EQ(*original_expr_2, *deserialized_expression_2);
  auto *expr_2 = static_cast<ColumnValueExpression *>(deserialized_expression_2.get());
  EXPECT_EQ(original_expr_2->GetNamespaceName(), expr_2->GetNamespaceName());
  EXPECT_EQ(original_expr_2->GetAlias(), expr_2->GetAlias());
  EXPECT_EQ(original_expr_2->GetColumnName(), expr_2->GetColumnName());
  EXPECT_EQ(original_expr_2->GetTableName(), expr_2->GetTableName());

  // Create expression
  std::shared_ptr<ColumnValueExpression> original_expr_3 =
      std::make_shared<ColumnValueExpression>("namespace_name", "table_name", "column_name");

  // Serialize expression
  auto json_3 = original_expr_3->ToJson();
  EXPECT_FALSE(json_3.is_null());

  // Deserialize expression
  auto deserialized_expression_3 = DeserializeExpression(json_3);
  EXPECT_EQ(*original_expr_3, *deserialized_expression_3);
  auto *expr_3 = static_cast<ColumnValueExpression *>(deserialized_expression_3.get());
  EXPECT_EQ(original_expr_3->GetNamespaceName(), expr_3->GetNamespaceName());
  EXPECT_EQ(original_expr_3->GetAlias(), expr_3->GetAlias());
  EXPECT_EQ(original_expr_3->GetColumnName(), expr_3->GetColumnName());
  EXPECT_EQ(original_expr_3->GetTableName(), expr_3->GetTableName());
  EXPECT_EQ(original_expr_3->GetColumnOid(), expr_3->GetColumnOid());
  EXPECT_EQ(original_expr_3->GetTableOid(), expr_3->GetTableOid());
}

// NOLINTNEXTLINE
TEST(ExpressionTests, DerivedValueExpressionTest) {
  auto tve1 = new DerivedValueExpression(type::TypeId::BOOLEAN, 1, 3);
  auto tve2 = new DerivedValueExpression(type::TypeId::BOOLEAN, 1, 3);
  auto tve3 = new DerivedValueExpression(type::TypeId::SMALLINT, 1, 3);
  auto tve4 = new DerivedValueExpression(type::TypeId::BOOLEAN, 2, 3);
  auto tve5 = new DerivedValueExpression(type::TypeId::BOOLEAN, 1, 4);

  EXPECT_TRUE(*tve1 == *tve2);
  EXPECT_FALSE(*tve1 == *tve3);
  EXPECT_FALSE(*tve1 == *tve4);
  EXPECT_FALSE(*tve1 == *tve5);

  EXPECT_EQ(tve1->Hash(), tve2->Hash());
  EXPECT_NE(tve1->Hash(), tve3->Hash());
  EXPECT_NE(tve1->Hash(), tve4->Hash());
  EXPECT_NE(tve1->Hash(), tve5->Hash());

  EXPECT_EQ(tve1->GetExpressionType(), ExpressionType::VALUE_TUPLE);
  EXPECT_EQ(tve1->GetReturnValueType(), type::TypeId::BOOLEAN);
  EXPECT_EQ(tve1->GetTupleIdx(), 1);
  EXPECT_EQ(tve1->GetValueIdx(), 3);

  tve1->DeriveExpressionName();
  EXPECT_EQ(tve1->GetExpressionName(), "VALUE_TUPLE");

  delete tve1;
  delete tve2;
  delete tve3;
  delete tve4;
  delete tve5;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, DerivedValueExpressionJsonTest) {
  // Create expression
  std::shared_ptr<DerivedValueExpression> original_expr =
      std::make_shared<DerivedValueExpression>(type::TypeId::BOOLEAN, 3, 3);

  EXPECT_EQ(*original_expr, *(original_expr->Copy()));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  auto *expr = static_cast<DerivedValueExpression *>(deserialized_expression.get());
  EXPECT_EQ(original_expr->GetTupleIdx(), expr->GetTupleIdx());
  EXPECT_EQ(original_expr->GetValueIdx(), expr->GetValueIdx());
  EXPECT_EQ(original_expr->GetReturnValueType(), expr->GetReturnValueType());
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ComparisonExpressionJsonTest) {
  // No Generic ComparisonExpression Test needed as it is simple.
  std::vector<std::shared_ptr<AbstractExpression>> children;
  children.emplace_back(std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetInteger(1)));
  children.emplace_back(std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetInteger(2)));

  // Create expression
  auto original_expr = new ComparisonExpression(ExpressionType::COMPARE_EQUAL, std::move(children));
  EXPECT_EQ(original_expr->GetExpressionType(), ExpressionType::COMPARE_EQUAL);
  EXPECT_EQ(original_expr->GetReturnValueType(), type::TypeId::BOOLEAN);
  original_expr->DeriveExpressionName();
  EXPECT_EQ(original_expr->GetExpressionName(), "= INTEGER = INTEGER");
  EXPECT_EQ(*original_expr, *(original_expr->Copy()));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);

  delete original_expr;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, StarExpressionJsonTest) {
  // No Generic StarExpression Test needed as it is simple.

  auto original_expr = new StarExpression();
  EXPECT_EQ(original_expr->GetExpressionType(), ExpressionType::STAR);
  EXPECT_EQ(original_expr->GetReturnValueType(), type::TypeId::INVALID);
  original_expr->DeriveExpressionName();
  EXPECT_EQ(original_expr->GetExpressionName(), "STAR");
  EXPECT_EQ(*original_expr, *(original_expr->Copy()));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);

  delete original_expr;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, DefaultValueExpressionJsonTest) {
  // No Generic DefaultExpression Test needed as it is simple.

  auto original_expr = new DefaultValueExpression();
  EXPECT_EQ(original_expr->GetExpressionType(), ExpressionType::VALUE_DEFAULT);
  EXPECT_EQ(original_expr->GetReturnValueType(), type::TypeId::INVALID);
  original_expr->DeriveExpressionName();
  EXPECT_EQ(original_expr->GetExpressionName(), "VALUE_DEFAULT");
  EXPECT_EQ(*original_expr, *(original_expr->Copy()));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);

  delete original_expr;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, SubqueryExpressionTest) {
  // Current implementation of subquery expression's ==operator and Hash() function
  // only handles: all fields inherited from abstract expression class and
  // subselect's select columns, where condition, and distinct flag
  PostgresParser pgparser;
  auto stmts0 = pgparser.BuildParseTree(
      "SELECT * FROM foo INNER JOIN bar ON foo.a = bar.a WHERE foo.a > 0 GROUP BY foo.b ORDER BY bar.b ASC LIMIT 5;");
  EXPECT_EQ(stmts0.size(), 1);
  EXPECT_EQ(stmts0[0]->GetType(), StatementType::SELECT);

  auto select0 = std::shared_ptr<SelectStatement>(reinterpret_cast<SelectStatement *>(stmts0[0].release()));
  auto subselect_expr0 = new SubqueryExpression(select0);

  auto stmts1 = pgparser.BuildParseTree(
      "SELECT * FROM foo INNER JOIN bar ON foo.a = bar.a WHERE foo.a > 0 GROUP BY foo.b ORDER BY bar.b ASC LIMIT 5;");
  auto select1 = std::shared_ptr<SelectStatement>(reinterpret_cast<SelectStatement *>(stmts1[0].release()));
  auto subselect_expr1 = new SubqueryExpression(select1);

  // different in select columns
  auto stmts2 = pgparser.BuildParseTree(
      "SELECT a, b FROM foo INNER JOIN bar ON foo.a = bar.a WHERE foo.a > 0 GROUP BY foo.b ORDER BY bar.b ASC LIMIT "
      "5;");
  auto select2 = std::shared_ptr<SelectStatement>(reinterpret_cast<SelectStatement *>(stmts2[0].release()));
  auto subselect_expr2 = new SubqueryExpression(select2);

  // different in distinct flag
  auto stmts3 = pgparser.BuildParseTree(
      "SELECT DISTINCT a, b FROM foo INNER JOIN bar ON foo.a = bar.a WHERE foo.a > 0 GROUP BY foo.b ORDER BY bar.b ASC "
      "LIMIT 5;");
  auto select3 = std::shared_ptr<SelectStatement>(reinterpret_cast<SelectStatement *>(stmts3[0].release()));
  auto subselect_expr3 = new SubqueryExpression(select3);

  // different in where
  auto stmts4 = pgparser.BuildParseTree(
      "SELECT * FROM foo INNER JOIN bar ON foo.b = bar.a WHERE foo.b > 0 GROUP BY foo.b ORDER BY bar.b ASC LIMIT 5;");
  auto select4 = std::shared_ptr<SelectStatement>(reinterpret_cast<SelectStatement *>(stmts4[0].release()));
  auto subselect_expr4 = new SubqueryExpression(select4);

  // different in where
  auto stmts5 = pgparser.BuildParseTree("SELECT * FROM foo INNER JOIN bar ON foo.b = bar.a;");
  auto select5 = std::shared_ptr<SelectStatement>(reinterpret_cast<SelectStatement *>(stmts5[0].release()));
  auto subselect_expr5 = new SubqueryExpression(select5);

  // depth is still -1 after deriveDepth, as the depth is set in binder
  EXPECT_EQ(subselect_expr0->DeriveDepth(), -1);
  EXPECT_TRUE(*subselect_expr0 == *subselect_expr1);
  EXPECT_FALSE(*subselect_expr0 == *subselect_expr2);
  EXPECT_FALSE(*subselect_expr2 == *subselect_expr3);
  EXPECT_FALSE(*subselect_expr0 == *subselect_expr4);
  EXPECT_FALSE(*subselect_expr0 == *subselect_expr5);
  EXPECT_EQ(subselect_expr0->Hash(), subselect_expr1->Hash());
  EXPECT_NE(subselect_expr0->Hash(), subselect_expr2->Hash());
  EXPECT_NE(subselect_expr2->Hash(), subselect_expr3->Hash());
  EXPECT_NE(subselect_expr0->Hash(), subselect_expr4->Hash());
  EXPECT_NE(subselect_expr0->Hash(), subselect_expr5->Hash());
  subselect_expr0->DeriveExpressionName();
  EXPECT_EQ(subselect_expr0->GetExpressionName(), "ROW_SUBQUERY");

  delete subselect_expr0;
  delete subselect_expr1;
  delete subselect_expr2;
  delete subselect_expr3;
  delete subselect_expr4;
  delete subselect_expr5;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, SimpleSubqueryExpressionJsonTest) {
  // Create expression
  PostgresParser pgparser;
  auto stmts = pgparser.BuildParseTree("SELECT * FROM foo;");
  EXPECT_EQ(stmts.size(), 1);
  EXPECT_EQ(stmts[0]->GetType(), StatementType::SELECT);

  auto select = std::shared_ptr<SelectStatement>(reinterpret_cast<SelectStatement *>(stmts[0].release()));
  auto original_expr = std::make_shared<SubqueryExpression>(select);
  EXPECT_EQ(*original_expr, *(original_expr->Copy()));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  auto *deserialized_subquery_expr = static_cast<SubqueryExpression *>(deserialized_expression.get());
  EXPECT_TRUE(deserialized_subquery_expr->GetSubselect() != nullptr);
  EXPECT_TRUE(deserialized_subquery_expr->GetSubselect()->GetSelectTable() != nullptr);
  EXPECT_EQ(original_expr->GetSubselect()->GetSelectTable()->GetTableName(),
            deserialized_subquery_expr->GetSubselect()->GetSelectTable()->GetTableName());
  EXPECT_EQ(original_expr->GetSubselect()->GetSelectColumns().size(),
            deserialized_subquery_expr->GetSubselect()->GetSelectColumns().size());
  EXPECT_EQ(1, deserialized_subquery_expr->GetSubselect()->GetSelectColumns().size());
  EXPECT_EQ(original_expr->GetSubselect()->GetSelectColumns()[0]->GetExpressionType(),
            deserialized_subquery_expr->GetSubselect()->GetSelectColumns()[0]->GetExpressionType());
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
  auto original_expr = std::make_shared<SubqueryExpression>(select);

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized_expression = DeserializeExpression(json);
  EXPECT_EQ(*original_expr, *deserialized_expression);
  auto *deserialized_subquery_expr = static_cast<SubqueryExpression *>(deserialized_expression.get());

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

  // Check Distinct
  EXPECT_FALSE(subselect->IsSelectDistinct());

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
}

}  // namespace terrier::parser::expression
