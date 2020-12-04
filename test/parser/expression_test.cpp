#include <memory>
#include <random>
#include <utility>
#include <vector>

#include "execution/sql/value_util.h"
#include "gtest/gtest.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/constant_value_expression.h"
// TODO(Tianyu): They are included here so they will get compiled and statically analyzed despite not being used
#include "nlohmann/json.hpp"
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
#include "type/type_id.h"

namespace noisepage::parser::expression {

bool CompareExpressionsEqual(const std::vector<common::ManagedPointer<AbstractExpression>> &expr_children,
                             const std::vector<std::unique_ptr<AbstractExpression>> &copy_children) {
  if (expr_children.size() != copy_children.size()) {
    return false;
  }
  for (size_t i = 0; i < expr_children.size(); i++) {
    if (*expr_children[i] != *copy_children[i]) {
      return false;
    }
  }
  return true;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ConstantValueExpressionTest) {
  // constant Booleans
  auto expr_b_1 = new ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  auto expr_b_2 = new ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(false));
  auto expr_b_3 = new ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));

  EXPECT_FALSE(*expr_b_1 == *expr_b_2);
  EXPECT_NE(expr_b_1->Hash(), expr_b_2->Hash());
  EXPECT_TRUE(*expr_b_1 == *expr_b_3);
  EXPECT_EQ(expr_b_1->Hash(), expr_b_3->Hash());

  // != is based on ==, so exercise it here, don't need to do with all types
  EXPECT_TRUE(*expr_b_1 != *expr_b_2);
  EXPECT_FALSE(*expr_b_1 != *expr_b_3);

  expr_b_1->DeriveExpressionName();
  EXPECT_EQ(expr_b_1->GetExpressionName(), "");

  delete expr_b_2;
  delete expr_b_3;

  // constant tinyints
  auto expr_ti_1 = new ConstantValueExpression(type::TypeId::TINYINT, execution::sql::Integer(1));
  auto expr_ti_2 = new ConstantValueExpression(type::TypeId::TINYINT, execution::sql::Integer(1));
  auto expr_ti_3 = new ConstantValueExpression(type::TypeId::TINYINT, execution::sql::Integer(127));

  EXPECT_TRUE(*expr_ti_1 == *expr_ti_2);
  EXPECT_EQ(expr_ti_1->Hash(), expr_ti_2->Hash());
  EXPECT_FALSE(*expr_ti_1 == *expr_ti_3);
  EXPECT_NE(expr_ti_1->Hash(), expr_ti_3->Hash());

  EXPECT_EQ(expr_ti_1->GetExpressionType(), ExpressionType::VALUE_CONSTANT);
  EXPECT_EQ(*expr_ti_1, ConstantValueExpression(type::TypeId::TINYINT, execution::sql::Integer(1)));
  // There is no need to deduce the return_value_type of constant value expression
  // and calling this function essentially does nothing
  // Only test if we can call it without error.
  expr_ti_1->DeriveReturnValueType();
  EXPECT_EQ(expr_ti_1->GetReturnValueType(), type::TypeId::TINYINT);
  EXPECT_EQ(expr_ti_1->GetChildrenSize(), 0);
  EXPECT_EQ(expr_ti_1->GetChildren(), std::vector<common::ManagedPointer<AbstractExpression>>());
  // Private members depth will be initialized as -1 and has_subquery as false.
  EXPECT_EQ(expr_ti_1->GetDepth(), -1);
  // Private members expression name will be initialized as empty string
  EXPECT_EQ(expr_ti_1->GetExpressionName(), "");
  expr_ti_1->DeriveExpressionName();
  EXPECT_EQ(expr_ti_1->GetExpressionName(), "");
  // depth is still -1 after deriveDepth, as the depth is set in binder
  EXPECT_EQ(expr_ti_1->DeriveDepth(), -1);
  EXPECT_FALSE(expr_ti_1->HasSubquery());

  delete expr_ti_1;
  delete expr_ti_2;
  delete expr_ti_3;

  // constant smallints
  auto expr_si_1 = new ConstantValueExpression(type::TypeId::SMALLINT, execution::sql::Integer(1));
  auto expr_si_2 = new ConstantValueExpression(type::TypeId::SMALLINT, execution::sql::Integer(1));
  auto expr_si_3 = new ConstantValueExpression(type::TypeId::SMALLINT, execution::sql::Integer(32767));

  EXPECT_TRUE(*expr_si_1 == *expr_si_2);
  EXPECT_EQ(expr_si_1->Hash(), expr_si_2->Hash());
  EXPECT_FALSE(*expr_si_1 == *expr_si_3);
  EXPECT_NE(expr_si_1->Hash(), expr_si_3->Hash());

  delete expr_si_1;
  delete expr_si_2;
  delete expr_si_3;

  // constant ints
  auto expr_i_1 = new ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(1));
  auto expr_i_2 = new ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(1));
  auto expr_i_3 = new ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(32768));

  EXPECT_TRUE(*expr_i_1 == *expr_i_2);
  EXPECT_EQ(expr_i_1->Hash(), expr_i_2->Hash());
  EXPECT_FALSE(*expr_i_1 == *expr_i_3);
  EXPECT_NE(expr_i_1->Hash(), expr_i_3->Hash());

  delete expr_i_1;
  delete expr_i_2;
  delete expr_i_3;

  // constant bigints
  auto expr_bi_1 = new ConstantValueExpression(type::TypeId::BIGINT, execution::sql::Integer(1));
  auto expr_bi_2 = new ConstantValueExpression(type::TypeId::BIGINT, execution::sql::Integer(1));
  auto expr_bi_3 = new ConstantValueExpression(type::TypeId::BIGINT, execution::sql::Integer(32768));

  EXPECT_TRUE(*expr_bi_1 == *expr_bi_2);
  EXPECT_EQ(expr_bi_1->Hash(), expr_bi_2->Hash());
  EXPECT_FALSE(*expr_bi_1 == *expr_bi_3);
  EXPECT_NE(expr_bi_1->Hash(), expr_bi_3->Hash());

  delete expr_bi_1;
  delete expr_bi_2;
  delete expr_bi_3;

  // constant double/decimalGetDecimal
  auto expr_d_1 = new ConstantValueExpression(type::TypeId::REAL, execution::sql::Real(static_cast<double>(1)));
  auto expr_d_2 = new ConstantValueExpression(type::TypeId::REAL, execution::sql::Real(static_cast<double>(1)));
  auto expr_d_3 = new ConstantValueExpression(type::TypeId::REAL, execution::sql::Real(static_cast<double>(32768)));

  EXPECT_TRUE(*expr_d_1 == *expr_d_2);
  EXPECT_EQ(expr_d_1->Hash(), expr_d_2->Hash());
  EXPECT_FALSE(*expr_d_1 == *expr_d_3);
  EXPECT_NE(expr_d_1->Hash(), expr_d_3->Hash());

  delete expr_d_1;
  delete expr_d_2;
  delete expr_d_3;

  // constant timestamp
  auto expr_ts_1 = new ConstantValueExpression(type::TypeId::TIMESTAMP, execution::sql::TimestampVal(1));
  auto expr_ts_2 = new ConstantValueExpression(type::TypeId::TIMESTAMP, execution::sql::TimestampVal(1));
  auto expr_ts_3 = new ConstantValueExpression(type::TypeId::TIMESTAMP, execution::sql::TimestampVal(32768));

  EXPECT_TRUE(*expr_ts_1 == *expr_ts_2);
  EXPECT_EQ(expr_ts_1->Hash(), expr_ts_2->Hash());
  EXPECT_FALSE(*expr_ts_1 == *expr_ts_3);
  EXPECT_NE(expr_ts_1->Hash(), expr_ts_3->Hash());

  delete expr_ts_1;
  delete expr_ts_2;
  delete expr_ts_3;

  // constant date
  auto expr_date_1 = new ConstantValueExpression(type::TypeId::DATE, execution::sql::DateVal(1));
  auto expr_date_2 = new ConstantValueExpression(type::TypeId::DATE, execution::sql::DateVal(1));
  auto expr_date_3 = new ConstantValueExpression(type::TypeId::DATE, execution::sql::DateVal(32768));

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

  auto string_val1 = execution::sql::ValueUtil::CreateStringVal(std::string_view("ConstantValueExpressionJsonTest"));

  auto original_expr = std::make_unique<ConstantValueExpression>(type::TypeId::VARCHAR, string_val1.first,
                                                                 std::move(string_val1.second));

  auto copy = original_expr->Copy();
  EXPECT_EQ(*original_expr, *copy);

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  const auto from_json_expr = new ConstantValueExpression();
  auto from_json_res = from_json_expr->FromJson(json);
  EXPECT_TRUE(*original_expr == *from_json_expr);

  delete from_json_expr;

  // Deserialize expression
  auto deserialized = DeserializeExpression(json);
  auto deserialized_expr = common::ManagedPointer(deserialized.result_);
  EXPECT_EQ(*original_expr, *deserialized_expr);

  auto string_val2 = execution::sql::ValueUtil::CreateStringVal(std::string_view("ConstantValueExpressionJsonTest"));
  EXPECT_EQ(*(deserialized_expr.CastManagedPointerTo<ConstantValueExpression>()),
            ConstantValueExpression(type::TypeId::VARCHAR, string_val2.first, std::move(string_val2.second)));
}

// NOLINTNEXTLINE
TEST(ExpressionTests, NullConstantValueExpressionJsonTest) {
  // Create expression
  auto original_expr = std::make_unique<ConstantValueExpression>(type::TypeId::VARCHAR);

  auto copy = original_expr->Copy();
  EXPECT_EQ(*original_expr, *copy);

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  const auto from_json_expr = new ConstantValueExpression();
  auto from_json_res = from_json_expr->FromJson(json);
  EXPECT_TRUE(*original_expr == *from_json_expr);

  delete from_json_expr;

  // Deserialize expression
  auto deserialized = DeserializeExpression(json);
  auto deserialized_expr = common::ManagedPointer(deserialized.result_);
  EXPECT_EQ(*original_expr, *deserialized_expr);
  EXPECT_TRUE(deserialized_expr.CastManagedPointerTo<ConstantValueExpression>()->IsNull());
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ConjunctionExpressionTest) {
  std::vector<std::unique_ptr<AbstractExpression>> children1;
  children1.emplace_back(
      std::make_unique<ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(true)));
  children1.emplace_back(
      std::make_unique<ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(false)));
  std::vector<std::unique_ptr<AbstractExpression>> children1cp;
  children1cp.emplace_back(
      std::make_unique<ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(true)));
  children1cp.emplace_back(
      std::make_unique<ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(false)));
  auto c_expr_1 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, std::move(children1));

  std::vector<std::unique_ptr<AbstractExpression>> children2;
  children2.emplace_back(
      std::make_unique<ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(true)));
  children2.emplace_back(
      std::make_unique<ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(false)));
  auto c_expr_2 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, std::move(children2));

  std::vector<std::unique_ptr<AbstractExpression>> children3;
  children3.emplace_back(
      std::make_unique<ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(true)));
  children3.emplace_back(
      std::make_unique<ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(true)));
  auto c_expr_3 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, std::move(children3));

  std::vector<std::unique_ptr<AbstractExpression>> children4;
  children4.emplace_back(
      std::make_unique<ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(true)));
  children4.emplace_back(
      std::make_unique<ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(false)));
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
  EXPECT_TRUE(CompareExpressionsEqual(c_expr_1->GetChildren(), children1cp));
  // Private members depth will be initialized as -1 and has_subquery as false.
  EXPECT_EQ(c_expr_1->GetDepth(), -1);
  // depth is still -1 after deriveDepth, as the depth is set in binder
  EXPECT_EQ(c_expr_1->DeriveDepth(), -1);
  EXPECT_FALSE(c_expr_1->HasSubquery());
  c_expr_1->DeriveExpressionName();
  EXPECT_EQ(c_expr_1->GetExpressionName(), "");

  delete c_expr_1;
  delete c_expr_2;
  delete c_expr_3;
  delete c_expr_4;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ConjunctionExpressionJsonTest) {
  // Create expression
  std::vector<std::unique_ptr<AbstractExpression>> children1;
  children1.emplace_back(
      std::make_unique<ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(true)));
  children1.emplace_back(
      std::make_unique<ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(false)));
  auto c_expr_1 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, std::move(children1));
  auto copy = c_expr_1->Copy();
  EXPECT_EQ(*c_expr_1, *copy);

  // Serialize expression
  auto json = c_expr_1->ToJson();
  EXPECT_FALSE(json.is_null());

  const auto from_json_expr = new ConjunctionExpression();
  from_json_expr->FromJson(json);
  EXPECT_TRUE(*c_expr_1 == *from_json_expr);

  delete from_json_expr;

  // Deserialize expression
  auto deserialized = DeserializeExpression(json);
  auto deserialized_expr = common::ManagedPointer(deserialized.result_);
  EXPECT_EQ(*c_expr_1, *deserialized_expr);
  auto deserialized_c_expr_1 = deserialized_expr.CastManagedPointerTo<ConjunctionExpression>();
  EXPECT_EQ(c_expr_1->GetReturnValueType(), deserialized_c_expr_1->GetReturnValueType());

  delete c_expr_1;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, AggregateExpressionTest) {
  // Create expression 1
  std::vector<std::unique_ptr<AbstractExpression>> children_1;
  children_1.emplace_back(std::make_unique<StarExpression>());
  std::vector<std::unique_ptr<AbstractExpression>> childrent_1_cp;
  childrent_1_cp.emplace_back(std::make_unique<StarExpression>());
  auto agg_expr_1 = new AggregateExpression(ExpressionType::AGGREGATE_COUNT, std::move(children_1), true);

  // Create expression 2
  std::vector<std::unique_ptr<AbstractExpression>> children_2;
  children_2.emplace_back(std::make_unique<StarExpression>());
  auto agg_expr_2 = new AggregateExpression(ExpressionType::AGGREGATE_COUNT, std::move(children_2), true);

  // Create expression 3, field distinct
  std::vector<std::unique_ptr<AbstractExpression>> children_3;
  children_3.emplace_back(std::make_unique<StarExpression>());
  auto agg_expr_3 = new AggregateExpression(ExpressionType::AGGREGATE_COUNT, std::move(children_3), false);

  // Expresion type comparison and children comparison are implemented in the base class abstract expression
  //  testing them here once is enough

  // Create expression 4, field childsize
  std::vector<std::unique_ptr<AbstractExpression>> children_4;
  children_4.emplace_back(std::make_unique<StarExpression>());
  children_4.emplace_back(std::make_unique<StarExpression>());
  auto agg_expr_4 = new AggregateExpression(ExpressionType::AGGREGATE_COUNT, std::move(children_4), true);

  // Create expression 5, field child type
  std::vector<std::unique_ptr<AbstractExpression>> children_5;
  children_5.emplace_back(
      std::make_unique<ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(false)));
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
  EXPECT_TRUE(CompareExpressionsEqual(agg_expr_1->GetChildren(), childrent_1_cp));
  EXPECT_TRUE(agg_expr_1->IsDistinct());
  // Private members depth will be initialized as -1 and has_subquery as false.
  EXPECT_EQ(agg_expr_1->GetDepth(), -1);
  EXPECT_FALSE(agg_expr_1->HasSubquery());
  agg_expr_1->DeriveExpressionName();
  EXPECT_EQ(agg_expr_1->GetExpressionName(), "");

  // Testing DeriveReturnValueType functionality
  std::vector<std::unique_ptr<AbstractExpression>> children_6;
  children_6.emplace_back(
      std::make_unique<ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(true)));
  auto agg_expr_6 = new AggregateExpression(ExpressionType::AGGREGATE_MAX, std::move(children_6), true);
  agg_expr_6->DeriveReturnValueType();

  EXPECT_FALSE(*agg_expr_1 == *agg_expr_6);
  EXPECT_NE(agg_expr_1->Hash(), agg_expr_6->Hash());
  EXPECT_EQ(agg_expr_6->GetReturnValueType(), type::TypeId::BOOLEAN);

  // Testing DeriveReturnValueType functionality
  std::vector<std::unique_ptr<AbstractExpression>> children_7;
  children_7.emplace_back(
      std::make_unique<ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(true)));
  auto agg_expr_7 = new AggregateExpression(ExpressionType::AGGREGATE_AVG, std::move(children_7), true);
  agg_expr_7->DeriveReturnValueType();

  EXPECT_FALSE(*agg_expr_1 == *agg_expr_7);
  EXPECT_NE(agg_expr_1->Hash(), agg_expr_7->Hash());
  EXPECT_EQ(agg_expr_7->GetReturnValueType(), type::TypeId::REAL);

  // Testing DeriveReturnValueType functionality
  std::vector<std::unique_ptr<AbstractExpression>> children_8;
  children_8.emplace_back(
      std::make_unique<ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(true)));
  auto agg_expr_8 = new AggregateExpression(ExpressionType(100), std::move(children_8), true);
  // TODO(WAN): Why is there a random NDEBUG here?
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
  std::vector<std::unique_ptr<AbstractExpression>> children;
  auto child_expr = std::make_unique<StarExpression>();
  children.push_back(std::move(child_expr));
  std::unique_ptr<AggregateExpression> original_expr =
      std::make_unique<AggregateExpression>(ExpressionType::AGGREGATE_COUNT, std::move(children), true /* distinct */);

  auto copy = original_expr->Copy();
  EXPECT_EQ(*original_expr, *copy);

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  const auto from_json_expr = new AggregateExpression();
  from_json_expr->FromJson(json);
  EXPECT_TRUE(*original_expr == *from_json_expr);

  delete from_json_expr;

  // Deserialize expression
  auto deserialized = DeserializeExpression(json);
  auto deserialized_expr = common::ManagedPointer(deserialized.result_);
  EXPECT_EQ(*original_expr, *deserialized_expr);
  EXPECT_EQ(original_expr->IsDistinct(), deserialized_expr.CastManagedPointerTo<AggregateExpression>()->IsDistinct());
}

// NOLINTNEXTLINE
TEST(ExpressionTests, CaseExpressionTest) {
  // Create expression 1
  std::vector<CaseExpression::WhenClause> when_clauses;
  when_clauses.emplace_back(
      CaseExpression::WhenClause{std::make_unique<StarExpression>(), std::make_unique<StarExpression>()});
  auto case_expr =
      new CaseExpression(type::TypeId::BOOLEAN, std::move(when_clauses), std::make_unique<StarExpression>());

  // Create expression 2
  std::vector<CaseExpression::WhenClause> when_clauses_2;
  when_clauses_2.emplace_back(
      CaseExpression::WhenClause{std::make_unique<StarExpression>(), std::make_unique<StarExpression>()});
  auto case_expr_2 =
      new CaseExpression(type::TypeId::BOOLEAN, std::move(when_clauses_2), std::make_unique<StarExpression>());

  // Create expression 3
  std::vector<CaseExpression::WhenClause> when_clauses_3;
  when_clauses_3.emplace_back(CaseExpression::WhenClause{
      std::make_unique<ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(false)),
      std::make_unique<StarExpression>()});
  auto case_expr_3 = new CaseExpression(
      type::TypeId::BOOLEAN, std::move(when_clauses_3),
      std::make_unique<ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(false)));

  // Create expression 4
  std::vector<CaseExpression::WhenClause> when_clauses_4;
  when_clauses_4.emplace_back(
      CaseExpression::WhenClause{std::make_unique<StarExpression>(), std::make_unique<StarExpression>()});
  auto case_expr_4 =
      new CaseExpression(type::TypeId::INTEGER, std::move(when_clauses_4), std::make_unique<StarExpression>());

  // TODO(WAN): StarExpression should probably be a singleton some day.
  // Reusable star expression.
  auto star_expr = std::make_unique<StarExpression>();

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
  EXPECT_EQ(*case_expr->GetWhenClauseCondition(0), *star_expr);
  EXPECT_EQ(*case_expr->GetWhenClauseResult(0), *star_expr);
  EXPECT_EQ(*case_expr->GetDefaultClause(), *star_expr);
  // Private members depth will be initialized as -1 and has_subquery as false.
  EXPECT_EQ(case_expr->GetDepth(), -1);
  EXPECT_FALSE(case_expr->HasSubquery());
  case_expr->DeriveSubqueryFlag();
  EXPECT_FALSE(case_expr->HasSubquery());
  case_expr->DeriveExpressionName();
  EXPECT_EQ(case_expr->GetExpressionName(), "");

  delete case_expr;
  delete case_expr_2;
  delete case_expr_3;
  delete case_expr_4;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, CaseExpressionJsonTest) {
  // Create expression
  std::vector<CaseExpression::WhenClause> when_clauses;
  when_clauses.emplace_back(
      CaseExpression::WhenClause{std::make_unique<StarExpression>(), std::make_unique<StarExpression>()});
  auto case_expr = std::make_unique<CaseExpression>(type::TypeId::BOOLEAN, std::move(when_clauses),
                                                    std::make_unique<StarExpression>());

  auto copy = case_expr->Copy();
  EXPECT_EQ(*case_expr, *copy);

  // Serialize expression
  auto json = case_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  const auto from_json_expr = new CaseExpression();
  from_json_expr->FromJson(json);
  EXPECT_TRUE(*case_expr == *from_json_expr);

  delete from_json_expr;

  // Deserialize expression
  auto deserialized = DeserializeExpression(json);
  auto deserialized_case_expr = common::ManagedPointer(deserialized.result_).CastManagedPointerTo<CaseExpression>();
  EXPECT_EQ(*case_expr, *deserialized_case_expr);
  EXPECT_EQ(case_expr->GetReturnValueType(), deserialized_case_expr->GetReturnValueType());
  EXPECT_TRUE(deserialized_case_expr->GetDefaultClause() != nullptr);
  EXPECT_EQ(std::make_unique<StarExpression>()->GetExpressionType(),
            deserialized_case_expr->GetDefaultClause()->GetExpressionType());
}

// NOLINTNEXTLINE
TEST(ExpressionTests, FunctionExpressionTest) {
  auto func_expr_1 = std::make_unique<FunctionExpression>("FullHouse", type::TypeId::VARCHAR,
                                                          std::vector<std::unique_ptr<AbstractExpression>>());
  auto func_expr_2 = std::make_unique<FunctionExpression>("FullHouse", type::TypeId::VARCHAR,
                                                          std::vector<std::unique_ptr<AbstractExpression>>());
  auto func_expr_3 = std::make_unique<FunctionExpression>("Flush", type::TypeId::VARCHAR,
                                                          std::vector<std::unique_ptr<AbstractExpression>>());
  auto func_expr_4 = std::make_unique<FunctionExpression>("FullHouse", type::TypeId::VARBINARY,
                                                          std::vector<std::unique_ptr<AbstractExpression>>());

  std::vector<std::unique_ptr<AbstractExpression>> children;
  auto child_expr = std::make_unique<StarExpression>();
  auto child_expr_2 = std::make_unique<ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  children.push_back(std::move(child_expr));
  children.push_back(std::move(child_expr_2));
  auto func_expr_5 = std::make_unique<FunctionExpression>("FullHouse", type::TypeId::VARCHAR, std::move(children));

  EXPECT_TRUE(*func_expr_1 == *func_expr_2);
  EXPECT_FALSE(*func_expr_1 == *func_expr_3);
  EXPECT_FALSE(*func_expr_1 == *func_expr_4);
  EXPECT_FALSE(*func_expr_1 == *func_expr_5);
  EXPECT_EQ(func_expr_1->Hash(), func_expr_2->Hash());
  EXPECT_NE(func_expr_1->Hash(), func_expr_3->Hash());
  EXPECT_NE(func_expr_1->Hash(), func_expr_4->Hash());
  EXPECT_NE(func_expr_1->Hash(), func_expr_5->Hash());
  func_expr_5->DeriveExpressionName();
  EXPECT_EQ(func_expr_5->GetExpressionName(), "FullHouse");
  func_expr_1->DeriveExpressionName();
  EXPECT_EQ(func_expr_1->GetExpressionName(), "FullHouse");
}

// NOLINTNEXTLINE
TEST(ExpressionTests, FunctionExpressionJsonTest) {
  // Create expression
  std::vector<std::unique_ptr<AbstractExpression>> children;
  auto fn_ret_type = type::TypeId::VARCHAR;
  auto original_expr = std::make_unique<FunctionExpression>("Funhouse", fn_ret_type, std::move(children));

  EXPECT_EQ(*original_expr, *(original_expr->Copy()));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized = DeserializeExpression(json);
  auto deserialized_expr = common::ManagedPointer(deserialized.result_).CastManagedPointerTo<FunctionExpression>();
  EXPECT_EQ(*original_expr, *deserialized_expr);
  EXPECT_EQ(deserialized_expr->GetFuncName(), "Funhouse");
  EXPECT_EQ(deserialized_expr->GetReturnValueType(), fn_ret_type);
}

// NOLINTNEXTLINE
TEST(ExpressionTests, OperatorExpressionTest) {
  // Most methods in the parent class (AbstractExpression Class) have already been tested
  // Following testcases will test only methods unique to the specific child class

  auto op_ret_type = type::TypeId::BOOLEAN;
  auto op_expr_1 = new OperatorExpression(ExpressionType::OPERATOR_NOT, op_ret_type,
                                          std::vector<std::unique_ptr<AbstractExpression>>());
  op_expr_1->DeriveReturnValueType();
  EXPECT_TRUE(op_expr_1->GetReturnValueType() == type::TypeId::BOOLEAN);
  op_expr_1->DeriveExpressionName();
  EXPECT_EQ(op_expr_1->GetExpressionName(), "");

  std::vector<std::unique_ptr<AbstractExpression>> children;
  children.emplace_back(
      std::make_unique<ConstantValueExpression>(type::TypeId::REAL, execution::sql::Real(static_cast<double>(1))));
  children.emplace_back(
      std::make_unique<ConstantValueExpression>(type::TypeId::BIGINT, execution::sql::Integer(32768)));
  std::vector<std::unique_ptr<AbstractExpression>> children_cp;
  children_cp.emplace_back(
      std::make_unique<ConstantValueExpression>(type::TypeId::REAL, execution::sql::Real(static_cast<double>(1))));
  children_cp.emplace_back(
      std::make_unique<ConstantValueExpression>(type::TypeId::BIGINT, execution::sql::Integer(32768)));

  auto op_expr_2 = new OperatorExpression(ExpressionType::OPERATOR_PLUS, type::TypeId::INVALID, std::move(children));
  op_expr_2->DeriveReturnValueType();
  EXPECT_TRUE(op_expr_2->GetReturnValueType() == type::TypeId::REAL);
  op_expr_2->DeriveExpressionName();
  EXPECT_EQ(op_expr_2->GetExpressionName(), "");

  auto child3 = std::make_unique<ConstantValueExpression>(type::TypeId::DATE, execution::sql::DateVal(1));
  children_cp.push_back(std::move(child3));
  auto op_expr_3 =
      new OperatorExpression(ExpressionType::OPERATOR_CONCAT, type::TypeId::INVALID, std::move(children_cp));

  op_expr_3->DeriveExpressionName();
  EXPECT_EQ(op_expr_3->GetExpressionName(), "");

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
    std::vector<std::unique_ptr<AbstractExpression>> children;
    auto op_ret_type = type::TypeId::BOOLEAN;
    auto original_expr = std::make_unique<OperatorExpression>(op, op_ret_type, std::move(children));

    EXPECT_EQ(*original_expr, *(original_expr->Copy()));

    // Serialize expression
    auto json = original_expr->ToJson();
    EXPECT_FALSE(json.is_null());

    // Deserialize expression
    auto deserialized = DeserializeExpression(json);
    auto deserialized_expr = common::ManagedPointer(deserialized.result_).CastManagedPointerTo<OperatorExpression>();
    EXPECT_EQ(*original_expr, *deserialized_expr);
    EXPECT_EQ(deserialized_expr->GetExpressionType(), op);
    EXPECT_EQ(deserialized_expr->GetReturnValueType(), op_ret_type);
  }
}

// NOLINTNEXTLINE
TEST(ExpressionTests, TypeCastExpressionJsonTest) {
  // No generic TypeCastExpression test
  // Create expression
  std::vector<std::unique_ptr<AbstractExpression>> children;
  auto child_expr = std::make_unique<StarExpression>();
  children.push_back(std::move(child_expr));
  auto original_expr = new TypeCastExpression(type::TypeId::SMALLINT, std::move(children));
  EXPECT_EQ(original_expr->GetExpressionType(), ExpressionType::OPERATOR_CAST);
  original_expr->DeriveExpressionName();
  EXPECT_EQ(original_expr->GetExpressionName(), "");
  EXPECT_EQ(*original_expr, *(original_expr->Copy()));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized = DeserializeExpression(json);
  auto deserialized_expr = common::ManagedPointer(deserialized.result_).CastManagedPointerTo<TypeCastExpression>();
  EXPECT_EQ(*original_expr, *deserialized_expr);
  EXPECT_EQ(deserialized_expr->GetExpressionType(), ExpressionType::OPERATOR_CAST);
  EXPECT_EQ(original_expr->GetReturnValueType(), deserialized_expr->GetReturnValueType());
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
  EXPECT_EQ(param_expr_1->GetReturnValueType(),
            type::TypeId::INVALID);  // default type is now INVALID so we know in the binder whether it's been bound yet
  EXPECT_EQ(param_expr_1->GetChildrenSize(), 0);
  EXPECT_EQ(param_expr_1->GetValueIdx(), 42);
  param_expr_1->DeriveExpressionName();
  EXPECT_EQ(param_expr_1->GetExpressionName(), "");

  delete param_expr_1;
  delete param_expr_2;
  delete param_expr_3;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ParameterValueExpressionJsonTest) {
  // Create expression
  std::unique_ptr<ParameterValueExpression> original_expr =
      std::make_unique<ParameterValueExpression>(42 /* value_idx */);

  EXPECT_EQ(*original_expr, *(original_expr->Copy()));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized = DeserializeExpression(json);
  auto deserialized_expr =
      common::ManagedPointer(deserialized.result_).CastManagedPointerTo<ParameterValueExpression>();
  EXPECT_EQ(*original_expr, *deserialized_expr);
  EXPECT_EQ(original_expr->GetValueIdx(), deserialized_expr->GetValueIdx());
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ColumnValueExpressionTest) {
  auto tve1 = new ColumnValueExpression("table_name", "column_name", "alias");
  auto tve2 = new ColumnValueExpression("table_name", "column_name", "alias");
  auto tve3 = new ColumnValueExpression("table_name2", "column_name", "alias");
  auto tve4 = new ColumnValueExpression("table_name", "column_name2", "alias");
  auto tve6 = new ColumnValueExpression("table_name", "column_name");
  auto tve7 = new ColumnValueExpression(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(3));
  auto tve8 = new ColumnValueExpression(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(3));
  auto tve9 = new ColumnValueExpression(catalog::db_oid_t(1), catalog::table_oid_t(4), catalog::col_oid_t(3));
  auto tve10 = new ColumnValueExpression(catalog::db_oid_t(1), catalog::table_oid_t(2), catalog::col_oid_t(4));
  auto tve14 = new ColumnValueExpression(catalog::db_oid_t(4), catalog::table_oid_t(2), catalog::col_oid_t(3));
  auto tve11 = new ColumnValueExpression("table_name", "column_name");

  EXPECT_TRUE(*tve1 == *tve2);
  EXPECT_FALSE(*tve1 == *tve3);
  EXPECT_FALSE(*tve1 == *tve4);
  EXPECT_TRUE(*tve7 == *tve8);
  EXPECT_FALSE(*tve7 == *tve9);
  EXPECT_FALSE(*tve7 == *tve10);
  EXPECT_FALSE(*tve7 == *tve1);
  EXPECT_FALSE(*tve7 == *tve14);
  EXPECT_TRUE(*tve11 == *tve6);

  EXPECT_EQ(tve1->Hash(), tve2->Hash());
  EXPECT_NE(tve1->Hash(), tve3->Hash());
  EXPECT_NE(tve1->Hash(), tve4->Hash());
  EXPECT_EQ(tve7->Hash(), tve8->Hash());
  EXPECT_NE(tve7->Hash(), tve9->Hash());
  EXPECT_NE(tve7->Hash(), tve10->Hash());
  EXPECT_NE(tve7->Hash(), tve1->Hash());
  EXPECT_NE(tve7->Hash(), tve14->Hash());
  EXPECT_EQ(tve11->Hash(), tve6->Hash());

  EXPECT_EQ(tve1->GetExpressionType(), ExpressionType::COLUMN_VALUE);
  EXPECT_EQ(tve1->GetReturnValueType(), type::TypeId::INVALID);
  EXPECT_EQ(tve1->GetAlias(), "alias");
  EXPECT_EQ(tve1->GetTableName(), "table_name");
  EXPECT_EQ(tve1->GetColumnName(), "column_name");
  // Uninitialized OIDs set to 0; TODO(Ling): change to INVALID_*_OID after catalog completion
  EXPECT_EQ(tve1->GetTableOid(), catalog::INVALID_TABLE_OID);
  EXPECT_EQ(tve1->GetDatabaseOid(), catalog::INVALID_DATABASE_OID);
  EXPECT_EQ(tve1->GetColumnOid(), catalog::INVALID_COLUMN_OID);

  EXPECT_EQ(tve11->GetAlias(), "");
  EXPECT_EQ(tve7->GetTableName(), "");
  EXPECT_EQ(tve7->GetColumnName(), "");
  EXPECT_EQ(tve7->GetColumnOid(), catalog::col_oid_t(3));
  EXPECT_EQ(tve7->GetTableOid(), catalog::table_oid_t(2));
  EXPECT_EQ(tve7->GetDatabaseOid(), catalog::db_oid_t(1));

  tve1->DeriveExpressionName();
  EXPECT_EQ(tve1->GetExpressionName(), "alias");
  tve7->DeriveExpressionName();
  EXPECT_EQ(tve7->GetExpressionName(), "");

  delete tve1;
  delete tve2;
  delete tve3;
  delete tve4;
  delete tve6;
  delete tve7;
  delete tve8;
  delete tve9;
  delete tve10;
  delete tve11;
  delete tve14;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ColumnValueExpressionJsonTest) {
  // Create expression
  std::unique_ptr<ColumnValueExpression> original_expr =
      std::make_unique<ColumnValueExpression>("table_name", "column_name", "alias");

  EXPECT_EQ(*original_expr, *(original_expr->Copy()));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized = DeserializeExpression(json);
  auto deserialized_expr = common::ManagedPointer(deserialized.result_).CastManagedPointerTo<ColumnValueExpression>();
  EXPECT_EQ(*original_expr, *deserialized_expr);
  EXPECT_EQ(original_expr->GetColumnName(), deserialized_expr->GetColumnName());
  EXPECT_EQ(original_expr->GetTableName(), deserialized_expr->GetTableName());
  EXPECT_EQ(original_expr->GetAlias(), deserialized_expr->GetAlias());

  // Create expression
  std::unique_ptr<ColumnValueExpression> original_expr_2 =
      std::make_unique<ColumnValueExpression>("table_name", "column_name");

  // Serialize expression
  auto json_2 = original_expr_2->ToJson();
  EXPECT_FALSE(json_2.is_null());

  // Deserialize expression
  auto deserialized_2 = DeserializeExpression(json_2);
  auto deserialized_expr_2 =
      common::ManagedPointer(deserialized_2.result_).CastManagedPointerTo<ColumnValueExpression>();
  EXPECT_EQ(*original_expr_2, *deserialized_expr_2);
  EXPECT_EQ(original_expr_2->GetAlias(), deserialized_expr_2->GetAlias());
  EXPECT_EQ(original_expr_2->GetColumnName(), deserialized_expr_2->GetColumnName());
  EXPECT_EQ(original_expr_2->GetTableName(), deserialized_expr_2->GetTableName());

  // Create expression
  std::unique_ptr<ColumnValueExpression> original_expr_3 =
      std::make_unique<ColumnValueExpression>("table_name", "column_name");

  // Serialize expression
  auto json_3 = original_expr_3->ToJson();
  EXPECT_FALSE(json_3.is_null());

  // Deserialize expression
  auto deserialized_3 = DeserializeExpression(json_3);
  auto deserialized_expr_3 =
      common::ManagedPointer(deserialized_3.result_).CastManagedPointerTo<ColumnValueExpression>();
  EXPECT_EQ(*original_expr_3, *deserialized_expr_3);
  EXPECT_EQ(original_expr_3->GetAlias(), deserialized_expr_3->GetAlias());
  EXPECT_EQ(original_expr_3->GetColumnName(), deserialized_expr_3->GetColumnName());
  EXPECT_EQ(original_expr_3->GetTableName(), deserialized_expr_3->GetTableName());
  EXPECT_EQ(original_expr_3->GetColumnOid(), deserialized_expr_3->GetColumnOid());
  EXPECT_EQ(original_expr_3->GetTableOid(), deserialized_expr_3->GetTableOid());
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
  EXPECT_EQ(tve1->GetExpressionName(), "");

  delete tve1;
  delete tve2;
  delete tve3;
  delete tve4;
  delete tve5;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, DerivedValueExpressionJsonTest) {
  // Create expression
  std::unique_ptr<DerivedValueExpression> original_expr =
      std::make_unique<DerivedValueExpression>(type::TypeId::BOOLEAN, 3, 3);

  EXPECT_EQ(*original_expr, *(original_expr->Copy()));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized = DeserializeExpression(json);
  auto deserialized_expr = common::ManagedPointer(deserialized.result_).CastManagedPointerTo<DerivedValueExpression>();
  EXPECT_EQ(*original_expr, *deserialized_expr);
  EXPECT_EQ(original_expr->GetTupleIdx(), deserialized_expr->GetTupleIdx());
  EXPECT_EQ(original_expr->GetValueIdx(), deserialized_expr->GetValueIdx());
  EXPECT_EQ(original_expr->GetReturnValueType(), deserialized_expr->GetReturnValueType());
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ComparisonExpressionJsonTest) {
  // No Generic ComparisonExpression Test needed as it is simple.
  std::vector<std::unique_ptr<AbstractExpression>> children;
  children.emplace_back(std::make_unique<ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(1)));
  children.emplace_back(std::make_unique<ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(2)));

  // Create expression
  auto original_expr = new ComparisonExpression(ExpressionType::COMPARE_EQUAL, std::move(children));
  EXPECT_EQ(original_expr->GetExpressionType(), ExpressionType::COMPARE_EQUAL);
  EXPECT_EQ(original_expr->GetReturnValueType(), type::TypeId::BOOLEAN);
  original_expr->DeriveExpressionName();
  EXPECT_EQ(original_expr->GetExpressionName(), "");
  EXPECT_EQ(*original_expr, *(original_expr->Copy()));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized = DeserializeExpression(json);
  auto deserialized_expr = common::ManagedPointer(deserialized.result_);
  EXPECT_EQ(*original_expr, *deserialized_expr);

  delete original_expr;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, StarExpressionJsonTest) {
  // No Generic StarExpression Test needed as it is simple.

  auto original_expr = new StarExpression();
  EXPECT_EQ(original_expr->GetExpressionType(), ExpressionType::STAR);
  EXPECT_EQ(original_expr->GetReturnValueType(), type::TypeId::INTEGER);
  original_expr->DeriveExpressionName();
  EXPECT_EQ(original_expr->GetExpressionName(), "");

  auto copy = original_expr->Copy();
  EXPECT_EQ(*original_expr, *copy);

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized = DeserializeExpression(json);
  auto deserialized_expr = common::ManagedPointer(deserialized.result_);
  EXPECT_EQ(*original_expr, *deserialized_expr);

  delete original_expr;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, DefaultValueExpressionJsonTest) {
  // No Generic DefaultExpression Test needed as it is simple.

  auto original_expr = new DefaultValueExpression();
  EXPECT_EQ(original_expr->GetExpressionType(), ExpressionType::VALUE_DEFAULT);
  EXPECT_EQ(original_expr->GetReturnValueType(), type::TypeId::INVALID);
  original_expr->DeriveExpressionName();
  EXPECT_EQ(original_expr->GetExpressionName(), "");
  auto copy = original_expr->Copy();
  EXPECT_EQ(*original_expr, *copy);

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized = DeserializeExpression(json);
  auto deserialized_expr = common::ManagedPointer(deserialized.result_);
  EXPECT_EQ(*original_expr, *deserialized_expr);

  delete original_expr;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, SubqueryExpressionTest) {
  // Current implementation of subquery expression's ==operator and Hash() function
  // only handles: all fields inherited from abstract expression class and
  // subselect's select columns, where condition, and distinct flag
  auto stmts0 = parser::PostgresParser::BuildParseTree(
      "SELECT * FROM foo INNER JOIN bar ON foo.a = bar.a WHERE foo.a > 0 GROUP BY foo.b ORDER BY bar.b ASC LIMIT 5;");
  EXPECT_EQ(stmts0->GetStatements().size(), 1);
  EXPECT_EQ(stmts0->GetStatement(0)->GetType(), StatementType::SELECT);

  auto select0 = std::unique_ptr<SelectStatement>(
      reinterpret_cast<SelectStatement *>(stmts0->TakeStatementsOwnership()[0].release()));
  auto subselect_expr0 = new SubqueryExpression(std::move(select0));

  auto stmts1 = parser::PostgresParser::BuildParseTree(
      "SELECT * FROM foo INNER JOIN bar ON foo.a = bar.a WHERE foo.a > 0 GROUP BY foo.b ORDER BY bar.b ASC LIMIT 5;");
  auto select1 = std::unique_ptr<SelectStatement>(
      reinterpret_cast<SelectStatement *>(stmts1->TakeStatementsOwnership()[0].release()));
  auto subselect_expr1 = new SubqueryExpression(std::move(select1));

  // different in select columns
  auto stmts2 = parser::PostgresParser::BuildParseTree(
      "SELECT a, b FROM foo INNER JOIN bar ON foo.a = bar.a WHERE foo.a > 0 GROUP BY foo.b ORDER BY bar.b ASC LIMIT "
      "5;");
  auto select2 = std::unique_ptr<SelectStatement>(
      reinterpret_cast<SelectStatement *>(stmts2->TakeStatementsOwnership()[0].release()));
  auto subselect_expr2 = new SubqueryExpression(std::move(select2));

  // different in distinct flag
  auto stmts3 = parser::PostgresParser::BuildParseTree(
      "SELECT DISTINCT a, b FROM foo INNER JOIN bar ON foo.a = bar.a WHERE foo.a > 0 GROUP BY foo.b ORDER BY bar.b ASC "
      "LIMIT 5;");
  auto select3 = std::unique_ptr<SelectStatement>(
      reinterpret_cast<SelectStatement *>(stmts3->TakeStatementsOwnership()[0].release()));
  auto subselect_expr3 = new SubqueryExpression(std::move(select3));

  // different in where
  auto stmts4 = parser::PostgresParser::BuildParseTree(
      "SELECT * FROM foo INNER JOIN bar ON foo.b = bar.a WHERE foo.b > 0 GROUP BY foo.b ORDER BY bar.b ASC LIMIT 5;");
  auto select4 = std::unique_ptr<SelectStatement>(
      reinterpret_cast<SelectStatement *>(stmts4->TakeStatementsOwnership()[0].release()));
  auto subselect_expr4 = new SubqueryExpression(std::move(select4));

  // different in where
  auto stmts5 = parser::PostgresParser::BuildParseTree("SELECT * FROM foo INNER JOIN bar ON foo.b = bar.a;");
  auto select5 = std::unique_ptr<SelectStatement>(
      reinterpret_cast<SelectStatement *>(stmts5->TakeStatementsOwnership()[0].release()));
  auto subselect_expr5 = new SubqueryExpression(std::move(select5));

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
  EXPECT_EQ(subselect_expr0->GetExpressionName(), "");

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
  auto result = parser::PostgresParser::BuildParseTree("SELECT * FROM foo;");
  EXPECT_EQ(result->GetStatements().size(), 1);
  EXPECT_EQ(result->GetStatement(0)->GetType(), StatementType::SELECT);

  auto select = std::unique_ptr<SelectStatement>(
      reinterpret_cast<SelectStatement *>(result->TakeStatementsOwnership()[0].release()));
  auto original_expr = std::make_unique<SubqueryExpression>(std::move(select));
  EXPECT_EQ(*original_expr, *(original_expr->Copy()));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized = DeserializeExpression(json);
  auto deserialized_expr = common::ManagedPointer(deserialized.result_).CastManagedPointerTo<SubqueryExpression>();
  EXPECT_EQ(*original_expr, *deserialized_expr);
  EXPECT_TRUE(deserialized_expr->GetSubselect() != nullptr);
  EXPECT_TRUE(deserialized_expr->GetSubselect()->GetSelectTable() != nullptr);
  EXPECT_EQ(original_expr->GetSubselect()->GetSelectTable()->GetTableName(),
            deserialized_expr->GetSubselect()->GetSelectTable()->GetTableName());
  EXPECT_EQ(original_expr->GetSubselect()->GetSelectColumns().size(),
            deserialized_expr->GetSubselect()->GetSelectColumns().size());
  EXPECT_EQ(1, deserialized_expr->GetSubselect()->GetSelectColumns().size());
  EXPECT_EQ(original_expr->GetSubselect()->GetSelectColumns()[0]->GetExpressionType(),
            deserialized_expr->GetSubselect()->GetSelectColumns()[0]->GetExpressionType());
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ComplexSubqueryExpressionJsonTest) {
  // Create expression
  auto result = parser::PostgresParser::BuildParseTree(
      "SELECT * FROM foo INNER JOIN bar ON foo.a = bar.a GROUP BY foo.b ORDER BY bar.b ASC LIMIT 5;");
  EXPECT_EQ(result->GetStatements().size(), 1);
  EXPECT_EQ(result->GetStatements()[0]->GetType(), StatementType::SELECT);

  auto select = std::unique_ptr<SelectStatement>(
      reinterpret_cast<SelectStatement *>(result->TakeStatementsOwnership()[0].release()));
  auto original_expr = std::make_unique<SubqueryExpression>(std::move(select));

  // Serialize expression
  auto json = original_expr->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize expression
  auto deserialized = DeserializeExpression(json);
  auto deserialized_expr = common::ManagedPointer(deserialized.result_).CastManagedPointerTo<SubqueryExpression>();
  EXPECT_EQ(*original_expr, *deserialized_expr);

  // Check Limit
  auto subselect = deserialized_expr->GetSubselect();
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

}  // namespace noisepage::parser::expression
