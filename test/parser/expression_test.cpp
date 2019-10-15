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
  EXPECT_EQ(expr_ti_1->GetChildren(), std::vector<common::ManagedPointer<AbstractExpression>>());
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
  auto original_expr = std::make_unique<ConstantValueExpression>(
      type::TransientValueFactory::GetVarChar("ConstantValueExpressionJsonTest"));

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
  EXPECT_EQ(deserialized_expr.CastManagedPointerTo<ConstantValueExpression>()->GetValue(),
            type::TransientValueFactory::GetVarChar("ConstantValueExpressionJsonTest"));
}

// NOLINTNEXTLINE
TEST(ExpressionTests, NullConstantValueExpressionJsonTest) {
  // Create expression
  auto original_expr =
      std::make_unique<ConstantValueExpression>(type::TransientValueFactory::GetNull(type::TypeId::VARCHAR));

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
  EXPECT_TRUE(deserialized_expr.CastManagedPointerTo<ConstantValueExpression>()->GetValue().Null());
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ConjunctionExpressionTest) {
  std::vector<std::unique_ptr<AbstractExpression>> children1;
  children1.emplace_back(std::make_unique<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
  children1.emplace_back(std::make_unique<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(false)));
  std::vector<std::unique_ptr<AbstractExpression>> children1cp;
  children1cp.emplace_back(std::make_unique<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
  children1cp.emplace_back(std::make_unique<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(false)));
  auto c_expr_1 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, std::move(children1));

  std::vector<std::unique_ptr<AbstractExpression>> children2;
  children2.emplace_back(std::make_unique<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
  children2.emplace_back(std::make_unique<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(false)));
  auto c_expr_2 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, std::move(children2));

  std::vector<std::unique_ptr<AbstractExpression>> children3;
  children3.emplace_back(std::make_unique<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
  children3.emplace_back(std::make_unique<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
  auto c_expr_3 = new ConjunctionExpression(ExpressionType::CONJUNCTION_AND, std::move(children3));

  std::vector<std::unique_ptr<AbstractExpression>> children4;
  children4.emplace_back(std::make_unique<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
  children4.emplace_back(std::make_unique<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(false)));
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
  EXPECT_EQ(c_expr_1->GetExpressionName(), "AND BOOLEAN AND BOOLEAN");

  delete c_expr_1;
  delete c_expr_2;
  delete c_expr_3;
  delete c_expr_4;
}

// NOLINTNEXTLINE
TEST(ExpressionTests, ConjunctionExpressionJsonTest) {
  // Create expression
  std::vector<std::unique_ptr<AbstractExpression>> children1;
  children1.emplace_back(std::make_unique<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
  children1.emplace_back(std::make_unique<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(false)));
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
  children_5.emplace_back(std::make_unique<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(false)));
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
  EXPECT_EQ(agg_expr_1->GetExpressionName(), "COUNT STAR");

  // Testing DeriveReturnValueType functionality
  std::vector<std::unique_ptr<AbstractExpression>> children_6;
  children_6.emplace_back(std::make_unique<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
  auto agg_expr_6 = new AggregateExpression(ExpressionType::AGGREGATE_MAX, std::move(children_6), true);
  agg_expr_6->DeriveReturnValueType();

  EXPECT_FALSE(*agg_expr_1 == *agg_expr_6);
  EXPECT_NE(agg_expr_1->Hash(), agg_expr_6->Hash());
  EXPECT_EQ(agg_expr_6->GetReturnValueType(), type::TransientValueFactory::GetBoolean(true).Type());

  // Testing DeriveReturnValueType functionality
  std::vector<std::unique_ptr<AbstractExpression>> children_7;
  children_7.emplace_back(std::make_unique<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
  auto agg_expr_7 = new AggregateExpression(ExpressionType::AGGREGATE_AVG, std::move(children_7), true);
  agg_expr_7->DeriveReturnValueType();

  EXPECT_FALSE(*agg_expr_1 == *agg_expr_7);
  EXPECT_NE(agg_expr_1->Hash(), agg_expr_7->Hash());
  EXPECT_EQ(agg_expr_7->GetReturnValueType(), type::TypeId::DECIMAL);

  // Testing DeriveReturnValueType functionality
  std::vector<std::unique_ptr<AbstractExpression>> children_8;
  children_8.emplace_back(std::make_unique<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
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
      std::make_unique<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(false)),
      std::make_unique<StarExpression>()});
  auto case_expr_3 =
      new CaseExpression(type::TypeId::BOOLEAN, std::move(when_clauses_3),
                         std::make_unique<ConstantValueExpression>(type::TransientValueFactory::GetBoolean(false)));

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
  EXPECT_EQ(case_expr->GetExpressionName(), "OPERATOR_CASE_EXPR");

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
}

// NOLINTNEXTLINE
TEST(ExpressionTests, StarExpressionJsonTest) {
  // No Generic StarExpression Test needed as it is simple.

  auto original_expr = new StarExpression();
  EXPECT_EQ(original_expr->GetExpressionType(), ExpressionType::STAR);
  EXPECT_EQ(original_expr->GetReturnValueType(), type::TypeId::INVALID);
  original_expr->DeriveExpressionName();
  EXPECT_EQ(original_expr->GetExpressionName(), "STAR");

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
  EXPECT_EQ(original_expr->GetExpressionName(), "VALUE_DEFAULT");
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
  PostgresParser pgparser;
  auto stmts0 = pgparser.BuildParseTree(
      "SELECT * FROM foo INNER JOIN bar ON foo.a = bar.a WHERE foo.a > 0 GROUP BY foo.b ORDER BY bar.b ASC LIMIT 5;");
  EXPECT_EQ(stmts0.GetStatements().size(), 1);
  EXPECT_EQ(stmts0.GetStatement(0)->GetType(), StatementType::SELECT);

  auto select0 = std::unique_ptr<SelectStatement>(
      reinterpret_cast<SelectStatement *>(stmts0.TakeStatementsOwnership()[0].release()));
  auto subselect_expr0 = new SubqueryExpression(std::move(select0));

  auto stmts1 = pgparser.BuildParseTree(
      "SELECT * FROM foo INNER JOIN bar ON foo.a = bar.a WHERE foo.a > 0 GROUP BY foo.b ORDER BY bar.b ASC LIMIT 5;");
  auto select1 = std::unique_ptr<SelectStatement>(
      reinterpret_cast<SelectStatement *>(stmts1.TakeStatementsOwnership()[0].release()));
  auto subselect_expr1 = new SubqueryExpression(std::move(select1));

  // different in select columns
  auto stmts2 = pgparser.BuildParseTree(
      "SELECT a, b FROM foo INNER JOIN bar ON foo.a = bar.a WHERE foo.a > 0 GROUP BY foo.b ORDER BY bar.b ASC LIMIT "
      "5;");
  auto select2 = std::unique_ptr<SelectStatement>(
      reinterpret_cast<SelectStatement *>(stmts2.TakeStatementsOwnership()[0].release()));
  auto subselect_expr2 = new SubqueryExpression(std::move(select2));

  // different in distinct flag
  auto stmts3 = pgparser.BuildParseTree(
      "SELECT DISTINCT a, b FROM foo INNER JOIN bar ON foo.a = bar.a WHERE foo.a > 0 GROUP BY foo.b ORDER BY bar.b ASC "
      "LIMIT 5;");
  auto select3 = std::unique_ptr<SelectStatement>(
      reinterpret_cast<SelectStatement *>(stmts3.TakeStatementsOwnership()[0].release()));
  auto subselect_expr3 = new SubqueryExpression(std::move(select3));

  // different in where
  auto stmts4 = pgparser.BuildParseTree(
      "SELECT * FROM foo INNER JOIN bar ON foo.b = bar.a WHERE foo.b > 0 GROUP BY foo.b ORDER BY bar.b ASC LIMIT 5;");
  auto select4 = std::unique_ptr<SelectStatement>(
      reinterpret_cast<SelectStatement *>(stmts4.TakeStatementsOwnership()[0].release()));
  auto subselect_expr4 = new SubqueryExpression(std::move(select4));

  // different in where
  auto stmts5 = pgparser.BuildParseTree("SELECT * FROM foo INNER JOIN bar ON foo.b = bar.a;");
  auto select5 = std::unique_ptr<SelectStatement>(
      reinterpret_cast<SelectStatement *>(stmts5.TakeStatementsOwnership()[0].release()));
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
  auto result = pgparser.BuildParseTree("SELECT * FROM foo;");
  EXPECT_EQ(result.GetStatements().size(), 1);
  EXPECT_EQ(result.GetStatement(0)->GetType(), StatementType::SELECT);

  auto select = std::unique_ptr<SelectStatement>(
      reinterpret_cast<SelectStatement *>(result.TakeStatementsOwnership()[0].release()));
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
  PostgresParser pgparser;
  auto result = pgparser.BuildParseTree(
      "SELECT * FROM foo INNER JOIN bar ON foo.a = bar.a GROUP BY foo.b ORDER BY bar.b ASC LIMIT 5;");
  EXPECT_EQ(result.GetStatements().size(), 1);
  EXPECT_EQ(result.GetStatements()[0]->GetType(), StatementType::SELECT);

  auto select = std::unique_ptr<SelectStatement>(
      reinterpret_cast<SelectStatement *>(result.TakeStatementsOwnership()[0].release()));
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

}  // namespace terrier::parser::expression
