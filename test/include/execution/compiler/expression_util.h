#pragma once
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/tuple_value_expression.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "execution/sql/value.h"

namespace terrier::parser {

/**
 * Helper class to reduce typing and increase readability when hand crafting expression.
 */
class ExpressionUtil {
 public:
  using Expression = std::shared_ptr<AbstractExpression>;
  using AggExpression = std::shared_ptr<AggregateExpression>;

/**
 * Create an integer constant expression
 */
  static Expression Constant(int32_t val) {
    return std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetInteger(val));
  }

/**
 * Create a floating point constant expression
 */
  static Expression Constant(double val) {
    return std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetDecimal(val));
  }

/**
 * Create a date constant expression
 */
  static Expression Constant(i16 year, u8 month, u8 day) {
    tpl::sql::Date date(year, month, day);
    type::date_t int_val(date.int_val);
    return std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetDate(int_val));
  }

/**
 * Create a date constant expression
 */
  static Expression Constant(date::year_month_day ymd) {
    tpl::sql::Date date(ymd.year(), ymd.month(), ymd.day());
    type::date_t int_val(date.int_val);
    return std::make_shared<ConstantValueExpression>(type::TransientValueFactory::GetDate(int_val));
  }

/**
 * Create a tuple value expression
 */
  static Expression TVE(uint32_t tuple_idx, uint32_t attr_idx, type::TypeId type) {
    return std::make_shared<ExecTupleValueExpression>(tuple_idx, attr_idx, type);
  }

/**
 * Create a Comparison expression
 */
  static Expression Comparison(ExpressionType comp_type, Expression child1, Expression child2) {
    return std::make_shared<ComparisonExpression>(comp_type, std::vector<Expression>{child1, child2});
  }

/**
 *  expression for child1 == child2
 */
  static Expression ComparisonEq(Expression child1, Expression child2) {
    return Comparison(ExpressionType::COMPARE_EQUAL, child1, child2);
  }

/**
 * Create expression for child1 == child2
 */
  static Expression ComparisonNeq(Expression child1, Expression child2) {
    return Comparison(ExpressionType::COMPARE_NOT_EQUAL, child1, child2);
  }

/**
 * Create expression for child1 < child2
 */
  static Expression ComparisonLt(Expression child1, Expression child2) {
    return Comparison(ExpressionType::COMPARE_LESS_THAN, child1, child2);
  }

/**
 * Create expression for child1 <= child2
 */
  static Expression ComparisonLe(Expression child1, Expression child2) {
    return Comparison(ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO, child1, child2);
  }

/**
 * Create expression for child1 > child2
 */
  static Expression ComparisonGt(Expression child1, Expression child2) {
    return Comparison(ExpressionType::COMPARE_GREATER_THAN, child1, child2);
  }

/**
 * Create expression for child1 >= child2
 */
  static Expression ComparisonGe(Expression child1, Expression child2) {
    return Comparison(ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO, child1, child2);
  }

/**
 * Create a unary operation expression
 */
  static Expression Operator(ExpressionType op_type, type::TypeId ret_type, Expression child) {
    return std::make_shared<OperatorExpression>(op_type, ret_type, std::vector<Expression>{child});
  }


/**
 * Create a binary operation expression
 */
  static Expression Operator(ExpressionType op_type, type::TypeId ret_type, Expression child1, Expression child2) {
    return std::make_shared<OperatorExpression>(op_type, ret_type, std::vector<Expression>{child1, child2});
  }

/**
 * create expression for child1 + child2
 */
  static Expression OpSum(Expression child1, Expression child2) {
    return Operator(ExpressionType::OPERATOR_PLUS, child1->GetReturnValueType(), child1, child2);
  }

/**
 * create expression for child1 - child2
 */
  static Expression OpMin(Expression child1, Expression child2) {
    return Operator(ExpressionType::OPERATOR_MINUS, child1->GetReturnValueType(), child1, child2);
  }

/**
 * create expression for child1 * child2
 */
  static Expression OpMul(Expression child1, Expression child2) {
    return Operator(ExpressionType::OPERATOR_MULTIPLY, child1->GetReturnValueType(), child1, child2);
  }

/**
 * create expression for child1 / child2
 */
  static Expression OpDiv(Expression child1, Expression child2) {
    return Operator(ExpressionType::OPERATOR_DIVIDE, child1->GetReturnValueType(), child1, child2);
  }

  /**
   * Create expression for child1 AND/OR child2
   */
  static Expression Conjunction(ExpressionType op_type, Expression child1, Expression child2) {
    return std::make_shared<ConjunctionExpression>(op_type, std::vector<Expression>{child1, child2});
  }

  /**
   * Create expression for child1 AND child2
   */
  static Expression ConjunctionAnd(Expression child1, Expression child2) {
    return Conjunction(ExpressionType::CONJUNCTION_AND, child1, child2);
  }

  /**
   * Create expression for child1 OR child2
   */
  static Expression ConjunctionOr(Expression child1, Expression child2) {
    return Conjunction(ExpressionType::CONJUNCTION_OR, child1, child2);
  }


/**
 * Create an aggregate expression
 */
  static AggExpression AggregateTerm(ExpressionType agg_type, Expression child) {
    return std::make_shared<AggregateExpression>(agg_type, std::vector<Expression>{child}, false);
  }

/**
 * Create a sum aggregate expression
 */
  static AggExpression AggSum(Expression child) {
    return AggregateTerm(ExpressionType::AGGREGATE_SUM, child);
  }

/**
 * Create a avg aggregate expression
 */
  static AggExpression AggAvg(Expression child) {
    return AggregateTerm(ExpressionType::AGGREGATE_AVG, child);
  }

/**
 * Create a count aggregate expression
 */
  static AggExpression AggCount(Expression child) {
    return AggregateTerm(ExpressionType::AGGREGATE_COUNT, child);
  }

/**
 * Create a count star aggregate expression
 */
  static AggExpression AggCountStar() {
    return AggregateTerm(ExpressionType::AGGREGATE_COUNT, Constant(1));
  }
};
}