#pragma once
#include "execution/sql/value.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/derived_value_expression.h"
#include "parser/expression/operator_expression.h"
#include "type/transient_value_factory.h"

namespace terrier::execution::compiler {

/**
 * Helper class to reduce typing and increase readability when hand crafting expression.
 */
class ExpressionUtil {
 public:
  using Expression = std::shared_ptr<parser::AbstractExpression>;
  using AggExpression = std::shared_ptr<parser::AggregateExpression>;

  /**
   * Create an integer constant expression
   */
  static Expression Constant(int32_t val) {
    return std::make_shared<parser::ConstantValueExpression>(type::TransientValueFactory::GetInteger(val));
  }

  /**
   * Create a floating point constant expression
   */
  static Expression Constant(double val) {
    return std::make_shared<parser::ConstantValueExpression>(type::TransientValueFactory::GetDecimal(val));
  }

  /**
   * Create a date constant expression
   */
  static Expression Constant(int16_t year, uint8_t month, uint8_t day) {
    sql::Date date(year, month, day);
    type::date_t int_val(date.int_val);
    return std::make_shared<parser::ConstantValueExpression>(type::TransientValueFactory::GetDate(int_val));
  }

  /**
   * Create a date constant expression
   */
  static Expression Constant(date::year_month_day ymd) {
    sql::Date date(ymd.year(), ymd.month(), ymd.day());
    type::date_t int_val(date.int_val);
    return std::make_shared<parser::ConstantValueExpression>(type::TransientValueFactory::GetDate(int_val));
  }

  /**
   * Create a column value expression
   */
  static Expression CVE(catalog::col_oid_t column_oid, type::TypeId type) {
    return std::make_shared<parser::ColumnValueExpression>(catalog::db_oid_t(0), catalog::table_oid_t(0), column_oid,
                                                           type);
  }

  /**
   * Create a derived value expression
   */
  static Expression DVE(type::TypeId type, int tuple_idx, int value_idx) {
    return std::make_shared<parser::DerivedValueExpression>(type, tuple_idx, value_idx);
  }

  /**
   * Create a Comparison expression
   */
  static Expression Comparison(parser::ExpressionType comp_type, Expression child1, Expression child2) {
    return std::make_shared<parser::ComparisonExpression>(comp_type, std::vector<Expression>{child1, child2});
  }

  /**
   *  expression for child1 == child2
   */
  static Expression ComparisonEq(Expression child1, Expression child2) {
    return Comparison(parser::ExpressionType::COMPARE_EQUAL, child1, child2);
  }

  /**
   * Create expression for child1 == child2
   */
  static Expression ComparisonNeq(Expression child1, Expression child2) {
    return Comparison(parser::ExpressionType::COMPARE_NOT_EQUAL, child1, child2);
  }

  /**
   * Create expression for child1 < child2
   */
  static Expression ComparisonLt(Expression child1, Expression child2) {
    return Comparison(parser::ExpressionType::COMPARE_LESS_THAN, child1, child2);
  }

  /**
   * Create expression for child1 <= child2
   */
  static Expression ComparisonLe(Expression child1, Expression child2) {
    return Comparison(parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO, child1, child2);
  }

  /**
   * Create expression for child1 > child2
   */
  static Expression ComparisonGt(Expression child1, Expression child2) {
    return Comparison(parser::ExpressionType::COMPARE_GREATER_THAN, child1, child2);
  }

  /**
   * Create expression for child1 >= child2
   */
  static Expression ComparisonGe(Expression child1, Expression child2) {
    return Comparison(parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO, child1, child2);
  }

  /**
   * Create a unary operation expression
   */
  static Expression Operator(parser::ExpressionType op_type, type::TypeId ret_type, Expression child) {
    return std::make_shared<parser::OperatorExpression>(op_type, ret_type, std::vector<Expression>{child});
  }

  /**
   * Create a binary operation expression
   */
  static Expression Operator(parser::ExpressionType op_type, type::TypeId ret_type, Expression child1,
                             Expression child2) {
    return std::make_shared<parser::OperatorExpression>(op_type, ret_type, std::vector<Expression>{child1, child2});
  }

  /**
   * create expression for child1 + child2
   */
  static Expression OpSum(Expression child1, Expression child2) {
    return Operator(parser::ExpressionType::OPERATOR_PLUS, child1->GetReturnValueType(), child1, child2);
  }

  /**
   * create expression for child1 - child2
   */
  static Expression OpMin(Expression child1, Expression child2) {
    return Operator(parser::ExpressionType::OPERATOR_MINUS, child1->GetReturnValueType(), child1, child2);
  }

  /**
   * create expression for child1 * child2
   */
  static Expression OpMul(Expression child1, Expression child2) {
    return Operator(parser::ExpressionType::OPERATOR_MULTIPLY, child1->GetReturnValueType(), child1, child2);
  }

  /**
   * create expression for child1 / child2
   */
  static Expression OpDiv(Expression child1, Expression child2) {
    return Operator(parser::ExpressionType::OPERATOR_DIVIDE, child1->GetReturnValueType(), child1, child2);
  }

  /**
   * Create expression for child1 AND/OR child2
   */
  static Expression Conjunction(parser::ExpressionType op_type, Expression child1, Expression child2) {
    return std::make_shared<parser::ConjunctionExpression>(op_type, std::vector<Expression>{child1, child2});
  }

  /**
   * Create expression for child1 AND child2
   */
  static Expression ConjunctionAnd(Expression child1, Expression child2) {
    return Conjunction(parser::ExpressionType::CONJUNCTION_AND, child1, child2);
  }

  /**
   * Create expression for child1 OR child2
   */
  static Expression ConjunctionOr(Expression child1, Expression child2) {
    return Conjunction(parser::ExpressionType::CONJUNCTION_OR, child1, child2);
  }

  /**
   * Create an aggregate expression
   */
  static AggExpression AggregateTerm(parser::ExpressionType agg_type, Expression child) {
    return std::make_shared<parser::AggregateExpression>(agg_type, std::vector<Expression>{child}, false);
  }

  /**
   * Create a sum aggregate expression
   */
  static AggExpression AggSum(Expression child) { return AggregateTerm(parser::ExpressionType::AGGREGATE_SUM, child); }

  /**
   * Create a avg aggregate expression
   */
  static AggExpression AggAvg(Expression child) { return AggregateTerm(parser::ExpressionType::AGGREGATE_AVG, child); }

  /**
   * Create a count aggregate expression
   */
  static AggExpression AggCount(Expression child) {
    return AggregateTerm(parser::ExpressionType::AGGREGATE_COUNT, child);
  }

  /**
   * Create a count star aggregate expression
   */
  static AggExpression AggCountStar() { return AggregateTerm(parser::ExpressionType::AGGREGATE_COUNT, Constant(1)); }
};
}  // namespace terrier::execution::compiler