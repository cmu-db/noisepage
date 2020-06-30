#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/ast/builtins.h"
#include "execution/sql/value.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/derived_value_expression.h"
#include "parser/expression/operator_expression.h"

namespace terrier::execution::compiler::test {

/**
 * Helper class to reduce typing and increase readability when hand crafting expression.
 */
class ExpressionMaker {
  using NewExpression = std::unique_ptr<parser::AbstractExpression>;
  using NewAggExpression = std::unique_ptr<parser::AggregateExpression>;

 public:
  using Expression = const parser::AbstractExpression *;
  using AggExpression = const parser::AggregateExpression *;

  /**
   * @return A constant expression with the given boolean value.
   */
  Expression ConstantBool(bool val) {
    return Alloc(std::make_unique<parser::ConstantValueExpression>(sql::GenericValue::CreateBoolean(val)));
  }

  /**
   * Create an integer constant expression
   */
  Expression Constant(int32_t val) {
    return Alloc(std::make_unique<parser::ConstantValueExpression>(sql::GenericValue::CreateInteger(val)));
  }

  /**
   * Create a floating point constant expression
   */
  Expression Constant(float val) {
    return Alloc(std::make_unique<parser::ConstantValueExpression>(sql::GenericValue::CreateFloat(val)));
  }

  /**
   * Create a date constant expression
   */
  Expression Constant(int16_t year, uint8_t month, uint8_t day) {
    return Alloc(std::make_unique<parser::ConstantValueExpression>(
        sql::GenericValue::CreateDate(static_cast<uint32_t>(year), month, day)));
  }

  /**
   * Create a string constant expression
   */
  Expression Constant(const std::string &str) {
    return Alloc(std::make_unique<parser::ConstantValueExpression>(sql::GenericValue::CreateVarchar(str)));
  }

  /**
   * Create a column value expression
   */
  Expression CVE(uint16_t column_oid, sql::TypeId type) {
    return Alloc(std::make_unique<parser::ColumnValueExpression>(column_oid, type));
  }

  /**
   * Create a derived value expression
   */
  Expression DVE(sql::TypeId type, int tuple_idx, int value_idx) {
    return Alloc(std::make_unique<parser::DerivedValueExpression>(type, tuple_idx, value_idx));
  }

  /**
   * Create a Comparison expression
   */
  Expression Compare(parser::ExpressionType comp_type, Expression child1, Expression child2) {
    return Alloc(std::make_unique<parser::ComparisonExpression>(comp_type, std::vector<Expression>{child1, child2}));
  }

  /**
   *  expression for child1 == child2
   */
  Expression CompareEq(Expression child1, Expression child2) {
    return Compare(parser::ExpressionType::COMPARE_EQUAL, child1, child2);
  }

  /**
   * Create expression for child1 == child2
   */
  Expression CompareNeq(Expression child1, Expression child2) {
    return Compare(parser::ExpressionType::COMPARE_NOT_EQUAL, child1, child2);
  }

  /**
   * Create expression for child1 < child2
   */
  Expression CompareLt(Expression child1, Expression child2) {
    return Compare(parser::ExpressionType::COMPARE_LESS_THAN, child1, child2);
  }

  /**
   * Create expression for child1 <= child2
   */
  Expression CompareLe(Expression child1, Expression child2) {
    return Compare(parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO, child1, child2);
  }

  /**
   * Create expression for child1 > child2
   */
  Expression CompareGt(Expression child1, Expression child2) {
    return Compare(parser::ExpressionType::COMPARE_GREATER_THAN, child1, child2);
  }

  /**
   * Create expression for child1 >= child2
   */
  Expression CompareGe(Expression child1, Expression child2) {
    return Compare(parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO, child1, child2);
  }

  /**
   * Create a unary operation expression
   */
  Expression Operator(parser::ExpressionType op_type, sql::TypeId ret_type, Expression child) {
    return Alloc(std::make_unique<parser::OperatorExpression>(op_type, ret_type, std::vector<Expression>{child}));
  }

  /**
   * Create a binary operation expression
   */
  Expression Operator(parser::ExpressionType op_type, sql::TypeId ret_type, Expression child1, Expression child2) {
    return Alloc(
        std::make_unique<parser::OperatorExpression>(op_type, ret_type, std::vector<Expression>{child1, child2}));
  }

  /**
   * create expression for child1 + child2
   */
  Expression OpSum(Expression child1, Expression child2) {
    return Operator(parser::ExpressionType::OPERATOR_PLUS, child1->GetReturnValueType(), child1, child2);
  }

  /**
   * create expression for child1 - child2
   */
  Expression OpMin(Expression child1, Expression child2) {
    return Operator(parser::ExpressionType::OPERATOR_MINUS, child1->GetReturnValueType(), child1, child2);
  }

  /**
   * create expression for child1 * child2
   */
  Expression OpMul(Expression child1, Expression child2) {
    return Operator(parser::ExpressionType::OPERATOR_MULTIPLY, child1->GetReturnValueType(), child1, child2);
  }

  /**
   * create expression for child1 / child2
   */
  Expression OpDiv(Expression child1, Expression child2) {
    return Operator(parser::ExpressionType::OPERATOR_DIVIDE, child1->GetReturnValueType(), child1, child2);
  }

  /**
   * Create expression for NOT(child)
   */
  Expression OpNot(Expression child) {
    return Operator(parser::ExpressionType::OPERATOR_NOT, sql::TypeId::Boolean, child);
  }

  /**
   * Create expression for child1 AND/OR child2
   */
  Expression Conjunction(parser::ExpressionType op_type, Expression child1, Expression child2) {
    return Alloc(std::make_unique<parser::ConjunctionExpression>(op_type, std::vector<Expression>{child1, child2}));
  }

  /**
   * Create expression for child1 AND child2
   */
  Expression ConjunctionAnd(Expression child1, Expression child2) {
    return Conjunction(parser::ExpressionType::CONJUNCTION_AND, child1, child2);
  }

  /**
   * Create expression for child1 OR child2
   */
  Expression ConjunctionOr(Expression child1, Expression child2) {
    return Conjunction(parser::ExpressionType::CONJUNCTION_OR, child1, child2);
  }

  /**
   * Create an expression for a builtin call.
   */
  Expression BuiltinFunction(ast::Builtin builtin, std::vector<Expression> args, const sql::TypeId return_value_type) {
    return Alloc(std::make_unique<parser::BuiltinFunctionExpression>(builtin, std::move(args), return_value_type));
  }

  /**
   * Create an aggregate expression
   */
  AggExpression AggregateTerm(parser::ExpressionType agg_type, Expression child, bool distinct) {
    return Alloc(std::make_unique<parser::AggregateExpression>(agg_type, std::vector<Expression>{child}, distinct));
  }

  /**
   * Create a sum aggregate expression
   */
  AggExpression AggSum(Expression child, bool distinct = false) {
    return AggregateTerm(parser::ExpressionType::AGGREGATE_SUM, child, distinct);
  }

  /**
   * Create a sum aggregate expression
   */
  AggExpression AggMin(Expression child, bool distinct = false) {
    return AggregateTerm(parser::ExpressionType::AGGREGATE_MIN, child, distinct);
  }

  /**
   * Create a sum aggregate expression
   */
  AggExpression AggMax(Expression child, bool distinct = false) {
    return AggregateTerm(parser::ExpressionType::AGGREGATE_MAX, child, distinct);
  }

  /**
   * Create a avg aggregate expression
   */
  AggExpression AggAvg(Expression child, bool distinct = false) {
    return AggregateTerm(parser::ExpressionType::AGGREGATE_AVG, child, distinct);
  }

  /**
   * Create a count aggregate expression
   */
  AggExpression AggCount(Expression child, bool distinct = false) {
    return AggregateTerm(parser::ExpressionType::AGGREGATE_COUNT, child, distinct);
  }

  /**
   * Create a count aggregate expression
   */
  AggExpression AggCountStar() { return AggregateTerm(parser::ExpressionType::AGGREGATE_COUNT, Constant(1), false); }

 private:
  Expression Alloc(NewExpression &&new_expr) {
    allocated_exprs_.emplace_back(std::move(new_expr));
    return allocated_exprs_.back().get();
  }

  AggExpression Alloc(NewAggExpression &&agg_expr) {
    allocated_aggs_.emplace_back(std::move(agg_expr));
    return allocated_aggs_.back().get();
  }

  std::vector<NewExpression> allocated_exprs_;
  std::vector<NewAggExpression> allocated_aggs_;
};

}  // namespace terrier::execution::compiler::test
