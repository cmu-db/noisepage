#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "execution/sql/value.h"
#include "execution/sql/value_util.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/derived_value_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/parameter_value_expression.h"
#include "parser/expression/star_expression.h"

namespace noisepage::execution::compiler::test {

/**
 * Helper class to reduce typing and increase readability when hand crafting expressions.
 */
class ExpressionMaker {
 public:
  using OwnedExpression = std::unique_ptr<parser::AbstractExpression>;
  using OwnedAggExpression = std::unique_ptr<parser::AggregateExpression>;
  using ManagedExpression = common::ManagedPointer<parser::AbstractExpression>;
  using ManagedAggExpression = common::ManagedPointer<parser::AggregateExpression>;

  ManagedExpression MakeManaged(OwnedExpression &&expr) {
    owned_exprs_.emplace_back(std::move(expr));
    return ManagedExpression(owned_exprs_.back());
  }

  ManagedAggExpression MakeAggManaged(OwnedAggExpression &&expr) {
    owned_agg_exprs_.emplace_back(std::move(expr));
    return ManagedAggExpression(owned_agg_exprs_.back());
  }

  /**
   * Create an integer constant expression
   */
  ManagedExpression Constant(int32_t val) {
    return MakeManaged(
        std::make_unique<parser::ConstantValueExpression>(type::TypeId::INTEGER, execution::sql::Integer(val)));
  }

  /**
   * Create a floating point constant expression
   */
  ManagedExpression Constant(double val) {
    return MakeManaged(
        std::make_unique<parser::ConstantValueExpression>(type::TypeId::DECIMAL, execution::sql::Real(val)));
  }

  /**
   * Create a string constant expression
   */
  ManagedExpression Constant(const std::string &str) {
    auto string_val = execution::sql::ValueUtil::CreateStringVal(str);
    return MakeManaged(std::make_unique<parser::ConstantValueExpression>(type::TypeId::VARCHAR, string_val.first,
                                                                         std::move(string_val.second)));
  }

  /**
   * Create a date constant expression
   */
  ManagedExpression Constant(int32_t year, uint32_t month, uint32_t day) {
    return MakeManaged(std::make_unique<parser::ConstantValueExpression>(
        type::TypeId::DATE, sql::DateVal(sql::Date::FromYMD(year, month, day))));
  }

  /**
   * Create an expression for a builtin call.
   */
  ManagedExpression Function(std::string &&func_name, const std::vector<ManagedExpression> &args,
                             const type::TypeId return_value_type, catalog::proc_oid_t proc_oid) {
    std::vector<execution::compiler::test::ExpressionMaker::OwnedExpression> children;
    children.reserve(args.size());
    for (const auto &arg : args) {
      children.emplace_back(arg->Copy());
    }
    return MakeManaged(std::make_unique<parser::FunctionExpression>(std::string{func_name}, return_value_type,
                                                                    std::move(children), proc_oid));
  }

  /**
   * Create a column value expression
   */
  ManagedExpression CVE(catalog::col_oid_t column_oid, type::TypeId type) {
    return MakeManaged(std::make_unique<parser::ColumnValueExpression>(catalog::table_oid_t(0), column_oid, type));
  }

  /**
   * Create a derived value expression
   */
  ManagedExpression DVE(type::TypeId type, int tuple_idx, int value_idx) {
    return MakeManaged(std::make_unique<parser::DerivedValueExpression>(type, tuple_idx, value_idx));
  }

  /**
   * Create a parameter value expression
   */
  ManagedExpression PVE(type::TypeId type, uint32_t param_idx) {
    return MakeManaged(std::make_unique<parser::ParameterValueExpression>(param_idx, type));
  }

  ManagedExpression Star() { return MakeManaged(std::make_unique<parser::StarExpression>()); }

  /**
   * Create a Comparison expression
   */
  ManagedExpression Comparison(parser::ExpressionType comp_type, ManagedExpression child1, ManagedExpression child2) {
    std::vector<OwnedExpression> children;
    children.emplace_back(child1->Copy());
    children.emplace_back(child2->Copy());
    return MakeManaged(std::make_unique<parser::ComparisonExpression>(comp_type, std::move(children)));
  }

  /**
   *  expression for child1 == child2
   */
  ManagedExpression ComparisonEq(ManagedExpression child1, ManagedExpression child2) {
    return Comparison(parser::ExpressionType::COMPARE_EQUAL, child1, child2);
  }

  /**
   * Create expression for child1 == child2
   */
  ManagedExpression ComparisonNeq(ManagedExpression child1, ManagedExpression child2) {
    return Comparison(parser::ExpressionType::COMPARE_NOT_EQUAL, child1, child2);
  }

  /**
   * Create expression for child1 < child2
   */
  ManagedExpression ComparisonLt(ManagedExpression child1, ManagedExpression child2) {
    return Comparison(parser::ExpressionType::COMPARE_LESS_THAN, child1, child2);
  }

  /**
   * Create expression for child1 <= child2
   */
  ManagedExpression ComparisonLe(ManagedExpression child1, ManagedExpression child2) {
    return Comparison(parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO, child1, child2);
  }

  /**
   * Create expression for child1 > child2
   */
  ManagedExpression ComparisonGt(ManagedExpression child1, ManagedExpression child2) {
    return Comparison(parser::ExpressionType::COMPARE_GREATER_THAN, child1, child2);
  }

  /**
   * Create expression for child1 >= child2
   */
  ManagedExpression ComparisonGe(ManagedExpression child1, ManagedExpression child2) {
    return Comparison(parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO, child1, child2);
  }

  /**
   * Create expression for child1 <= input <= child2
   */
  ManagedExpression ComparisonBetween(ManagedExpression input, ManagedExpression child1, ManagedExpression child2) {
    return ConjunctionAnd(ConjunctionOr(ComparisonGt(input, child1), ComparisonEq(input, child1)),
                          ConjunctionOr(ComparisonLt(input, child2), ComparisonEq(input, child2)));
  }

  /**
   * Create expression for child1 Like child2
   */
  ManagedExpression ComparisonLike(ManagedExpression child1, ManagedExpression child2) {
    return Comparison(parser::ExpressionType::COMPARE_LIKE, child1, child2);
  }

  /**
   * Create expression for child1 Not Like child2
   */
  ManagedExpression ComparisonNotLike(ManagedExpression child1, ManagedExpression child2) {
    return Comparison(parser::ExpressionType::COMPARE_NOT_LIKE, child1, child2);
  }

  /**
   * Create a unary operation expression
   */
  ManagedExpression Operator(parser::ExpressionType op_type, type::TypeId ret_type, ManagedExpression child) {
    std::vector<OwnedExpression> children;
    children.emplace_back(child->Copy());
    return MakeManaged(std::make_unique<parser::OperatorExpression>(op_type, ret_type, std::move(children)));
  }

  /**
   * Create a binary operation expression
   */
  ManagedExpression Operator(parser::ExpressionType op_type, type::TypeId ret_type, ManagedExpression child1,
                             ManagedExpression child2) {
    std::vector<OwnedExpression> children;
    children.emplace_back(child1->Copy());
    children.emplace_back(child2->Copy());
    return MakeManaged(std::make_unique<parser::OperatorExpression>(op_type, ret_type, std::move(children)));
  }

  /**
   * create expression for child1 + child2
   */
  ManagedExpression OpSum(ManagedExpression child1, ManagedExpression child2) {
    return Operator(parser::ExpressionType::OPERATOR_PLUS, child1->GetReturnValueType(), child1, child2);
  }

  /**
   * create expression for child1 - child2
   */
  ManagedExpression OpMin(ManagedExpression child1, ManagedExpression child2) {
    return Operator(parser::ExpressionType::OPERATOR_MINUS, child1->GetReturnValueType(), child1, child2);
  }

  /**
   * create expression for child1 * child2
   */
  ManagedExpression OpMul(ManagedExpression child1, ManagedExpression child2) {
    return Operator(parser::ExpressionType::OPERATOR_MULTIPLY, child1->GetReturnValueType(), child1, child2);
  }

  /**
   * create expression for child1 / child2
   */
  ManagedExpression OpDiv(ManagedExpression child1, ManagedExpression child2) {
    return Operator(parser::ExpressionType::OPERATOR_DIVIDE, child1->GetReturnValueType(), child1, child2);
  }

  /**
   * create expression for -child
   */
  ManagedExpression OpNeg(ManagedExpression child) {
    return Operator(parser::ExpressionType::OPERATOR_UNARY_MINUS, child->GetReturnValueType(), child);
  }

  /**
   * Create expression for NOT(child)
   */
  ManagedExpression OpNot(ManagedExpression child) {
    return Operator(parser::ExpressionType::OPERATOR_NOT, type::TypeId::BOOLEAN, child);
  }

  /**
   * Create expression for child1 AND/OR child2
   */
  ManagedExpression Conjunction(parser::ExpressionType op_type, ManagedExpression child1, ManagedExpression child2) {
    std::vector<OwnedExpression> children;
    children.emplace_back(child1->Copy());
    children.emplace_back(child2->Copy());
    return MakeManaged(std::make_unique<parser::ConjunctionExpression>(op_type, std::move(children)));
  }

  /**
   * Create expression for child1 AND child2
   */
  ManagedExpression ConjunctionAnd(ManagedExpression child1, ManagedExpression child2) {
    return Conjunction(parser::ExpressionType::CONJUNCTION_AND, child1, child2);
  }

  /**
   * Create expression for child1 OR child2
   */
  ManagedExpression ConjunctionOr(ManagedExpression child1, ManagedExpression child2) {
    return Conjunction(parser::ExpressionType::CONJUNCTION_OR, child1, child2);
  }

  /**
   * Create an aggregate expression
   */
  ManagedAggExpression AggregateTerm(parser::ExpressionType agg_type, ManagedExpression child, bool distinct) {
    std::vector<OwnedExpression> children;
    children.emplace_back(child->Copy());

    auto agg = MakeAggManaged(std::make_unique<parser::AggregateExpression>(agg_type, std::move(children), distinct));
    agg->DeriveReturnValueType();
    return agg;
  }

  /**
   * Create a sum aggregate expression
   */
  ManagedAggExpression AggSum(ManagedExpression child, bool distinct = false) {
    return AggregateTerm(parser::ExpressionType::AGGREGATE_SUM, child, distinct);
  }

  /**
   * Create a avg aggregate expression
   */
  ManagedAggExpression AggAvg(ManagedExpression child, bool distinct = false) {
    return AggregateTerm(parser::ExpressionType::AGGREGATE_AVG, child, distinct);
  }

  /**
   * Create a count aggregate expression
   */
  ManagedAggExpression AggCount(ManagedExpression child, bool distinct = false) {
    return AggregateTerm(parser::ExpressionType::AGGREGATE_COUNT, child, distinct);
  }

 private:
  // To ease memory management
  std::vector<OwnedExpression> owned_exprs_;
  std::vector<OwnedAggExpression> owned_agg_exprs_;
};
}  // namespace noisepage::execution::compiler::test
