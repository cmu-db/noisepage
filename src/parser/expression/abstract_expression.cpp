#include <memory>
#include <vector>

#include "parser/expression/abstract_expression.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/case_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/parameter_value_expression.h"
#include "parser/expression/star_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/expression/tuple_value_expression.h"
#include "parser/expression/type_cast_expression.h"

namespace terrier::parser {

/**
 * Derived expressions should call this base method
 * @return expression serialized to json
 */
nlohmann::json AbstractExpression::ToJson() const {
  nlohmann::json j;
  j["expression_type"] = expression_type_;
  j["return_value_type"] = return_value_type_;
  j["children"] = children_;
  return j;
}

/**
 * Derived expressions should call this base method
 * @param j json to deserialize
 */
void AbstractExpression::FromJson(const nlohmann::json &j) {
  expression_type_ = j.at("expression_type").get<ExpressionType>();
  return_value_type_ = j.at("return_value_type").get<type::TypeId>();
  children_ = {};

  // Deserialize children
  auto children_json = j.at("children").get<std::vector<nlohmann::json>>();
  for (const auto &child_json : children_json) {
    children_.push_back(DeserializeExpression(child_json));
  }
}

std::shared_ptr<AbstractExpression> DeserializeExpression(const nlohmann::json &j) {
  std::shared_ptr<AbstractExpression> expr;

  auto expression_type = j.at("expression_type").get<ExpressionType>();
  switch (expression_type) {
    case ExpressionType::AGGREGATE_COUNT:
    case ExpressionType::AGGREGATE_COUNT_STAR:
    case ExpressionType::AGGREGATE_SUM:
    case ExpressionType::AGGREGATE_MIN:
    case ExpressionType::AGGREGATE_MAX:
    case ExpressionType::AGGREGATE_AVG: {
      expr = std::make_shared<AggregateExpression>();
      break;
    }

    case ExpressionType::OPERATOR_CASE_EXPR: {
      expr = std::make_shared<CaseExpression>();
      break;
    }

    case ExpressionType::COMPARE_EQUAL:
    case ExpressionType::COMPARE_NOT_EQUAL:
    case ExpressionType::COMPARE_LESS_THAN:
    case ExpressionType::COMPARE_GREATER_THAN:
    case ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
    case ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
    case ExpressionType::COMPARE_LIKE:
    case ExpressionType::COMPARE_NOT_LIKE:
    case ExpressionType::COMPARE_IN:
    case ExpressionType::COMPARE_IS_DISTINCT_FROM: {
      expr = std::make_shared<ComparisonExpression>();
      break;
    }

    case ExpressionType::CONJUNCTION_AND:
    case ExpressionType::CONJUNCTION_OR: {
      expr = std::make_shared<ConjunctionExpression>();
      break;
    }

    case ExpressionType::VALUE_CONSTANT: {
      expr = std::make_shared<ConstantValueExpression>();
      break;
    }

    case ExpressionType::FUNCTION: {
      expr = std::make_shared<FunctionExpression>();
      break;
    }

    case ExpressionType::OPERATOR_UNARY_MINUS:
    case ExpressionType::OPERATOR_PLUS:
    case ExpressionType::OPERATOR_MINUS:
    case ExpressionType::OPERATOR_MULTIPLY:
    case ExpressionType::OPERATOR_DIVIDE:
    case ExpressionType::OPERATOR_CONCAT:
    case ExpressionType::OPERATOR_MOD:
    case ExpressionType::OPERATOR_NOT:
    case ExpressionType::OPERATOR_IS_NULL:
    case ExpressionType::OPERATOR_IS_NOT_NULL:
    case ExpressionType::OPERATOR_EXISTS: {
      expr = std::make_shared<OperatorExpression>();
      break;
    }

    case ExpressionType::VALUE_PARAMETER: {
      expr = std::make_shared<ParameterValueExpression>();
      break;
    }

    case ExpressionType::STAR: {
      expr = std::make_shared<StarExpression>();
      break;
    }

    case ExpressionType::ROW_SUBQUERY: {
      expr = std::make_shared<SubqueryExpression>();
      break;
    }

    case ExpressionType::VALUE_TUPLE: {
      expr = std::make_shared<TupleValueExpression>();
      break;
    }

    case ExpressionType::OPERATOR_CAST: {
      expr = std::make_shared<TypeCastExpression>();
      break;
    }

    default:
      throw std::runtime_error("Unknown expression type during deserialization");
  }
  expr->FromJson(j);
  return expr;
}

}  // namespace terrier::parser
