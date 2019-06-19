#pragma once

#include <string>
#include "common/exception.h"
#include "common/macros.h"

namespace terrier::parser {

/**
 * All possible expression types.
 * TODO(WAN): Do we really need some of this stuff? e.g. HASH_RANGE for elastic
 */
enum class ExpressionType : uint8_t {
  INVALID,

  OPERATOR_UNARY_MINUS,
  OPERATOR_PLUS,
  OPERATOR_MINUS,
  OPERATOR_MULTIPLY,
  OPERATOR_DIVIDE,
  OPERATOR_CONCAT,
  OPERATOR_MOD,
  OPERATOR_CAST,
  OPERATOR_NOT,
  OPERATOR_IS_NULL,
  OPERATOR_IS_NOT_NULL,
  OPERATOR_EXISTS,

  COMPARE_EQUAL,
  COMPARE_NOT_EQUAL,
  COMPARE_LESS_THAN,
  COMPARE_GREATER_THAN,
  COMPARE_LESS_THAN_OR_EQUAL_TO,
  COMPARE_GREATER_THAN_OR_EQUAL_TO,
  COMPARE_LIKE,
  COMPARE_NOT_LIKE,
  COMPARE_IN,
  COMPARE_IS_DISTINCT_FROM,

  CONJUNCTION_AND,
  CONJUNCTION_OR,

  VALUE_CONSTANT,
  VALUE_PARAMETER,
  VALUE_TUPLE,
  VALUE_TUPLE_ADDRESS,
  VALUE_NULL,
  VALUE_VECTOR,
  VALUE_SCALAR,
  VALUE_DEFAULT,

  AGGREGATE_COUNT,
  AGGREGATE_COUNT_STAR,
  AGGREGATE_SUM,
  AGGREGATE_MIN,
  AGGREGATE_MAX,
  AGGREGATE_AVG,

  FUNCTION,

  HASH_RANGE,

  OPERATOR_CASE_EXPR,
  OPERATOR_NULL_IF,
  OPERATOR_COALESCE,

  ROW_SUBQUERY,
  SELECT_SUBQUERY,

  STAR,
  PLACEHOLDER,
  COLUMN_REF,
  FUNCTION_REF,
  TABLE_REF
};

// When short_str is true, return a short version of ExpressionType string
// For example, + instead of Operator_Plus. It's used to generate the
// expression name
std::string ExpressionTypeToString(ExpressionType type, bool short_str = false) {
  switch (type) {
    case ExpressionType::INVALID: {
      return ("INVALID");
    }
    case ExpressionType::OPERATOR_PLUS: {
      return short_str ? "+" : ("OPERATOR_PLUS");
    }
    case ExpressionType::OPERATOR_MINUS: {
      return short_str ? "-" : ("OPERATOR_MINUS");
    }
    case ExpressionType::OPERATOR_MULTIPLY: {
      return short_str ? "*" : ("OPERATOR_MULTIPLY");
    }
    case ExpressionType::OPERATOR_DIVIDE: {
      return short_str ? "/" : ("OPERATOR_DIVIDE");
    }
    case ExpressionType::OPERATOR_CONCAT: {
      return ("OPERATOR_CONCAT");
    }
    case ExpressionType::OPERATOR_MOD: {
      return ("OPERATOR_MOD");
    }
    case ExpressionType::OPERATOR_CAST: {
      return ("OPERATOR_CAST");
    }
    case ExpressionType::OPERATOR_NOT: {
      return ("OPERATOR_NOT");
    }
    case ExpressionType::OPERATOR_IS_NULL: {
      return ("OPERATOR_IS_NULL");
    }
    case ExpressionType::OPERATOR_EXISTS: {
      return ("OPERATOR_EXISTS");
    }
    case ExpressionType::OPERATOR_UNARY_MINUS: {
      return ("OPERATOR_UNARY_MINUS");
    }
    case ExpressionType::COMPARE_EQUAL: {
      return short_str ? "=" : ("COMPARE_EQUAL");
    }
    case ExpressionType::COMPARE_NOT_EQUAL: {
      return short_str ? "!=" : ("COMPARE_NOT_EQUAL");
    }
    case ExpressionType::COMPARE_LESS_THAN: {
      return short_str ? "<" : ("COMPARE_LESS_THAN");
    }
    case ExpressionType::COMPARE_GREATER_THAN: {
      return short_str ? ">" : ("COMPARE_GREATER_THAN");
    }
    case ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO: {
      return short_str ? "<=" : ("COMPARE_LESS_THAN_OR_EQUAL_TO");
    }
    case ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO: {
      return short_str ? ">=" : ("COMPARE_GREATER_THAN_OR_EQUAL_TO");
    }
    case ExpressionType::COMPARE_LIKE: {
      return short_str ? "~~" : ("COMPARE_LIKE");
    }
    case ExpressionType::COMPARE_NOT_LIKE: {
      return short_str ? "!~~" : ("COMPARE_NOT_LIKE");
    }
    case ExpressionType::COMPARE_IN: {
      return ("COMPARE_IN");
    }
    case ExpressionType::COMPARE_IS_DISTINCT_FROM: {
      return ("COMPARE_IS_DISTINCT_FROM");
    }
    case ExpressionType::CONJUNCTION_AND: {
      return ("CONJUNCTION_AND");
    }
    case ExpressionType::CONJUNCTION_OR: {
      return ("CONJUNCTION_OR");
    }
    case ExpressionType::VALUE_CONSTANT: {
      return ("VALUE_CONSTANT");
    }
    case ExpressionType::VALUE_PARAMETER: {
      return ("VALUE_PARAMETER");
    }
    case ExpressionType::VALUE_TUPLE: {
      return ("VALUE_TUPLE");
    }
    case ExpressionType::VALUE_TUPLE_ADDRESS: {
      return ("VALUE_TUPLE_ADDRESS");
    }
    case ExpressionType::VALUE_NULL: {
      return ("VALUE_NULL");
    }
    case ExpressionType::VALUE_VECTOR: {
      return ("VALUE_VECTOR");
    }
    case ExpressionType::VALUE_SCALAR: {
      return ("VALUE_SCALAR");
    }
    case ExpressionType::AGGREGATE_COUNT: {
      return ("AGGREGATE_COUNT");
    }
    case ExpressionType::AGGREGATE_COUNT_STAR: {
      return ("AGGREGATE_COUNT_STAR");
    }
    case ExpressionType::AGGREGATE_SUM: {
      return ("AGGREGATE_SUM");
    }
    case ExpressionType::AGGREGATE_MIN: {
      return ("AGGREGATE_MIN");
    }
    case ExpressionType::AGGREGATE_MAX: {
      return ("AGGREGATE_MAX");
    }
    case ExpressionType::AGGREGATE_AVG: {
      return ("AGGREGATE_AVG");
    }
    case ExpressionType::FUNCTION: {
      return ("FUNCTION");
    }
    case ExpressionType::HASH_RANGE: {
      return ("HASH_RANGE");
    }
    case ExpressionType::OPERATOR_CASE_EXPR: {
      return ("OPERATOR_CASE_EXPR");
    }
    case ExpressionType::OPERATOR_NULL_IF: {
      return ("OPERATOR_NULL_IF");
    }
    case ExpressionType::OPERATOR_COALESCE: {
      return ("OPERATOR_COALESCE");
    }
    case ExpressionType::ROW_SUBQUERY: {
      return ("ROW_SUBQUERY");
    }
    case ExpressionType::SELECT_SUBQUERY: {
      return ("SELECT_SUBQUERY");
    }
    case ExpressionType::STAR: {
      return ("STAR");
    }
    case ExpressionType::PLACEHOLDER: {
      return ("PLACEHOLDER");
    }
    case ExpressionType::COLUMN_REF: {
      return ("COLUMN_REF");
    }
    case ExpressionType::FUNCTION_REF: {
      return ("FUNCTION_REF");
    }
    case ExpressionType::OPERATOR_IS_NOT_NULL: {
      return ("IS_NOT_NULL");
    }
    default: {
      throw CONVERSION_EXCEPTION(
          ("No string conversion for ExpressionType value " + std::to_string(static_cast<int>(type))).c_str());
    }
  }
}

/**
 * @param type the expression type to be examined
 * @return whether this expression type is one of the aggregate expression types
 */
bool IsAggregateExpression(ExpressionType type) {
  switch (type) {
    case ExpressionType::AGGREGATE_COUNT:
    case ExpressionType::AGGREGATE_COUNT_STAR:
    case ExpressionType::AGGREGATE_SUM:
    case ExpressionType::AGGREGATE_MIN:
    case ExpressionType::AGGREGATE_MAX:
    case ExpressionType::AGGREGATE_AVG:
      return true;
    default:
      return false;
  }
}

/**
 * @param type the expression type to be examined
 * @return whether this expression type is one of the operator expression types
 */
bool IsOperatorExpression(ExpressionType type) {
  switch (type) {
    case ExpressionType::OPERATOR_PLUS:
    case ExpressionType::OPERATOR_MINUS:
    case ExpressionType::OPERATOR_MULTIPLY:
    case ExpressionType::OPERATOR_DIVIDE:
    case ExpressionType::OPERATOR_CONCAT:
    case ExpressionType::OPERATOR_MOD:
    case ExpressionType::OPERATOR_CAST:
    case ExpressionType::OPERATOR_NOT:
    case ExpressionType::OPERATOR_IS_NULL:
    case ExpressionType::OPERATOR_IS_NOT_NULL:
    case ExpressionType::OPERATOR_EXISTS:
    case ExpressionType::OPERATOR_UNARY_MINUS:
      return true;
    default:
      return false;
  }
}
}  // namespace terrier::parser
