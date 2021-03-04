#include "parser/expression_defs.h"

#include <string>

#include "common/error/exception.h"

namespace noisepage::parser {

std::string ExpressionTypeToString(ExpressionType type, bool short_str) {
  switch (type) {
    case ExpressionType::INVALID: {
      return "INVALID";
    }
    case ExpressionType::OPERATOR_PLUS: {
      return short_str ? "+" : "OPERATOR_PLUS";
    }
    case ExpressionType::OPERATOR_MINUS: {
      return short_str ? "-" : "OPERATOR_MINUS";
    }
    case ExpressionType::OPERATOR_MULTIPLY: {
      return short_str ? "*" : "OPERATOR_MULTIPLY";
    }
    case ExpressionType::OPERATOR_DIVIDE: {
      return short_str ? "/" : "OPERATOR_DIVIDE";
    }
    case ExpressionType::OPERATOR_CONCAT: {
      return "OPERATOR_CONCAT";
    }
    case ExpressionType::OPERATOR_MOD: {
      return "OPERATOR_MOD";
    }
    case ExpressionType::OPERATOR_CAST: {
      return "OPERATOR_CAST";
    }
    case ExpressionType::OPERATOR_NOT: {
      return "OPERATOR_NOT";
    }
    case ExpressionType::OPERATOR_IS_NULL: {
      return "OPERATOR_IS_NULL";
    }
    case ExpressionType::OPERATOR_IS_NOT_NULL: {
      return "OPERATOR_IS_NOT_NULL";
    }
    case ExpressionType::OPERATOR_EXISTS: {
      return "OPERATOR_EXISTS";
    }
    case ExpressionType::OPERATOR_UNARY_MINUS: {
      return "OPERATOR_UNARY_MINUS";
    }
    case ExpressionType::COMPARE_EQUAL: {
      return short_str ? "=" : "COMPARE_EQUAL";
    }
    case ExpressionType::COMPARE_NOT_EQUAL: {
      return short_str ? "!=" : "COMPARE_NOT_EQUAL";
    }
    case ExpressionType::COMPARE_LESS_THAN: {
      return short_str ? "<" : "COMPARE_LESS_THAN";
    }
    case ExpressionType::COMPARE_GREATER_THAN: {
      return short_str ? ">" : "COMPARE_GREATER_THAN";
    }
    case ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO: {
      return short_str ? "<=" : "COMPARE_LESS_THAN_OR_EQUAL_TO";
    }
    case ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO: {
      return short_str ? ">=" : "COMPARE_GREATER_THAN_OR_EQUAL_TO";
    }
    case ExpressionType::COMPARE_LIKE: {
      return short_str ? "~~" : "COMPARE_LIKE";
    }
    case ExpressionType::COMPARE_NOT_LIKE: {
      return short_str ? "!~~" : "COMPARE_NOT_LIKE";
    }
    case ExpressionType::COMPARE_IN: {
      return short_str ? "IN" : "COMPARE_IN";
    }
    case ExpressionType::COMPARE_IS_DISTINCT_FROM: {
      return short_str ? "IS_DISTINCT_FROM" : "COMPARE_IS_DISTINCT_FROM";
    }
    case ExpressionType::CONJUNCTION_AND: {
      return short_str ? "AND" : "CONJUNCTION_AND";
    }
    case ExpressionType::CONJUNCTION_OR: {
      return short_str ? "OR" : "CONJUNCTION_OR";
    }
    case ExpressionType::VALUE_CONSTANT: {
      return "VALUE_CONSTANT";
    }
    case ExpressionType::VALUE_DEFAULT: {
      return "VALUE_DEFAULT";
    }
    case ExpressionType::VALUE_PARAMETER: {
      return "VALUE_PARAMETER";
    }
    case ExpressionType::VALUE_TUPLE: {
      return "VALUE_TUPLE";
    }
    case ExpressionType::COLUMN_VALUE: {
      return "COLUMN_VALUE";
    }
    case ExpressionType::VALUE_TUPLE_ADDRESS: {
      return "VALUE_TUPLE_ADDRESS";
    }
    case ExpressionType::VALUE_NULL: {
      return "VALUE_NULL";
    }
    case ExpressionType::VALUE_VECTOR: {
      return "VALUE_VECTOR";
    }
    case ExpressionType::VALUE_SCALAR: {
      return "VALUE_SCALAR";
    }
    case ExpressionType::AGGREGATE_COUNT: {
      return short_str ? "COUNT" : "AGGREGATE_COUNT";
    }
    case ExpressionType::AGGREGATE_SUM: {
      return short_str ? "SUM" : "AGGREGATE_SUM";
    }
    case ExpressionType::AGGREGATE_MIN: {
      return short_str ? "MIN" : "AGGREGATE_MIN";
    }
    case ExpressionType::AGGREGATE_MAX: {
      return short_str ? "MAX" : "AGGREGATE_MAX";
    }
    case ExpressionType::AGGREGATE_AVG: {
      return short_str ? "AVG" : "AGGREGATE_AVG";
    }
    case ExpressionType::AGGREGATE_TOP_K: {
      return short_str ? "TOP_K" : "AGGREGATE_TOP_K";
    }
    case ExpressionType::AGGREGATE_HISTOGRAM: {
      return short_str ? "HISTOGRAM" : "AGGREGATE_HISTOGRAM";
    }
    case ExpressionType::FUNCTION: {
      return "FUNCTION";
    }
    case ExpressionType::HASH_RANGE: {
      return "HASH_RANGE";
    }
    case ExpressionType::OPERATOR_CASE_EXPR: {
      return "OPERATOR_CASE_EXPR";
    }
    case ExpressionType::OPERATOR_NULL_IF: {
      return "OPERATOR_NULL_IF";
    }
    case ExpressionType::OPERATOR_COALESCE: {
      return "OPERATOR_COALESCE";
    }
    case ExpressionType::ROW_SUBQUERY: {
      return "ROW_SUBQUERY";
    }
    case ExpressionType::STAR: {
      return "STAR";
    }
    case ExpressionType::TABLE_STAR: {
      return "TABLE_STAR";
    }
    case ExpressionType::PLACEHOLDER: {
      return "PLACEHOLDER";
    }
    case ExpressionType::COLUMN_REF: {
      return "COLUMN_REF";
    }
    case ExpressionType::FUNCTION_REF: {
      return "FUNCTION_REF";
    }
    default: {
      throw CONVERSION_EXCEPTION(
          ("No string conversion for ExpressionType value " + std::to_string(static_cast<int>(type))).c_str());
    }
  }
}

}  // namespace noisepage::parser
