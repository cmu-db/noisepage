#pragma once

#include <string>

#include "common/macros.h"

namespace noisepage::parser {

/**
 * All possible expression types.
 * TODO(WAN): Do we really need some of this stuff? e.g. HASH_RANGE for elastic
 * TODO(LING): The binder via OperatorExpression now assumes a certain ordering on the ExpressionType.
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

  COLUMN_VALUE,

  VALUE_CONSTANT,
  VALUE_PARAMETER,
  VALUE_TUPLE,
  VALUE_TUPLE_ADDRESS,
  VALUE_NULL,
  VALUE_VECTOR,
  VALUE_SCALAR,
  VALUE_DEFAULT,

  AGGREGATE_COUNT,
  AGGREGATE_SUM,
  AGGREGATE_MIN,
  AGGREGATE_MAX,
  AGGREGATE_AVG,
  AGGREGATE_TOP_K,
  AGGREGATE_HISTOGRAM,

  FUNCTION,

  HASH_RANGE,

  OPERATOR_CASE_EXPR,
  OPERATOR_NULL_IF,
  OPERATOR_COALESCE,

  ROW_SUBQUERY,

  STAR,
  TABLE_STAR,
  PLACEHOLDER,
  COLUMN_REF,
  FUNCTION_REF,
  TABLE_REF
};

/**
 * When short_str is true, return a short version of ExpressionType string
 * For example, + instead of Operator_Plus. It's used to generate the expression name
 * @param type Expression Type
 * @param short_str Flag if a short version of the Expression Type should be returned
 * @return String representation of the Expression Type
 */
std::string ExpressionTypeToString(ExpressionType type, bool short_str);

}  // namespace noisepage::parser
