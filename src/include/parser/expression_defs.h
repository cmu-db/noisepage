#pragma once

#include <string>

#include "common/enum_defs.h"

namespace noisepage::parser {

#define EXPRESSION_TYPE_ENUM(T)                       \
  T(ExpressionType, INVALID)                          \
                                                      \
  T(ExpressionType, OPERATOR_UNARY_MINUS)             \
  T(ExpressionType, OPERATOR_PLUS)                    \
  T(ExpressionType, OPERATOR_MINUS)                   \
  T(ExpressionType, OPERATOR_MULTIPLY)                \
  T(ExpressionType, OPERATOR_DIVIDE)                  \
  T(ExpressionType, OPERATOR_CONCAT)                  \
  T(ExpressionType, OPERATOR_MOD)                     \
  T(ExpressionType, OPERATOR_CAST)                    \
  T(ExpressionType, OPERATOR_NOT)                     \
  T(ExpressionType, OPERATOR_IS_NULL)                 \
  T(ExpressionType, OPERATOR_IS_NOT_NULL)             \
  T(ExpressionType, OPERATOR_EXISTS)                  \
                                                      \
  T(ExpressionType, COMPARE_EQUAL)                    \
  T(ExpressionType, COMPARE_NOT_EQUAL)                \
  T(ExpressionType, COMPARE_LESS_THAN)                \
  T(ExpressionType, COMPARE_GREATER_THAN)             \
  T(ExpressionType, COMPARE_LESS_THAN_OR_EQUAL_TO)    \
  T(ExpressionType, COMPARE_GREATER_THAN_OR_EQUAL_TO) \
  T(ExpressionType, COMPARE_LIKE)                     \
  T(ExpressionType, COMPARE_NOT_LIKE)                 \
  T(ExpressionType, COMPARE_IN)                       \
  T(ExpressionType, COMPARE_IS_DISTINCT_FROM)         \
                                                      \
  T(ExpressionType, CONJUNCTION_AND)                  \
  T(ExpressionType, CONJUNCTION_OR)                   \
                                                      \
  T(ExpressionType, COLUMN_VALUE)                     \
                                                      \
  T(ExpressionType, VALUE_CONSTANT)                   \
  T(ExpressionType, VALUE_PARAMETER)                  \
  T(ExpressionType, VALUE_TUPLE)                      \
  T(ExpressionType, VALUE_TUPLE_ADDRESS)              \
  T(ExpressionType, VALUE_NULL)                       \
  T(ExpressionType, VALUE_VECTOR)                     \
  T(ExpressionType, VALUE_SCALAR)                     \
  T(ExpressionType, VALUE_DEFAULT)                    \
                                                      \
  T(ExpressionType, AGGREGATE_COUNT)                  \
  T(ExpressionType, AGGREGATE_SUM)                    \
  T(ExpressionType, AGGREGATE_MIN)                    \
  T(ExpressionType, AGGREGATE_MAX)                    \
  T(ExpressionType, AGGREGATE_AVG)                    \
  T(ExpressionType, AGGREGATE_TOP_K)                  \
  T(ExpressionType, AGGREGATE_HISTOGRAM)              \
                                                      \
  T(ExpressionType, FUNCTION)                         \
                                                      \
  T(ExpressionType, HASH_RANGE)                       \
                                                      \
  T(ExpressionType, OPERATOR_CASE_EXPR)               \
  T(ExpressionType, OPERATOR_NULL_IF)                 \
  T(ExpressionType, OPERATOR_COALESCE)                \
                                                      \
  T(ExpressionType, ROW_SUBQUERY)                     \
                                                      \
  T(ExpressionType, STAR)                             \
  T(ExpressionType, TABLE_STAR)                       \
  T(ExpressionType, PLACEHOLDER)                      \
  T(ExpressionType, COLUMN_REF)                       \
  T(ExpressionType, FUNCTION_REF)                     \
  T(ExpressionType, TABLE_REF)

/**
 * All possible expression types.
 * TODO(WAN): Do we really need some of this stuff? e.g. HASH_RANGE for elastic
 * TODO(LING): The binder via OperatorExpression now assumes a certain ordering on the ExpressionType.
 */
ENUM_DEFINE(ExpressionType, uint8_t, EXPRESSION_TYPE_ENUM);
#undef EXPRESSION_TYPE_ENUM

/** @return A short representation of @p type, e.g., "+" over "OPERATOR_PLUS". Used for expression name generation. */
std::string ExpressionTypeToShortString(ExpressionType type);

}  // namespace noisepage::parser
