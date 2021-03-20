#include "parser/expression_defs.h"

#include <string>

#include "common/error/exception.h"
#include "gtest/gtest.h"

namespace noisepage::parser {

// NOLINTNEXTLINE
TEST(ExpressionDefsTests, ExpressionTypeToStringTest) {
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::INVALID), "INVALID");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::INVALID), "INVALID");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::OPERATOR_PLUS), "+");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_PLUS), "OPERATOR_PLUS");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::OPERATOR_MINUS), "-");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_MINUS), "OPERATOR_MINUS");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::OPERATOR_MULTIPLY), "*");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_MULTIPLY), "OPERATOR_MULTIPLY");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::OPERATOR_DIVIDE), "/");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_DIVIDE), "OPERATOR_DIVIDE");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::OPERATOR_CONCAT), "OPERATOR_CONCAT");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_CONCAT), "OPERATOR_CONCAT");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::OPERATOR_MOD), "OPERATOR_MOD");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_MOD), "OPERATOR_MOD");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::OPERATOR_CAST), "OPERATOR_CAST");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_CAST), "OPERATOR_CAST");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::OPERATOR_NOT), "OPERATOR_NOT");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_NOT), "OPERATOR_NOT");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::OPERATOR_IS_NULL), "OPERATOR_IS_NULL");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_IS_NULL), "OPERATOR_IS_NULL");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::OPERATOR_IS_NOT_NULL), "OPERATOR_IS_NOT_NULL");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_IS_NOT_NULL), "OPERATOR_IS_NOT_NULL");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::OPERATOR_EXISTS), "OPERATOR_EXISTS");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_EXISTS), "OPERATOR_EXISTS");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::OPERATOR_UNARY_MINUS), "OPERATOR_UNARY_MINUS");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_UNARY_MINUS), "OPERATOR_UNARY_MINUS");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::COMPARE_EQUAL), "=");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_EQUAL), "COMPARE_EQUAL");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::COMPARE_NOT_EQUAL), "!=");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_NOT_EQUAL), "COMPARE_NOT_EQUAL");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::COMPARE_LESS_THAN), "<");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_LESS_THAN), "COMPARE_LESS_THAN");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::COMPARE_GREATER_THAN), ">");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_GREATER_THAN), "COMPARE_GREATER_THAN");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO), "<=");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO), "COMPARE_LESS_THAN_OR_EQUAL_TO");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO), ">=");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO),
            "COMPARE_GREATER_THAN_OR_EQUAL_TO");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::COMPARE_LIKE), "~~");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_LIKE), "COMPARE_LIKE");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::COMPARE_NOT_LIKE), "!~~");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_NOT_LIKE), "COMPARE_NOT_LIKE");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::COMPARE_IN), "IN");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_IN), "COMPARE_IN");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::COMPARE_IS_DISTINCT_FROM), "IS_DISTINCT_FROM");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_IS_DISTINCT_FROM), "COMPARE_IS_DISTINCT_FROM");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::CONJUNCTION_AND), "AND");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::CONJUNCTION_AND), "CONJUNCTION_AND");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::CONJUNCTION_OR), "OR");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::CONJUNCTION_OR), "CONJUNCTION_OR");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::VALUE_CONSTANT), "VALUE_CONSTANT");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_CONSTANT), "VALUE_CONSTANT");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::VALUE_PARAMETER), "VALUE_PARAMETER");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_PARAMETER), "VALUE_PARAMETER");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::VALUE_TUPLE), "VALUE_TUPLE");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_TUPLE), "VALUE_TUPLE");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::COLUMN_VALUE), "COLUMN_VALUE");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COLUMN_VALUE), "COLUMN_VALUE");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::VALUE_TUPLE_ADDRESS), "VALUE_TUPLE_ADDRESS");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_TUPLE_ADDRESS), "VALUE_TUPLE_ADDRESS");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::VALUE_NULL), "VALUE_NULL");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_NULL), "VALUE_NULL");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::VALUE_VECTOR), "VALUE_VECTOR");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_VECTOR), "VALUE_VECTOR");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::VALUE_SCALAR), "VALUE_SCALAR");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_SCALAR), "VALUE_SCALAR");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::AGGREGATE_COUNT), "COUNT");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_COUNT), "AGGREGATE_COUNT");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::AGGREGATE_SUM), "SUM");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_SUM), "AGGREGATE_SUM");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::AGGREGATE_MIN), "MIN");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_MIN), "AGGREGATE_MIN");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::AGGREGATE_MAX), "MAX");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_MAX), "AGGREGATE_MAX");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::AGGREGATE_AVG), "AVG");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_AVG), "AGGREGATE_AVG");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::AGGREGATE_TOP_K), "TOP_K");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_TOP_K), "AGGREGATE_TOP_K");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::AGGREGATE_HISTOGRAM), "HISTOGRAM");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_HISTOGRAM), "AGGREGATE_HISTOGRAM");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::FUNCTION), "FUNCTION");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::FUNCTION), "FUNCTION");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::HASH_RANGE), "HASH_RANGE");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::HASH_RANGE), "HASH_RANGE");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::OPERATOR_CASE_EXPR), "OPERATOR_CASE_EXPR");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_CASE_EXPR), "OPERATOR_CASE_EXPR");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::OPERATOR_NULL_IF), "OPERATOR_NULL_IF");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_NULL_IF), "OPERATOR_NULL_IF");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::OPERATOR_COALESCE), "OPERATOR_COALESCE");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_COALESCE), "OPERATOR_COALESCE");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::ROW_SUBQUERY), "ROW_SUBQUERY");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::ROW_SUBQUERY), "ROW_SUBQUERY");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::STAR), "STAR");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::STAR), "STAR");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::TABLE_STAR), "TABLE_STAR");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::TABLE_STAR), "TABLE_STAR");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::PLACEHOLDER), "PLACEHOLDER");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::PLACEHOLDER), "PLACEHOLDER");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::COLUMN_REF), "COLUMN_REF");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COLUMN_REF), "COLUMN_REF");
  EXPECT_EQ(ExpressionTypeToShortString(ExpressionType::FUNCTION_REF), "FUNCTION_REF");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::FUNCTION_REF), "FUNCTION_REF");
  EXPECT_THROW(ExpressionTypeToShortString(ExpressionType(100)), ConversionException);
}

}  // namespace noisepage::parser
