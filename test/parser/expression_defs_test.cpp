#include "parser/expression_defs.h"

#include <string>

#include "common/error/exception.h"
#include "gtest/gtest.h"

namespace noisepage::parser {

// NOLINTNEXTLINE
TEST(ExpressionDefsTests, ExpressionTypeToStringTest) {
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::INVALID, true), "INVALID");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::INVALID, false), "INVALID");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_PLUS, true), "+");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_PLUS, false), "OPERATOR_PLUS");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_MINUS, true), "-");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_MINUS, false), "OPERATOR_MINUS");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_MULTIPLY, true), "*");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_MULTIPLY, false), "OPERATOR_MULTIPLY");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_DIVIDE, true), "/");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_DIVIDE, false), "OPERATOR_DIVIDE");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_CONCAT, true), "OPERATOR_CONCAT");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_CONCAT, false), "OPERATOR_CONCAT");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_MOD, true), "OPERATOR_MOD");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_MOD, false), "OPERATOR_MOD");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_CAST, true), "OPERATOR_CAST");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_CAST, false), "OPERATOR_CAST");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_NOT, true), "OPERATOR_NOT");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_NOT, false), "OPERATOR_NOT");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_IS_NULL, true), "OPERATOR_IS_NULL");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_IS_NULL, false), "OPERATOR_IS_NULL");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_IS_NOT_NULL, true), "OPERATOR_IS_NOT_NULL");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_IS_NOT_NULL, false), "OPERATOR_IS_NOT_NULL");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_EXISTS, true), "OPERATOR_EXISTS");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_EXISTS, false), "OPERATOR_EXISTS");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_UNARY_MINUS, true), "OPERATOR_UNARY_MINUS");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_UNARY_MINUS, false), "OPERATOR_UNARY_MINUS");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_EQUAL, true), "=");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_EQUAL, false), "COMPARE_EQUAL");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_NOT_EQUAL, true), "!=");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_NOT_EQUAL, false), "COMPARE_NOT_EQUAL");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_LESS_THAN, true), "<");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_LESS_THAN, false), "COMPARE_LESS_THAN");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_GREATER_THAN, true), ">");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_GREATER_THAN, false), "COMPARE_GREATER_THAN");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO, true), "<=");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO, false),
            "COMPARE_LESS_THAN_OR_EQUAL_TO");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO, true), ">=");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO, false),
            "COMPARE_GREATER_THAN_OR_EQUAL_TO");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_LIKE, true), "~~");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_LIKE, false), "COMPARE_LIKE");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_NOT_LIKE, true), "!~~");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_NOT_LIKE, false), "COMPARE_NOT_LIKE");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_IN, true), "IN");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_IN, false), "COMPARE_IN");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_IS_DISTINCT_FROM, true), "IS_DISTINCT_FROM");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COMPARE_IS_DISTINCT_FROM, false), "COMPARE_IS_DISTINCT_FROM");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::CONJUNCTION_AND, true), "AND");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::CONJUNCTION_AND, false), "CONJUNCTION_AND");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::CONJUNCTION_OR, true), "OR");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::CONJUNCTION_OR, false), "CONJUNCTION_OR");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_CONSTANT, true), "VALUE_CONSTANT");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_CONSTANT, false), "VALUE_CONSTANT");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_PARAMETER, true), "VALUE_PARAMETER");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_PARAMETER, false), "VALUE_PARAMETER");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_TUPLE, true), "VALUE_TUPLE");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_TUPLE, false), "VALUE_TUPLE");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COLUMN_VALUE, true), "COLUMN_VALUE");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COLUMN_VALUE, false), "COLUMN_VALUE");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_TUPLE_ADDRESS, true), "VALUE_TUPLE_ADDRESS");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_TUPLE_ADDRESS, false), "VALUE_TUPLE_ADDRESS");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_NULL, true), "VALUE_NULL");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_NULL, false), "VALUE_NULL");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_VECTOR, true), "VALUE_VECTOR");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_VECTOR, false), "VALUE_VECTOR");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_SCALAR, true), "VALUE_SCALAR");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::VALUE_SCALAR, false), "VALUE_SCALAR");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_COUNT, true), "COUNT");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_COUNT, false), "AGGREGATE_COUNT");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_SUM, true), "SUM");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_SUM, false), "AGGREGATE_SUM");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_MIN, true), "MIN");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_MIN, false), "AGGREGATE_MIN");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_MAX, true), "MAX");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_MAX, false), "AGGREGATE_MAX");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_AVG, true), "AVG");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_AVG, false), "AGGREGATE_AVG");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_TOP_K, true), "TOP_K");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_TOP_K, false), "AGGREGATE_TOP_K");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_HISTOGRAM, true), "HISTOGRAM");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::AGGREGATE_HISTOGRAM, false), "AGGREGATE_HISTOGRAM");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::FUNCTION, true), "FUNCTION");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::FUNCTION, false), "FUNCTION");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::HASH_RANGE, true), "HASH_RANGE");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::HASH_RANGE, false), "HASH_RANGE");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_CASE_EXPR, true), "OPERATOR_CASE_EXPR");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_CASE_EXPR, false), "OPERATOR_CASE_EXPR");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_NULL_IF, true), "OPERATOR_NULL_IF");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_NULL_IF, false), "OPERATOR_NULL_IF");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_COALESCE, true), "OPERATOR_COALESCE");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::OPERATOR_COALESCE, false), "OPERATOR_COALESCE");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::ROW_SUBQUERY, true), "ROW_SUBQUERY");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::ROW_SUBQUERY, false), "ROW_SUBQUERY");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::STAR, true), "STAR");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::STAR, false), "STAR");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::TABLE_STAR, true), "TABLE_STAR");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::TABLE_STAR, false), "TABLE_STAR");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::PLACEHOLDER, true), "PLACEHOLDER");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::PLACEHOLDER, false), "PLACEHOLDER");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COLUMN_REF, true), "COLUMN_REF");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::COLUMN_REF, false), "COLUMN_REF");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::FUNCTION_REF, true), "FUNCTION_REF");
  EXPECT_EQ(ExpressionTypeToString(ExpressionType::FUNCTION_REF, false), "FUNCTION_REF");
  EXPECT_THROW(ExpressionTypeToString(ExpressionType(100), true), ConversionException);
}

}  // namespace noisepage::parser
