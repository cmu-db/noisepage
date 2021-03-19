#include "parser/expression_defs.h"

#include <string>

namespace noisepage::parser {

std::string ExpressionTypeToShortString(ExpressionType type) {
  switch (type) {
      // clang-format off
    case ExpressionType::OPERATOR_PLUS:                     return "+";
    case ExpressionType::OPERATOR_MINUS:                    return "-";
    case ExpressionType::OPERATOR_MULTIPLY:                 return "*";
    case ExpressionType::OPERATOR_DIVIDE:                   return "/";
    case ExpressionType::COMPARE_EQUAL:                     return "=";
    case ExpressionType::COMPARE_NOT_EQUAL:                 return "!=";
    case ExpressionType::COMPARE_LESS_THAN:                 return "<";
    case ExpressionType::COMPARE_GREATER_THAN:              return ">";
    case ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:     return "<=";
    case ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:  return ">=";
    case ExpressionType::COMPARE_LIKE:                      return "~~";
    case ExpressionType::COMPARE_NOT_LIKE:                  return "!~~";
    case ExpressionType::COMPARE_IN:                        return "IN";
    case ExpressionType::COMPARE_IS_DISTINCT_FROM:          return "IS_DISTINCT_FROM";
    case ExpressionType::CONJUNCTION_AND:                   return "AND";
    case ExpressionType::CONJUNCTION_OR:                    return "OR";
    case ExpressionType::AGGREGATE_COUNT:                   return "COUNT";
    case ExpressionType::AGGREGATE_SUM:                     return "SUM";
    case ExpressionType::AGGREGATE_MIN:                     return "MIN";
    case ExpressionType::AGGREGATE_MAX:                     return "MAX";
    case ExpressionType::AGGREGATE_AVG:                     return "AVG";
    case ExpressionType::AGGREGATE_TOP_K:                   return "TOP_K";
    case ExpressionType::AGGREGATE_HISTOGRAM:               return "HISTOGRAM";
    default: return ExpressionTypeToString(type);
      // clang-format on
  }
}

}  // namespace noisepage::parser
