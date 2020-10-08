#pragma once

#include <queue>
#include <utility>
#include <vector>

#include "brain/brain_defs.h"
#include "brain/operating_unit.h"
#include "parser/expression/abstract_expression.h"

namespace terrier::brain {

/**
 * Utility class for OperatingUnits
 * Includes some conversion/utility code
 */
class OperatingUnitUtil {
 public:
  /**
   * Derive the type of computation
   * @param expr Expression
   * @returns type of computation
   */
  static type::TypeId DeriveComputation(common::ManagedPointer<parser::AbstractExpression> expr) {
    if (expr->GetChildrenSize() == 0) {
      // Not a computation
      return type::TypeId::INVALID;
    }

    auto lchild = expr->GetChild(0);
    if (lchild->GetReturnValueType() != type::TypeId::INVALID &&
        lchild->GetReturnValueType() != type::TypeId::PARAMETER_OFFSET) {
      return lchild->GetReturnValueType();
    }

    if (expr->GetChildrenSize() > 1) {
      auto rchild = expr->GetChild(1);
      if (rchild->GetReturnValueType() != type::TypeId::INVALID &&
          rchild->GetReturnValueType() != type::TypeId::PARAMETER_OFFSET) {
        return rchild->GetReturnValueType();
      }
    }

    return type::TypeId::INVALID;
  }

  /**
   * Converts a expression to brain::ExecutionOperatingUnitType
   *
   * Function returns brain::ExecutionOperatingUnitType::INVALID if the
   * parser::ExpressionType does not have an equivalent conversion.
   *
   * @param expr Expression
   * @returns converted equivalent brain::ExecutionOperatingUnitType
   */
  static std::pair<type::TypeId, ExecutionOperatingUnitType> ConvertExpressionType(
      common::ManagedPointer<parser::AbstractExpression> expr) {
    auto type = DeriveComputation(expr);
    switch (expr->GetExpressionType()) {
      case parser::ExpressionType::AGGREGATE_COUNT:
        return std::make_pair(type, ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS);
      case parser::ExpressionType::AGGREGATE_SUM:
      case parser::ExpressionType::AGGREGATE_AVG:
      case parser::ExpressionType::OPERATOR_PLUS:
      case parser::ExpressionType::OPERATOR_MINUS: {
        switch (type) {
          case type::TypeId::TINYINT:
          case type::TypeId::SMALLINT:
          case type::TypeId::INTEGER:
          case type::TypeId::BIGINT:
            return std::make_pair(type, ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS);
          case type::TypeId::DECIMAL:
            return std::make_pair(type, ExecutionOperatingUnitType::OP_DECIMAL_PLUS_OR_MINUS);
          default:
            return std::make_pair(type, ExecutionOperatingUnitType::INVALID);
        }
      }
      case parser::ExpressionType::OPERATOR_MULTIPLY: {
        switch (type) {
          case type::TypeId::TINYINT:
          case type::TypeId::SMALLINT:
          case type::TypeId::INTEGER:
          case type::TypeId::BIGINT:
            return std::make_pair(type, ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY);
          case type::TypeId::DECIMAL:
            return std::make_pair(type, ExecutionOperatingUnitType::OP_DECIMAL_MULTIPLY);
          default:
            return std::make_pair(type, ExecutionOperatingUnitType::INVALID);
        }
      }
      case parser::ExpressionType::OPERATOR_DIVIDE: {
        switch (type) {
          case type::TypeId::TINYINT:
          case type::TypeId::SMALLINT:
          case type::TypeId::INTEGER:
          case type::TypeId::BIGINT:
            return std::make_pair(type, ExecutionOperatingUnitType::OP_INTEGER_DIVIDE);
          case type::TypeId::DECIMAL:
            return std::make_pair(type, ExecutionOperatingUnitType::OP_DECIMAL_DIVIDE);
          default:
            return std::make_pair(type, ExecutionOperatingUnitType::INVALID);
        }
      }
      case parser::ExpressionType::AGGREGATE_MAX:
      case parser::ExpressionType::AGGREGATE_MIN:
      case parser::ExpressionType::COMPARE_EQUAL:
      case parser::ExpressionType::COMPARE_NOT_EQUAL:
      case parser::ExpressionType::COMPARE_LESS_THAN:
      case parser::ExpressionType::COMPARE_GREATER_THAN:
      case parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
      case parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO: {
        switch (type) {
          case type::TypeId::BOOLEAN:
            return std::make_pair(type, ExecutionOperatingUnitType::OP_BOOL_COMPARE);
          case type::TypeId::TINYINT:
          case type::TypeId::SMALLINT:
          case type::TypeId::INTEGER:
          case type::TypeId::BIGINT:
            return std::make_pair(type, ExecutionOperatingUnitType::OP_INTEGER_COMPARE);
          case type::TypeId::DECIMAL:
            return std::make_pair(type, ExecutionOperatingUnitType::OP_DECIMAL_COMPARE);
          case type::TypeId::TIMESTAMP:
          case type::TypeId::DATE:
            return std::make_pair(type, ExecutionOperatingUnitType::OP_INTEGER_COMPARE);
          case type::TypeId::VARCHAR:
          case type::TypeId::VARBINARY:
            // TODO(wz2): Revisit this since VARCHAR/VARBINARY is more than just integer
            return std::make_pair(type, ExecutionOperatingUnitType::OP_INTEGER_COMPARE);
          default:
            return std::make_pair(type, ExecutionOperatingUnitType::INVALID);
        }
      }
      default:
        return std::make_pair(type, ExecutionOperatingUnitType::INVALID);
    }
  }

  /**
   * Extracts features from an expression into a vector
   * @param expr Expression to extract features from
   * @return vector of extracted features
   */
  static std::vector<std::pair<type::TypeId, ExecutionOperatingUnitType>> ExtractFeaturesFromExpression(
      common::ManagedPointer<parser::AbstractExpression> expr) {
    if (expr == nullptr) return std::vector<std::pair<type::TypeId, ExecutionOperatingUnitType>>();

    std::vector<std::pair<type::TypeId, ExecutionOperatingUnitType>> feature_types;
    std::queue<common::ManagedPointer<parser::AbstractExpression>> work;
    work.push(expr);

    while (!work.empty()) {
      auto head = work.front();
      work.pop();

      auto feature = ConvertExpressionType(head);
      if (feature.second != ExecutionOperatingUnitType::INVALID) {
        feature_types.push_back(feature);
      }

      for (auto child : head->GetChildren()) {
        work.push(child);
      }
    }

    return feature_types;
  }

  /**
   * Whether or not an operating unit type can be merged
   * @param feature OperatingUnitType to consider
   * @returns mergeable or not
   */
  static bool IsOperatingUnitTypeMergeable(ExecutionOperatingUnitType feature) {
    return feature > ExecutionOperatingUnitType::PLAN_OPS_DELIMITER;
  }

  /**
   * Determines whether the operating unit type is a blocking OU
   * @param feature OperatingUnitType to consider
   * @returns blocking or not
   */
  static bool IsOperatingUnitTypeBlocking(ExecutionOperatingUnitType feature);

  /**
   * Gets the non-parallel type for the OU f
   * @param feature Parallel OU
   * @returns Corresponding non-parallel OU or INVALID
   */
  static ExecutionOperatingUnitType GetNonParallelType(ExecutionOperatingUnitType feature);

  /** @return The ExecutionOperatingUnitFeature that has the corresponding type. It must be unique in the vector. */
  static const ExecutionOperatingUnitFeature &GetFeature(execution::translator_id_t translator_id,
                                                         const std::vector<ExecutionOperatingUnitFeature> &features,
                                                         ExecutionOperatingUnitType type) {
    UNUSED_ATTRIBUTE bool found = false;
    size_t idx = 0;
    for (size_t i = 0; i < features.size(); ++i) {
      bool same_translator = translator_id == features[i].GetTranslatorId();
      bool same_feature = type == features[i].GetExecutionOperatingUnitType();
      if (same_translator && same_feature) {
        TERRIER_ASSERT(!found, "There are multiple features of the same type.");
        found = true;
        idx = i;
      }
    }
    TERRIER_ASSERT(found, "The feature was not found.");
    return features[idx];
  }
};

}  // namespace terrier::brain
