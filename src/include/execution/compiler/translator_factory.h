#pragma once

#include <memory>
#include "execution/compiler/expression/expression_translator.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/util/region.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"
#include "planner/plannodes/abstract_plan_node.h"
namespace terrier::execution::compiler {

class Pipeline;
class OperatorTranslator;

/**
 * Translator Factory
 */
class TranslatorFactory {
 public:
  /**
   * Create a regular expression translator
   */
  static std::unique_ptr<OperatorTranslator> CreateRegularTranslator(const planner::AbstractPlanNode *op,
                                                                     CodeGen *codegen);

  /**
   * Create a bottom expression translator
   */
  static std::unique_ptr<OperatorTranslator> CreateBottomTranslator(const planner::AbstractPlanNode *op,
                                                                    CodeGen *codegen);

  /**
   * Create a top expression translator
   */
  static std::unique_ptr<OperatorTranslator> CreateTopTranslator(const planner::AbstractPlanNode *op,
                                                                 OperatorTranslator *bottom, CodeGen *codegen);

  /**
   * Create a left expression translator
   */
  static std::unique_ptr<OperatorTranslator> CreateLeftTranslator(const planner::AbstractPlanNode *op,
                                                                  CodeGen *codegen);

  /**
   * Create a right expression translator
   */
  static std::unique_ptr<OperatorTranslator> CreateRightTranslator(const planner::AbstractPlanNode *op,
                                                                   OperatorTranslator *left, CodeGen *codegen);

  /**
   * Creates an expression translator
   */
  static std::unique_ptr<ExpressionTranslator> CreateExpressionTranslator(const parser::AbstractExpression *expression,
                                                                          CodeGen *codegen);

  /**
   * Whether this is a comparison operation
   */
  static bool IsComparisonOp(parser::ExpressionType type) {
    return type == parser::ExpressionType::COMPARE_EQUAL || type == parser::ExpressionType::COMPARE_NOT_EQUAL ||
           type == parser::ExpressionType::COMPARE_LESS_THAN || type == parser::ExpressionType::COMPARE_GREATER_THAN ||
           type == parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO ||
           type == parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO;
  }

  /**
   * Whether this is an arithmetic operation
   */
  static bool IsArithmeticOp(parser::ExpressionType type) {
    return type == parser::ExpressionType::OPERATOR_PLUS || type == parser::ExpressionType::OPERATOR_MINUS ||
           type == parser::ExpressionType::OPERATOR_MULTIPLY || type == parser::ExpressionType::OPERATOR_DIVIDE ||
           type == parser::ExpressionType::OPERATOR_MOD;
  }

  /**
   * Whether this is a unary operation
   */
  static bool IsUnaryOp(parser::ExpressionType type) {
    return type == parser::ExpressionType::OPERATOR_UNARY_MINUS || type == parser::ExpressionType::OPERATOR_NOT;
  }

  /**
   * Whether this is a conjunction operation
   */
  static bool IsConjunctionOp(parser::ExpressionType type) {
    return (type == parser::ExpressionType::CONJUNCTION_OR || type == parser::ExpressionType::CONJUNCTION_AND);
  }

  /**
   * Whether this is a constant value
   */
  static bool IsConstantVal(parser::ExpressionType type) { return ((type) == parser::ExpressionType::VALUE_CONSTANT); }

  /**
   * Whether this is a column value
   */
  static bool IsColumnVal(parser::ExpressionType type) { return ((type) == parser::ExpressionType::COLUMN_VALUE); }

  /**
   * Whether this is a derived value
   */
  static bool IsDerivedVal(parser::ExpressionType type) { return ((type) == parser::ExpressionType::VALUE_TUPLE); }

  /**
   * Whether this is a parameter value
   */
  static bool IsParamVal(parser::ExpressionType type) { return ((type) == parser::ExpressionType::VALUE_PARAMETER); }

  /**
   * Whether this is a null operation
   */
  static bool IsNullOp(parser::ExpressionType type) {
    return (type == parser::ExpressionType::OPERATOR_IS_NULL || type == parser::ExpressionType::OPERATOR_IS_NOT_NULL);
  }

  /**
   * Whether this is a Star expression.
   */
  static bool IsStar(parser::ExpressionType type) { return type == parser::ExpressionType::STAR; }

  /**
   * Whether this is a Function expression.
   */
  static bool IsFunction(parser::ExpressionType type) { return type == parser::ExpressionType::FUNCTION; }
};

}  // namespace terrier::execution::compiler
