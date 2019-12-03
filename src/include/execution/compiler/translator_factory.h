#pragma once

#include <memory>
#include "execution/compiler/expression/expression_translator.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/util/region.h"
#include "parser/expression/abstract_expression.h"
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
  static std::unique_ptr<OperatorTranslator> CreateRegularTranslator(const terrier::planner::AbstractPlanNode *op,
                                                                     CodeGen *codegen);

  /**
   * Create a bottom expression translator
   */
  static std::unique_ptr<OperatorTranslator> CreateBottomTranslator(const terrier::planner::AbstractPlanNode *op,
                                                                    CodeGen *codegen);

  /**
   * Create a top expression translator
   */
  static std::unique_ptr<OperatorTranslator> CreateTopTranslator(const terrier::planner::AbstractPlanNode *op,
                                                                 OperatorTranslator *bottom, CodeGen *codegen);

  /**
   * Create a left expression translator
   */
  static std::unique_ptr<OperatorTranslator> CreateLeftTranslator(const terrier::planner::AbstractPlanNode *op,
                                                                  CodeGen *codegen);

  /**
   * Create a right expression translator
   */
  static std::unique_ptr<OperatorTranslator> CreateRightTranslator(const terrier::planner::AbstractPlanNode *op,
                                                                   OperatorTranslator *left, CodeGen *codegen);

  /**
   * Creates an expression translator
   */
  static std::unique_ptr<ExpressionTranslator> CreateExpressionTranslator(
      const terrier::parser::AbstractExpression *expression, CodeGen *codegen);

  /**
   * Whether this is a comparison operation
   */
  static bool IsComparisonOp(terrier::parser::ExpressionType type) {
    return ((type) <= terrier::parser::ExpressionType::COMPARE_IS_DISTINCT_FROM &&
            (type) >= terrier::parser::ExpressionType::COMPARE_EQUAL);
  }

  /**
   * Whether this is an arithmetic operation
   */
  static bool IsArithmeticOp(terrier::parser::ExpressionType type) {
    return ((type) <= terrier::parser::ExpressionType::OPERATOR_MOD &&
            (type) >= terrier::parser::ExpressionType::OPERATOR_PLUS);
  }

  /**
   * Whether this is a unary operation
   */
  static bool IsUnaryOp(terrier::parser::ExpressionType type) {
    return ((type) == terrier::parser::ExpressionType::OPERATOR_UNARY_MINUS ||
            ((type) >= terrier::parser::ExpressionType::OPERATOR_CAST &&
             (type) <= terrier::parser::ExpressionType::OPERATOR_EXISTS));
  }

  /**
   * Whether this is a conjunction operation
   */
  static bool IsConjunctionOp(terrier::parser::ExpressionType type) {
    return ((type) <= terrier::parser::ExpressionType::CONJUNCTION_OR &&
            (type) >= terrier::parser::ExpressionType::CONJUNCTION_AND);
  }

  /**
   * Whether this is a constant value
   */
  static bool IsConstantVal(terrier::parser::ExpressionType type) {
    return ((type) == terrier::parser::ExpressionType::VALUE_CONSTANT);
  }

  /**
   * Whether this is a column value
   */
  static bool IsColumnVal(terrier::parser::ExpressionType type) {
    return ((type) == terrier::parser::ExpressionType::COLUMN_VALUE);
  }

  /**
   * Whether this is a derived value
   */
  static bool IsDerivedVal(terrier::parser::ExpressionType type) {
    return ((type) == terrier::parser::ExpressionType::VALUE_TUPLE);
  }

  /**
   * Whether this is a null operation
   */
  static bool IsNullOp(terrier::parser::ExpressionType type) {
    return ((type) >= terrier::parser::ExpressionType::OPERATOR_IS_NULL &&
            (type) <= terrier::parser::ExpressionType::OPERATOR_IS_NOT_NULL);
  }
};

}  // namespace terrier::execution::compiler
