#pragma once

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
  static std::unique_ptr<OperatorTranslator> CreateRegularTranslator(const terrier::planner::AbstractPlanNode *op,
                                                                     CodeGen *codegen);
  static std::unique_ptr<OperatorTranslator> CreateBottomTranslator(const terrier::planner::AbstractPlanNode *op,
                                                                    CodeGen *codegen);
  static std::unique_ptr<OperatorTranslator> CreateTopTranslator(const terrier::planner::AbstractPlanNode *op,
                                                                 OperatorTranslator *bottom, CodeGen *codegen);
  static std::unique_ptr<OperatorTranslator> CreateLeftTranslator(const terrier::planner::AbstractPlanNode *op,
                                                                  CodeGen *codegen);
  static std::unique_ptr<OperatorTranslator> CreateRightTranslator(const terrier::planner::AbstractPlanNode *op,
                                                                   OperatorTranslator *left, CodeGen *codegen);

  /**
   * Creates an expression translator
   * @param expression expression to translate
   * @param context compilation context
   * @return created translator
   */
  static std::unique_ptr<ExpressionTranslator> CreateExpressionTranslator(
      const terrier::parser::AbstractExpression *expression, CodeGen *codegen);
};

}  // namespace terrier::execution::compiler
