#pragma once

#include "execution/compiler/expression/expression_translator.h"
#include "execution/compiler/operator/operator_translator.h"
#include "parser/expression/abstract_expression.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "execution/util/region.h"
namespace tpl::compiler {

class Pipeline;
class OperatorTranslator;

/**
 * Translator Factory
 */
class TranslatorFactory {
 public:
  static OperatorTranslator *CreateRegularTranslator(const terrier::planner::AbstractPlanNode * op, CodeGen* codegen);
  static OperatorTranslator *CreateBottomTranslator(const terrier::planner::AbstractPlanNode * op, CodeGen* codegen);
  static OperatorTranslator *CreateTopTranslator(const terrier::planner::AbstractPlanNode * op, OperatorTranslator* bottom, CodeGen* codegen);
  static OperatorTranslator *CreateLeftTranslator(const terrier::planner::AbstractPlanNode * op, CodeGen* codegen);
  static OperatorTranslator *CreateRightTranslator(const terrier::planner::AbstractPlanNode * op, OperatorTranslator * left, CodeGen* codegen);

  /**
   * Creates an expression translator
   * @param expression expression to translate
   * @param context compilation context
   * @return created translator
   */
  static ExpressionTranslator *CreateExpressionTranslator(const terrier::parser::AbstractExpression *expression, CodeGen* codegen);
};

}  // namespace tpl::compiler
