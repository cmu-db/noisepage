#pragma once

#include "execution/compiler/expression/expression_translator.h"
#include "execution/compiler/operator/operator_translator.h"
#include "parser/expression/abstract_expression.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace tpl::compiler {

class Pipeline;
class OperatorTranslator;

/**
 * Translator Factory
 */
class TranslatorFactory {
 public:
  /**
   * Creates a plan node translator
   * @param op plan node to translate
   * @param pipeline current pipeline
   * @return created translator
   */
  static OperatorTranslator *CreateTranslator(const terrier::planner::AbstractPlanNode &op, Pipeline *pipeline);

  /**
   * Creates an expression translator
   * @param expression expression to translate
   * @param context compilation context
   * @return created translator
   */
  static ExpressionTranslator *CreateTranslator(const terrier::parser::AbstractExpression &expression,
                                                CompilationContext *context);
};

}  // namespace tpl::compiler
