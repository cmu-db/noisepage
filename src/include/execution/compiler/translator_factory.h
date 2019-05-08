#pragma once

#include "parser/expression/abstract_expression.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/expression/expression_translator.h"

namespace tpl::compiler {

class Pipeline;
class OperatorTranslator;

class TranslatorFactory {
 public:
  static OperatorTranslator *CreateTranslator(const terrier::planner::AbstractPlanNode &op, Pipeline *pipeline);
  static ExpressionTranslator *CreateTranslator(const terrier::parser::AbstractExpression &ex, CompilationContext &context);
};

}