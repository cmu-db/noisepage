#pragma once

#include "parser/expression/abstract_expression.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "execution/compiler/expression/expression_translator.h"
#include "execution/compiler/operator/operator_translator.h"

namespace tpl::compiler {

class CompilationContext;
class Pipeline;

class TranslatorFactory {
 public:
  OperatorTranslator *CreateTranslator(const terrier::planner::AbstractPlanNode &node, CompilationContext *ctx, Pipeline *pipeline);
  ExpressionTranslator *CreateTranslator(const terrier::parser::AbstractExpression &expr, CompilationContext *ctx);
};

}