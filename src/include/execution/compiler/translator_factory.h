#pragma once

#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/expression/expression_translator.h"

namespace terrier::parser {
class AbstractExpression;
}

namespace terrier::planner {
class AbstractPlanNode;
}

namespace tpl::compiler {

class Pipeline;

class TranslatorFactory {
 public:
  OperatorTranslator *CreateTranslator(const terrier::planner::AbstractPlanNode &op, Pipeline *pipeline);
  ExpressionTranslator *CreateTranslator(const terrier::parser::AbstractExpression &ex);
};

}