#pragma once

namespace terrier::parser {
class AbstractExpression;
}

namespace terrier::planner {
class AbstractPlanNode;
}

namespace tpl::compiler {

class Pipeline;
class OperatorTranslator;
class ExpressionTranslator;

class TranslatorFactory {
 public:
  OperatorTranslator *CreateTranslator(const terrier::planner::AbstractPlanNode &op, Pipeline *pipeline);
  ExpressionTranslator *CreateTranslator(const terrier::parser::AbstractExpression &ex);
};

}