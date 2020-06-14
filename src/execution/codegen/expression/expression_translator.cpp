#include "execution/sql/codegen/expression/expression_translator.h"

#include "execution/sql/codegen/compilation_context.h"
#include "execution/sql/planner/expressions/abstract_expression.h"

namespace terrier::execution::codegen {

ExpressionTranslator::ExpressionTranslator(const planner::AbstractExpression &expr,
                                           CompilationContext *compilation_context)
    : expr_(expr), compilation_context_(compilation_context) {}

CodeGen *ExpressionTranslator::GetCodeGen() const { return compilation_context_->GetCodeGen(); }

}  // namespace terrier::execution::codegen
