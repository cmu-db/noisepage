#include "execution/compiler/expression/expression_translator.h"

#include "execution/compiler/compilation_context.h"
#include "parser/expression/abstract_expression.h"

namespace noisepage::execution::compiler {

ExpressionTranslator::ExpressionTranslator(const parser::AbstractExpression &expr,
                                           CompilationContext *compilation_context)
    : expr_(expr), compilation_context_(compilation_context) {}

CodeGen *ExpressionTranslator::GetCodeGen() const { return compilation_context_->GetCodeGen(); }

ast::Expr *ExpressionTranslator::GetExecutionContextPtr() const {
  return compilation_context_->GetExecutionContextPtrFromQueryState();
}

}  // namespace noisepage::execution::compiler
