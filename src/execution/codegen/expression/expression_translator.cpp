#include "execution/codegen/expression/expression_translator.h"

#include "execution/codegen/compilation_context.h"
#include "parser/expression/abstract_expression.h"

namespace terrier::execution::codegen {

ExpressionTranslator::ExpressionTranslator(const parser::AbstractExpression &expr,
                                           CompilationContext *compilation_context)
    : expr_(expr), compilation_context_(compilation_context) {}

CodeGen *ExpressionTranslator::GetCodeGen() const { return compilation_context_->GetCodeGen(); }

}  // namespace terrier::execution::codegen
