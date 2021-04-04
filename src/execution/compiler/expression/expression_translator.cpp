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

void ExpressionTranslator::DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) {
  for (auto child : expr_.GetChildren()) {
    compilation_context_->LookupTranslator(*child)->DefineHelperFunctions(decls);
  }
}

void ExpressionTranslator::DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) {
  for (auto child : expr_.GetChildren()) {
    compilation_context_->LookupTranslator(*child)->DefineHelperStructs(decls);
  }
}

}  // namespace noisepage::execution::compiler
