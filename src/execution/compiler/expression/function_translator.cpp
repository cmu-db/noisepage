#include "execution/compiler/expression/function_translator.h"

#include "catalog/catalog_accessor.h"
#include "execution/compiler/translator_factory.h"
#include "execution/functions/function_context.h"

#include "parser/expression/function_expression.h"

namespace terrier::execution::compiler {

FunctionTranslator::FunctionTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen) {
  for (auto child : expression->GetChildren()) {
    params_.push_back(TranslatorFactory::CreateExpressionTranslator(child.Get(), codegen_));
  }
}

ast::Expr *FunctionTranslator::DeriveExpr(ExpressionEvaluator *evaluator) {
  auto func_expr = GetExpressionAs<parser::FunctionExpression>();
  auto proc_oid = func_expr->GetProcOid();
  auto func_context = codegen_->Accessor()->GetFunctionContext(proc_oid);
  if (!func_context->IsBuiltin()) {
    UNREACHABLE("User-defined functions are not supported");
  }

  std::vector<ast::Expr *> params;
  if (func_context->IsExecCtxRequired()) {
    params.push_back(codegen_->MakeExpr(codegen_->GetExecCtxVar()));
  }
  for (auto &param : params_) {
    params.push_back(param->DeriveExpr(evaluator));
  }

  return codegen_->BuiltinCall(func_context->GetBuiltin(), std::move(params));
}
};  // namespace terrier::execution::compiler
