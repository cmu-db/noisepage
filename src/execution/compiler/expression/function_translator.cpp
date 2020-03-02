#include "execution/compiler/expression/function_translator.h"
#include "execution/compiler/translator_factory.h"
#include "execution/sql/value.h"
#include "parser/expression/function_expression.h"
#include "type/transient_value_peeker.h"

namespace terrier::execution::compiler {
FunctionTranslator::FunctionTranslator(const terrier::parser::AbstractExpression *expression, CodeGen *codegen)
    : ExpressionTranslator(expression, codegen) {
  for (auto child : expression->GetChildren()) {
    params_.push_back(TranslatorFactory::CreateExpressionTranslator(child.Get(), codegen_));
  }
}

ast::Expr *FunctionTranslator::DeriveExpr(ExpressionEvaluator *evaluator) {
  auto proc_oid = GetExpressionAs<parser::FunctionExpression>()->GetProcOid();
  auto udf_ctx = codegen_->Accessor()->GetProcCtxPtr(proc_oid);
  if (!udf_ctx->IsBuiltin()) {
    UNREACHABLE("We don't support non-builtin UDF's yet!");
  }

  std::vector<ast::Expr *> params;
  for (auto &param : params_) {
    params.push_back(param->DeriveExpr(evaluator));
  }

  return codegen_->BuiltinCall(udf_ctx->GetBuiltin(), std::move(params));
}
};  // namespace terrier::execution::compiler
