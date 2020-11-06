#include "execution/compiler/expression/function_translator.h"

#include "catalog/catalog_accessor.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/work_context.h"
#include "execution/functions/function_context.h"
#include "parser/expression/function_expression.h"

namespace noisepage::execution::compiler {

FunctionTranslator::FunctionTranslator(const parser::FunctionExpression &expr, CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  for (const auto child : expr.GetChildren()) {
    compilation_context->Prepare(*child);
  }
}

ast::Expr *FunctionTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const {
  auto *codegen = GetCodeGen();

  const auto &func_expr = GetExpressionAs<parser::FunctionExpression>();
  auto proc_oid = func_expr.GetProcOid();
  auto func_context = codegen->GetCatalogAccessor()->GetFunctionContext(proc_oid);
  if (!func_context->IsBuiltin()) {
    UNREACHABLE("User-defined functions are not supported");
  }

  std::vector<ast::Expr *> params;
  if (func_context->IsExecCtxRequired()) {
    params.push_back(GetExecutionContextPtr());
  }
  for (auto child : func_expr.GetChildren()) {
    auto *derived_expr = ctx->DeriveValue(*child, provider);
    params.push_back(derived_expr);
  }

  return codegen->CallBuiltin(func_context->GetBuiltin(), params);
}

}  // namespace noisepage::execution::compiler
