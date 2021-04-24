#include "execution/compiler/expression/function_translator.h"

#include "catalog/catalog_accessor.h"
#include "execution/ast/ast.h"
#include "execution/ast/ast_clone.h"
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

  if (!func_context->IsBuiltin()) {
    auto ident_expr = main_fn_;
    std::vector<ast::Expr *> args;
    for (auto &expr : params) {
      args.emplace_back(expr);
    }
    return GetCodeGen()->Call(ident_expr, std::move(args));
  }

  return codegen->CallBuiltin(func_context->GetBuiltin(), params);
}

void FunctionTranslator::DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) {
  ExpressionTranslator::DefineHelperFunctions(decls);
  auto proc_oid = GetExpressionAs<parser::FunctionExpression>().GetProcOid();
  auto func_context = GetCodeGen()->GetCatalogAccessor()->GetFunctionContext(proc_oid);
  if (func_context->IsBuiltin()) {
    return;
  }
  auto *file = reinterpret_cast<execution::ast::File *>(
      ast::AstClone::Clone(func_context->GetFile(), GetCodeGen()->GetAstContext()->GetNodeFactory(), nullptr,
                           GetCodeGen()->GetAstContext().Get()));
  auto udf_decls = file->Declarations();
  main_fn_ = udf_decls.back()->Name();
  size_t num_added = 0;
  for (ast::Decl *udf_decl : udf_decls) {
    if (udf_decl->IsFunctionDecl()) {
      decls->insert(decls->begin() + num_added, udf_decl->As<ast::FunctionDecl>());
      num_added++;
    }
  }
}

void FunctionTranslator::DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) {
  ExpressionTranslator::DefineHelperStructs(decls);
  auto proc_oid = GetExpressionAs<parser::FunctionExpression>().GetProcOid();
  auto func_context = GetCodeGen()->GetCatalogAccessor()->GetFunctionContext(proc_oid);
  if (func_context->IsBuiltin()) {
    return;
  }
  auto *file = reinterpret_cast<execution::ast::File *>(
      ast::AstClone::Clone(func_context->GetFile(), GetCodeGen()->GetAstContext()->GetNodeFactory(), nullptr,
                           GetCodeGen()->GetAstContext().Get()));
  auto udf_decls = file->Declarations();
  size_t num_added = 0;
  for (ast::Decl *udf_decl : udf_decls) {
    if (udf_decl->IsStructDecl()) {
      decls->insert(decls->begin() + num_added, udf_decl->As<ast::StructDecl>());
      num_added++;
    }
  }
}

}  // namespace noisepage::execution::compiler
